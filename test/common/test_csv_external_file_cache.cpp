#include "catch.hpp"
#include "caching_test_utils.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "test_helpers.hpp"

#include <condition_variable>

namespace duckdb {

namespace {

class ConcurrentReadTrapFileSystem : public LocalFileSystem {
public:
	ConcurrentReadTrapFileSystem(string remote_path_a_p, string local_path_a_p, string remote_path_b_p,
	                             string local_path_b_p)
	    : remote_path_a(std::move(remote_path_a_p)), local_path_a(std::move(local_path_a_p)),
	      remote_path_b(std::move(remote_path_b_p)), local_path_b(std::move(local_path_b_p)) {
	}

	string GetName() const override {
		return "ConcurrentReadTrapFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return path == remote_path_a || path == remote_path_b;
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		auto local_path = TryGetLocalPath(path);
		if (!local_path.empty()) {
			return LocalFileSystem::OpenFile(local_path, flags, opener);
		}
		return LocalFileSystem::OpenFile(path, flags, opener);
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		auto local_path = TryGetLocalPath(filename);
		if (!local_path.empty()) {
			return LocalFileSystem::FileExists(local_path, opener);
		}
		return LocalFileSystem::FileExists(filename, opener);
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override {
		if (path == remote_path_a || path == remote_path_b) {
			return {OpenFileInfo(path)};
		}
		return LocalFileSystem::Glob(path, opener);
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		WaitForConcurrentRead(handle.GetPath());
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

private:
	string TryGetLocalPath(const string &path) const {
		if (path == remote_path_a) {
			return local_path_a;
		}
		if (path == remote_path_b) {
			return local_path_b;
		}
		return string();
	}

	void WaitForConcurrentRead(const string &path) {
		if (path != local_path_a && path != local_path_b) {
			return;
		}

		annotated_unique_lock<annotated_mutex> lock(lock_mutex);
		if (released) {
			return;
		}

		read_paths.insert(path);
		if (read_paths.size() >= 2) {
			released = true;
			condition.notify_all();
			return;
		}

		const auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		while (!released) {
			if (condition.wait_until(lock, timeout) == std::cv_status::timeout && !released) {
				released = true;
				condition.notify_all();
				throw IOException("Timed out waiting for a concurrent CSV read");
			}
		}
	}

private:
	string remote_path_a;
	string local_path_a;
	string remote_path_b;
	string local_path_b;
	mutable annotated_mutex lock_mutex;
	std::condition_variable condition;
	std::unordered_set<string> read_paths DUCKDB_GUARDED_BY(lock_mutex);
	bool released DUCKDB_GUARDED_BY(lock_mutex) = false;
};

string MakeCSVContent() {
	string result = "i,payload\n";
	const string payload(64, 'x');
	for (idx_t i = 0; i < 10000; i++) {
		result += StringUtil::Format("%llu,%s\n", i, payload);
	}
	return result;
}

} // namespace

TEST_CASE("duckdb-internal-9877", "[csv][external_file_cache]") {
	constexpr idx_t BLOCK_SIZE = 4096;
	const string remote_path_a = "deadlock-repro://csv_external_cache_a.csv";
	const string remote_path_b = "deadlock-repro://csv_external_cache_b.csv";
	CachingTestFileGuard test_file_a("csv_external_cache_lock_repro_a.csv", MakeCSVContent());
	CachingTestFileGuard test_file_b("csv_external_cache_lock_repro_b.csv", MakeCSVContent());

	DBConfig config;
	config.SetOptionByName("enable_external_file_cache", true);
	auto vfs = make_uniq<VirtualFileSystem>();
	auto trap_fs = make_uniq<ConcurrentReadTrapFileSystem>(remote_path_a, test_file_a.GetPath(), remote_path_b,
	                                                       test_file_b.GetPath());
	vfs->RegisterSubSystem(std::move(trap_fs));
	config.file_system = std::move(vfs);

	DuckDB db(":memory:", &config);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET validate_external_file_cache='NO_VALIDATION'"));
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("SET external_file_cache_remote_block_size=%llu", BLOCK_SIZE)));

	auto result = con.Query(StringUtil::Format(
	    "SELECT count(*)::BIGINT, sum(i)::BIGINT FROM read_csv(['%s', '%s'], auto_detect=false, header=true, "
	    "parallel=false, buffer_size=%llu, "
	    "columns={'i':'BIGINT','payload':'VARCHAR'})",
	    remote_path_a, remote_path_b, BLOCK_SIZE));
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 20000);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 99990000);
}

} // namespace duckdb
