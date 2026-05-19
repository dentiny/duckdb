#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "test_helpers.hpp"

namespace duckdb {

namespace {

class ExternalCacheTestFile {
public:
	ExternalCacheTestFile(const string &filename, const string &content_p)
	    : path(TestCreatePath(filename)), content(content_p) {
		auto local_fs = FileSystem::CreateLocal();
		auto handle = local_fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		handle->Write(QueryContext(), const_cast<char *>(content.data()), content.size(), 0);
		handle->Sync();
	}

	~ExternalCacheTestFile() {
		auto local_fs = FileSystem::CreateLocal();
		local_fs->TryRemoveFile(path);
	}

	const string &Path() const {
		return path;
	}

	const string &Content() const {
		return content;
	}

private:
	string path;
	string content;
};

class VersionedLocalFileSystem : public LocalFileSystem {
public:
	explicit VersionedLocalFileSystem(string version_tag_p) : version_tag(std::move(version_tag_p)) {
	}

	string GetVersionTag(FileHandle &handle) override {
		return version_tag;
	}

	void SetVersionTag(string version_tag_p) {
		version_tag = std::move(version_tag_p);
	}

private:
	string version_tag;
};

OpenFileInfo MakeOpenFileInfo(const string &path, bool validate) {
	OpenFileInfo info(path);
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(validate);
	return info;
}

void UnloadBlock(const shared_ptr<BlockHandle> &block) {
	auto &memory = block->GetMemory();
	auto lock = memory.GetLock();
	REQUIRE(memory.GetReaders() == 0);
	REQUIRE(memory.CanUnload());
	memory.Unload(lock);
}

BufferHandle ReadRange(CachingFileHandle &handle, const string &expected_content, idx_t size = 0, idx_t location = 0) {
	data_ptr_t buffer;
	if (size == 0) {
		size = expected_content.size();
	}
	auto pin = handle.Read(buffer, size, location);
	REQUIRE(pin.IsValid());
	REQUIRE(memcmp(buffer, expected_content.data() + location, size) == 0);
	return pin;
}

} // namespace

TEST_CASE("External file cache removes unopened range entries on handle close", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &cache = db.instance->GetExternalFileCache();
	auto local_fs = FileSystem::CreateLocal();
	CachingFileSystem caching_fs(*local_fs, *db.instance);
	ExternalCacheTestFile test_file("external_cache_empty_open.txt", "hello");

	{
		auto handle = caching_fs.OpenFile(MakeOpenFileInfo(test_file.Path(), false), FileFlags::FILE_FLAGS_READ);
		REQUIRE(cache.GetCachedFileCount() == 1);
	}

	REQUIRE(cache.GetCachedFileCount() == 0);
}

TEST_CASE("External file cache keeps empty entry until all handles close", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &cache = db.instance->GetExternalFileCache();
	auto local_fs = FileSystem::CreateLocal();
	CachingFileSystem caching_fs(*local_fs, *db.instance);
	ExternalCacheTestFile test_file("external_cache_empty_multi_open.txt", "hello");

	auto handle_a = caching_fs.OpenFile(MakeOpenFileInfo(test_file.Path(), false), FileFlags::FILE_FLAGS_READ);
	auto handle_b = caching_fs.OpenFile(MakeOpenFileInfo(test_file.Path(), false), FileFlags::FILE_FLAGS_READ);
	REQUIRE(cache.GetCachedFileCount() == 1);

	handle_a.reset();
	REQUIRE(cache.GetCachedFileCount() == 1);

	handle_b.reset();
	REQUIRE(cache.GetCachedFileCount() == 0);
}

TEST_CASE("External file cache erase is based on cached file identity", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &cache = db.instance->GetExternalFileCache();
	const string path = TestCreatePath("external_cache_identity.txt");

	auto old_cached_file = cache.GetOrCreateCachedFile(path);
	REQUIRE(cache.GetCachedFileCount() == 1);
	cache.ReleaseCachedFileHandle(old_cached_file);
	REQUIRE(cache.GetCachedFileCount() == 0);

	auto new_cached_file = cache.GetOrCreateCachedFile(path);
	REQUIRE(cache.GetCachedFileCount() == 1);
	cache.TryEraseFile(old_cached_file);
	REQUIRE(cache.GetCachedFileCount() == 1);

	cache.ReleaseCachedFileHandle(new_cached_file);
	REQUIRE(cache.GetCachedFileCount() == 0);
}

TEST_CASE("External file cache removes data-backed entry after block unload", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &cache = db.instance->GetExternalFileCache();
	auto local_fs = FileSystem::CreateLocal();
	CachingFileSystem caching_fs(*local_fs, *db.instance);
	ExternalCacheTestFile test_file("external_cache_block_unload.txt", "cached data");

	auto handle = caching_fs.OpenFile(MakeOpenFileInfo(test_file.Path(), false), FileFlags::FILE_FLAGS_READ);
	auto pin = ReadRange(*handle, test_file.Content());
	auto block = pin.GetBlockHandle();
	pin.Destroy();
	handle.reset();
	REQUIRE(cache.GetCachedFileCount() == 1);

	UnloadBlock(block);
	REQUIRE(cache.GetCachedFileCount() == 0);
}

TEST_CASE("External file cache keeps data-backed entry while handle is active", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &cache = db.instance->GetExternalFileCache();
	auto local_fs = FileSystem::CreateLocal();
	CachingFileSystem caching_fs(*local_fs, *db.instance);
	ExternalCacheTestFile test_file("external_cache_active_handle.txt", "cached data");

	auto handle = caching_fs.OpenFile(MakeOpenFileInfo(test_file.Path(), false), FileFlags::FILE_FLAGS_READ);
	auto pin = ReadRange(*handle, test_file.Content());
	auto block = pin.GetBlockHandle();
	pin.Destroy();

	UnloadBlock(block);
	REQUIRE(cache.GetCachedFileCount() == 1);

	handle.reset();
	REQUIRE(cache.GetCachedFileCount() == 0);
}

TEST_CASE("External file cache range cleanup is not double-counted", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &cache = db.instance->GetExternalFileCache();
	auto local_fs = FileSystem::CreateLocal();
	CachingFileSystem caching_fs(*local_fs, *db.instance);
	ExternalCacheTestFile test_file("external_cache_double_cleanup.txt", "cached data");

	auto handle = caching_fs.OpenFile(MakeOpenFileInfo(test_file.Path(), false), FileFlags::FILE_FLAGS_READ);
	for (idx_t i = 0; i < 2; i++) {
		auto pin = ReadRange(*handle, test_file.Content());
		auto block = pin.GetBlockHandle();
		pin.Destroy();
		UnloadBlock(block);
		REQUIRE(cache.GetCachedFileCount() == 1);
	}

	handle.reset();
	REQUIRE(cache.GetCachedFileCount() == 0);
}

TEST_CASE("External file cache invalidation resets range accounting", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &cache = db.instance->GetExternalFileCache();
	VersionedLocalFileSystem local_fs("v1");
	CachingFileSystem caching_fs(local_fs, *db.instance);
	ExternalCacheTestFile test_file("external_cache_invalidation.txt", "cached data");

	auto handle = caching_fs.OpenFile(MakeOpenFileInfo(test_file.Path(), true), FileFlags::FILE_FLAGS_READ);
	auto pin = ReadRange(*handle, test_file.Content());
	pin.Destroy();
	handle.reset();
	REQUIRE(cache.GetCachedFileCount() == 1);

	local_fs.SetVersionTag("v2");
	auto invalidating_handle =
	    caching_fs.OpenFile(MakeOpenFileInfo(test_file.Path(), true), FileFlags::FILE_FLAGS_READ);
	REQUIRE(cache.GetCachedFileCount() == 1);

	invalidating_handle.reset();
	REQUIRE(cache.GetCachedFileCount() == 0);
}

} // namespace duckdb
