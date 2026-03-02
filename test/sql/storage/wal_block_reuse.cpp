#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

using namespace duckdb;

// Verify that blocks freed by DROP TABLE are not reused before the WAL is truncated.
TEST_CASE("WAL replay block reuse after drop", "[storage]") {
	duckdb::DBConfig config;
	config.options.checkpoint_on_shutdown = false;
	config.options.checkpoint_wal_size = idx_t(-1);
	config.SetOptionByName("debug_skip_checkpoint_on_commit", true);
	// Force row groups to be flushed to disk immediately during INSERT.
	config.SetOptionByName("write_buffer_row_group_count", duckdb::Value::UBIGINT(1));

	std::string file_path = TestCreatePath("wal_block_reuse.db");
	DeleteDatabase(file_path);

	std::mutex mtx;
	std::condition_variable cv;
	int phase = 0;

	auto wait_for_phase = [&](int target) {
		std::unique_lock<std::mutex> lock(mtx);
		cv.wait(lock, [&] { return phase >= target; });
	};

	auto advance_phase = [&](int target) {
		{
			std::lock_guard<std::mutex> lock(mtx);
			phase = target;
		}
		cv.notify_all();
	};

	std::atomic<bool> thread_a_ok {true};
	std::atomic<bool> thread_b_ok {true};

	{
		duckdb::DuckDB db(file_path, &config);

		std::thread thread_a([&]() {
			duckdb::Connection con_a(db);

			auto res = con_a.Query("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR, c VARCHAR, d VARCHAR)");
			if (res->HasError()) {
				thread_a_ok = false;
				return;
			}

			res = con_a.Query("INSERT INTO t SELECT i, "
			                  "'row_' || i || repeat('x', 20), "
			                  "'data_' || i || repeat('y', 30), "
			                  "'col_' || i || repeat('z', 25) "
			                  "FROM generate_series(0, 249999) t(i)");
			if (res->HasError()) {
				thread_a_ok = false;
				return;
			}

			advance_phase(1);
			wait_for_phase(2);
		});

		std::thread thread_b([&]() {
			duckdb::Connection con_b(db);

			wait_for_phase(1);

			auto res = con_b.Query("DROP TABLE t");
			if (res->HasError()) {
				thread_b_ok = false;
				advance_phase(2);
				return;
			}

			res = con_b.Query("CREATE TABLE t2 (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE)");
			if (res->HasError()) {
				thread_b_ok = false;
				advance_phase(2);
				return;
			}

			res = con_b.Query("INSERT INTO t2 SELECT "
			                  "i * 3.14159, i * 2.71828, i * 1.41421, i * 1.73205 "
			                  "FROM generate_series(0, 999999) t(i)");
			if (res->HasError()) {
				thread_b_ok = false;
				advance_phase(2);
				return;
			}

			advance_phase(2);
		});

		thread_a.join();
		thread_b.join();

		REQUIRE(thread_a_ok);
		REQUIRE(thread_b_ok);
	}

	// Reopen triggers WAL replay.
	{
		duckdb::DuckDB db(file_path, &config);
		duckdb::Connection con(db);

		auto result = con.Query("SELECT COUNT(*) FROM t2");
		REQUIRE(!result->HasError());
		REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1000000);
	}
}
