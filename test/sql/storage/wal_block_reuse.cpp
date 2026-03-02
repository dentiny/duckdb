#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// Verify that blocks freed by DROP TABLE are not reused before the WAL is truncated.
TEST_CASE("WAL replay block reuse after drop", "[storage]") {
	duckdb::DBConfig config;
	config.options.checkpoint_on_shutdown = false;
	config.options.checkpoint_wal_size = idx_t(-1);
	config.SetOptionByName("debug_skip_checkpoint_on_commit", true);
	// Force row groups to be flushed to disk immediately during INSERT.
	// This makes the WAL store block pointers (via WriteRowGroupData) instead of inline data (WriteInsert).
	// Without this, the INSERT data stays in transient in-memory segments, the WAL stores inline chunks,
	// and WAL replay never reads from the overwritten blocks — so the corruption would be invisible.
	config.SetOptionByName("write_buffer_row_group_count", duckdb::Value::UBIGINT(1));

	std::string file_path = TestCreatePath("wal_block_reuse.db");
	DeleteDatabase(file_path);

	{
		duckdb::DuckDB db(file_path, &config);
		duckdb::Connection con(db);

		// Create table with PRIMARY KEY index (forces block reads during WAL replay)
		auto res = con.Query("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR, c VARCHAR, d VARCHAR)");
		REQUIRE(!res->HasError());

		// Insert data (allocates blocks, WAL records block pointers)
		res = con.Query("INSERT INTO t SELECT i, "
		                "'row_' || i || repeat('x', 20), "
		                "'data_' || i || repeat('y', 30), "
		                "'col_' || i || repeat('z', 25) "
		                "FROM generate_series(0, 249999) t(i)");
		REQUIRE(!res->HasError());

		// Drop table (frees blocks - bug would put them on free_list immediately)
		res = con.Query("DROP TABLE t");
		REQUIRE(!res->HasError());

		// Create new table with different schema
		res = con.Query("CREATE TABLE t2 (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE)");
		REQUIRE(!res->HasError());

		// Insert into new table (reuses freed blocks if bug exists, overwrites with DOUBLE data)
		res = con.Query("INSERT INTO t2 SELECT "
		                "i * 3.14159, i * 2.71828, i * 1.41421, i * 1.73205 "
		                "FROM generate_series(0, 999999) t(i)");
		REQUIRE(!res->HasError());
	}

	// Reopen triggers WAL replay. The WAL contains:
	//   CREATE TABLE t (with PRIMARY KEY) → INSERT t → DROP t → CREATE t2 → INSERT t2
	// If blocks were prematurely reused, replaying t's INSERT reads corrupted data (DOUBLE instead of VARCHAR).
	{
		duckdb::DuckDB db(file_path, &config);
		duckdb::Connection con(db);

		auto result = con.Query("SELECT COUNT(*) FROM t2");
		REQUIRE(!result->HasError());
		REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1000000);
	}
}
