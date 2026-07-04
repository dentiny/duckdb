#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

// Benchmarks Appender ingestion using a TPC-H lineitem-inspired schema.
// The lineitem table is the largest and most column-diverse table in TPC-H (16 columns),
// making it a well-known target for bulk ingestion performance evaluation.
//
// Schema: 4x INTEGER, 4x DOUBLE, 2x VARCHAR (1-char flags), 3x INTEGER (dates as days
//         since epoch), 1x VARCHAR (shipinstruct), 1x VARCHAR (shipmode), 1x VARCHAR (comment)
//
// l_orderkey / l_linenumber are assigned so that every (orderkey, linenumber) pair is
// unique (each orderkey gets at most 7 line items, linenumber 1–7), allowing the
// PRIMARY KEY variant to run without constraint violations.
//
// All variants persist to disk (InMemory() returns false) to reflect realistic ingestion.

static constexpr int32_t LINEITEM_ROW_COUNT = 1000000;

// Days since 1970-01-01 for 1994-01-01 (start of the TPC-H date range)
static constexpr int32_t LINEITEM_BASE_DATE = 8827;

static const char CREATE_LINEITEM_NO_PK[] =
    "CREATE TABLE lineitem("
    "l_orderkey INTEGER, "
    "l_partkey INTEGER, "
    "l_suppkey INTEGER, "
    "l_linenumber INTEGER, "
    "l_quantity DOUBLE, "
    "l_extendedprice DOUBLE, "
    "l_discount DOUBLE, "
    "l_tax DOUBLE, "
    "l_returnflag VARCHAR, "
    "l_linestatus VARCHAR, "
    "l_shipdate INTEGER, "
    "l_commitdate INTEGER, "
    "l_receiptdate INTEGER, "
    "l_shipinstruct VARCHAR, "
    "l_shipmode VARCHAR, "
    "l_comment VARCHAR)";

static const char CREATE_LINEITEM_PK[] =
    "CREATE TABLE lineitem("
    "l_orderkey INTEGER, "
    "l_partkey INTEGER, "
    "l_suppkey INTEGER, "
    "l_linenumber INTEGER, "
    "l_quantity DOUBLE, "
    "l_extendedprice DOUBLE, "
    "l_discount DOUBLE, "
    "l_tax DOUBLE, "
    "l_returnflag VARCHAR, "
    "l_linestatus VARCHAR, "
    "l_shipdate INTEGER, "
    "l_commitdate INTEGER, "
    "l_receiptdate INTEGER, "
    "l_shipinstruct VARCHAR, "
    "l_shipmode VARCHAR, "
    "l_comment VARCHAR, "
    "PRIMARY KEY (l_orderkey, l_linenumber))";

// TPC-H enumeration domains
static const char *const RETURNFLAGS[] = {"A", "N", "R"};
static const uint32_t RETURNFLAG_LENS[] = {1, 1, 1};

static const char *const LINESTATUS[] = {"O", "F"};
static const uint32_t LINESTATUS_LENS[] = {1, 1};

static const char *const SHIPINSTRUCT[] = {"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"};
static const uint32_t SHIPINSTRUCT_LENS[] = {17, 11, 4, 16};

static const char *const SHIPMODE[] = {"AIR", "AIR REG", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};
static const uint32_t SHIPMODE_LENS[] = {3, 7, 3, 4, 4, 7, 4, 5};

static const char COMMENT_STR[] = "regular annual package";
static constexpr uint32_t COMMENT_LEN = 22;

// ---------------------------------------------------------------------------
// Base class: shared RunBenchmark / Cleanup / VerifyResult / BenchmarkInfo
// All variants persist to disk.
// ---------------------------------------------------------------------------
class AppenderLineitemBenchmark : public DuckDBBenchmark {
protected:
	AppenderLineitemBenchmark(bool register_benchmark, const string &name)
	    : DuckDBBenchmark(register_benchmark, name, "[appender_lineitem]") {
	}

public:
	bool InMemory() override {
		return false;
	}

	void RunBenchmark(DuckDBBenchmarkState *state) override {
		Appender appender(state->conn, "lineitem");
		for (int32_t i = 0; i < LINEITEM_ROW_COUNT; i++) {
			const idx_t rf = idx_t(i) % 3;
			const idx_t ls = idx_t(i) % 2;
			const idx_t si = idx_t(i) % 4;
			const idx_t sm = idx_t(i) % 8;
			appender.BeginRow();
			appender.Append<int32_t>(i / 7 + 1);                                        // l_orderkey
			appender.Append<int32_t>(i % 200000 + 1);                                   // l_partkey
			appender.Append<int32_t>(i % 10000 + 1);                                    // l_suppkey
			appender.Append<int32_t>(i % 7 + 1);                                        // l_linenumber
			appender.Append<double>(1.0 + double(i % 50));                              // l_quantity
			appender.Append<double>(900.0 + double(i % 104949) * 0.01);                 // l_extendedprice
			appender.Append<double>(double(i % 11) * 0.01);                             // l_discount
			appender.Append<double>(double(i % 9) * 0.01);                              // l_tax
			appender.Append<string_t>(string_t(RETURNFLAGS[rf], RETURNFLAG_LENS[rf]));  // l_returnflag
			appender.Append<string_t>(string_t(LINESTATUS[ls], LINESTATUS_LENS[ls]));   // l_linestatus
			appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);                    // l_shipdate
			appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);                    // l_commitdate
			appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);                    // l_receiptdate
			appender.Append<string_t>(string_t(SHIPINSTRUCT[si], SHIPINSTRUCT_LENS[si])); // l_shipinstruct
			appender.Append<string_t>(string_t(SHIPMODE[sm], SHIPMODE_LENS[sm]));       // l_shipmode
			appender.Append<string_t>(string_t(COMMENT_STR, COMMENT_LEN));              // l_comment
			appender.EndRow();
		}
		appender.Close();
	}

	void Cleanup(DuckDBBenchmarkState *state) override {
		state->conn.Query("DROP TABLE lineitem");
		Load(state);
	}

	string VerifyResult(QueryResult *result) override {
		return string();
	}

	string BenchmarkInfo() override {
		return "Ingest 1M rows into a 16-column TPC-H lineitem-like table using the Appender (persistent storage)";
	}
};

// ---------------------------------------------------------------------------
// Variant 1: no primary key
// ---------------------------------------------------------------------------
class AppenderLineitem1MBenchmark : public AppenderLineitemBenchmark {
	AppenderLineitem1MBenchmark(bool register_benchmark)
	    : AppenderLineitemBenchmark(register_benchmark, "AppenderLineitem1M") {
	}

public:
	static AppenderLineitem1MBenchmark *GetInstance() {
		static AppenderLineitem1MBenchmark singleton(true);
		auto b = duckdb::unique_ptr<AppenderLineitem1MBenchmark>(new AppenderLineitem1MBenchmark(false));
		return &singleton;
	}

	void Load(DuckDBBenchmarkState *state) override {
		state->conn.Query(CREATE_LINEITEM_NO_PK);
	}
};
auto global_instance_AppenderLineitem1M = AppenderLineitem1MBenchmark::GetInstance();

// ---------------------------------------------------------------------------
// Variant 2: composite primary key (l_orderkey, l_linenumber)
// ---------------------------------------------------------------------------
class AppenderLineitem1MPrimaryKeyBenchmark : public AppenderLineitemBenchmark {
	AppenderLineitem1MPrimaryKeyBenchmark(bool register_benchmark)
	    : AppenderLineitemBenchmark(register_benchmark, "AppenderLineitem1MPrimaryKey") {
	}

public:
	static AppenderLineitem1MPrimaryKeyBenchmark *GetInstance() {
		static AppenderLineitem1MPrimaryKeyBenchmark singleton(true);
		auto b = duckdb::unique_ptr<AppenderLineitem1MPrimaryKeyBenchmark>(
		    new AppenderLineitem1MPrimaryKeyBenchmark(false));
		return &singleton;
	}

	void Load(DuckDBBenchmarkState *state) override {
		state->conn.Query(CREATE_LINEITEM_PK);
	}
};
auto global_instance_AppenderLineitem1MPrimaryKey = AppenderLineitem1MPrimaryKeyBenchmark::GetInstance();

// ---------------------------------------------------------------------------
// DirectAppender base: same schema / data / disk persistence as above, but
// uses DirectAppender which flushes directly into the table without going
// through the full query pipeline.
// ---------------------------------------------------------------------------
class DirectAppenderLineitemBenchmark : public AppenderLineitemBenchmark {
protected:
	DirectAppenderLineitemBenchmark(bool register_benchmark, const string &name)
	    : AppenderLineitemBenchmark(register_benchmark, name) {
	}

public:
	void RunBenchmark(DuckDBBenchmarkState *state) override {
		DirectAppender appender(state->conn, "lineitem");
		for (int32_t i = 0; i < LINEITEM_ROW_COUNT; i++) {
			const idx_t rf = idx_t(i) % 3;
			const idx_t ls = idx_t(i) % 2;
			const idx_t si = idx_t(i) % 4;
			const idx_t sm = idx_t(i) % 8;
			appender.BeginRow();
			appender.Append<int32_t>(i / 7 + 1);                                        // l_orderkey
			appender.Append<int32_t>(i % 200000 + 1);                                   // l_partkey
			appender.Append<int32_t>(i % 10000 + 1);                                    // l_suppkey
			appender.Append<int32_t>(i % 7 + 1);                                        // l_linenumber
			appender.Append<double>(1.0 + double(i % 50));                              // l_quantity
			appender.Append<double>(900.0 + double(i % 104949) * 0.01);                 // l_extendedprice
			appender.Append<double>(double(i % 11) * 0.01);                             // l_discount
			appender.Append<double>(double(i % 9) * 0.01);                              // l_tax
			appender.Append<string_t>(string_t(RETURNFLAGS[rf], RETURNFLAG_LENS[rf]));  // l_returnflag
			appender.Append<string_t>(string_t(LINESTATUS[ls], LINESTATUS_LENS[ls]));   // l_linestatus
			appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);                    // l_shipdate
			appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);                    // l_commitdate
			appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);                    // l_receiptdate
			appender.Append<string_t>(string_t(SHIPINSTRUCT[si], SHIPINSTRUCT_LENS[si])); // l_shipinstruct
			appender.Append<string_t>(string_t(SHIPMODE[sm], SHIPMODE_LENS[sm]));       // l_shipmode
			appender.Append<string_t>(string_t(COMMENT_STR, COMMENT_LEN));              // l_comment
			appender.EndRow();
		}
		appender.Close();
	}

	string BenchmarkInfo() override {
		return "Ingest 1M rows into a 16-column TPC-H lineitem-like table using DirectAppender (persistent storage)";
	}
};

// ---------------------------------------------------------------------------
// DirectAppender variant 1: no primary key
// ---------------------------------------------------------------------------
class DirectAppenderLineitem1MBenchmark : public DirectAppenderLineitemBenchmark {
	DirectAppenderLineitem1MBenchmark(bool register_benchmark)
	    : DirectAppenderLineitemBenchmark(register_benchmark, "DirectAppenderLineitem1M") {
	}

public:
	static DirectAppenderLineitem1MBenchmark *GetInstance() {
		static DirectAppenderLineitem1MBenchmark singleton(true);
		auto b =
		    duckdb::unique_ptr<DirectAppenderLineitem1MBenchmark>(new DirectAppenderLineitem1MBenchmark(false));
		return &singleton;
	}

	void Load(DuckDBBenchmarkState *state) override {
		state->conn.Query(CREATE_LINEITEM_NO_PK);
	}
};
auto global_instance_DirectAppenderLineitem1M = DirectAppenderLineitem1MBenchmark::GetInstance();

// ---------------------------------------------------------------------------
// DirectAppender variant 2: composite primary key (l_orderkey, l_linenumber)
// ---------------------------------------------------------------------------
class DirectAppenderLineitem1MPrimaryKeyBenchmark : public DirectAppenderLineitemBenchmark {
	DirectAppenderLineitem1MPrimaryKeyBenchmark(bool register_benchmark)
	    : DirectAppenderLineitemBenchmark(register_benchmark, "DirectAppenderLineitem1MPrimaryKey") {
	}

public:
	static DirectAppenderLineitem1MPrimaryKeyBenchmark *GetInstance() {
		static DirectAppenderLineitem1MPrimaryKeyBenchmark singleton(true);
		auto b = duckdb::unique_ptr<DirectAppenderLineitem1MPrimaryKeyBenchmark>(
		    new DirectAppenderLineitem1MPrimaryKeyBenchmark(false));
		return &singleton;
	}

	void Load(DuckDBBenchmarkState *state) override {
		state->conn.Query(CREATE_LINEITEM_PK);
	}
};
auto global_instance_DirectAppenderLineitem1MPrimaryKey = DirectAppenderLineitem1MPrimaryKeyBenchmark::GetInstance();
