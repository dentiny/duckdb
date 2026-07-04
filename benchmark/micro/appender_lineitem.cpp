#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

// Benchmarks DirectAppender ingestion of a TPC-H lineitem-inspired schema.
// The lineitem table is the largest and most column-diverse table in TPC-H
// (16 columns: 4×INTEGER, 4×DOUBLE, 2×VARCHAR flags, 3×INTEGER dates,
//  1×VARCHAR shipinstruct, 1×VARCHAR shipmode, 1×VARCHAR comment).
//
// All variants persist to disk so checkpoint cost (including compression
// detection) is fully exercised.  The VARCHAR columns in this schema have
// short average lengths (1–22 bytes), making them a direct test of the
// FSST short-string early-exit optimisation in StringFinalAnalyze.

static constexpr int32_t LINEITEM_ROW_COUNT = 1000000;
static constexpr int32_t LINEITEM_BASE_DATE = 8827; // 1994-01-01 as days since epoch

static const char CREATE_LINEITEM[] =
    "CREATE TABLE lineitem("
    "l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER,"
    "l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE,"
    "l_returnflag VARCHAR, l_linestatus VARCHAR,"
    "l_shipdate INTEGER, l_commitdate INTEGER, l_receiptdate INTEGER,"
    "l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR)";

static const char *const RETURNFLAGS[]     = {"A", "N", "R"};
static const uint32_t    RETURNFLAG_LENS[] = {1, 1, 1};
static const char *const LINESTATUS[]      = {"O", "F"};
static const uint32_t    LINESTATUS_LENS[] = {1, 1};
static const char *const SHIPINSTRUCT[]    = {"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"};
static const uint32_t    SHIPINSTRUCT_LENS[] = {17, 11, 4, 16};
static const char *const SHIPMODE[]        = {"AIR", "AIR REG", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};
static const uint32_t    SHIPMODE_LENS[]   = {3, 7, 3, 4, 4, 7, 4, 5};
static const char        COMMENT_STR[]     = "regular annual package";
static constexpr uint32_t COMMENT_LEN      = 22;

// ---------------------------------------------------------------------------
// Base class — shared schema, data generation, and disk persistence
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
			appender.Append<int32_t>(i / 7 + 1);
			appender.Append<int32_t>(i % 200000 + 1);
			appender.Append<int32_t>(i % 10000 + 1);
			appender.Append<int32_t>(i % 7 + 1);
			appender.Append<double>(1.0 + double(i % 50));
			appender.Append<double>(900.0 + double(i % 104949) * 0.01);
			appender.Append<double>(double(i % 11) * 0.01);
			appender.Append<double>(double(i % 9) * 0.01);
			appender.Append<string_t>(string_t(RETURNFLAGS[rf], RETURNFLAG_LENS[rf]));
			appender.Append<string_t>(string_t(LINESTATUS[ls], LINESTATUS_LENS[ls]));
			appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);
			appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);
			appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);
			appender.Append<string_t>(string_t(SHIPINSTRUCT[si], SHIPINSTRUCT_LENS[si]));
			appender.Append<string_t>(string_t(SHIPMODE[sm], SHIPMODE_LENS[sm]));
			appender.Append<string_t>(string_t(COMMENT_STR, COMMENT_LEN));
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
		return "Ingest 1M rows into a 16-column TPC-H lineitem-like table using DirectAppender (persistent)";
	}
};

// ---------------------------------------------------------------------------
// Variant 1: no primary key  (exercises VARCHAR compression detection —
//            the primary target of the FSST short-string optimisation)
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
		state->conn.Query(CREATE_LINEITEM);
	}
};
auto global_instance_AppenderLineitem1M = AppenderLineitem1MBenchmark::GetInstance();

// ---------------------------------------------------------------------------
// Variant 2: composite primary key (l_orderkey, l_linenumber) —
//            adds ART index maintenance on top of the compression cost
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
		state->conn.Query(
		    "CREATE TABLE lineitem("
		    "l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER,"
		    "l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE,"
		    "l_returnflag VARCHAR, l_linestatus VARCHAR,"
		    "l_shipdate INTEGER, l_commitdate INTEGER, l_receiptdate INTEGER,"
		    "l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR,"
		    "PRIMARY KEY (l_orderkey, l_linenumber))");
	}
};
auto global_instance_AppenderLineitem1MPrimaryKey = AppenderLineitem1MPrimaryKeyBenchmark::GetInstance();
