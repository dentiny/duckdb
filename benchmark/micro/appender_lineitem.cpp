#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

// TPC-H lineitem-inspired 16-column benchmark for compression analysis.
// All variants use file-based persistent storage (InMemory=false) and
// run 250 hot iterations for stable medians.
//
// AppenderLineitem1M               — auto compression detection (baseline)
// AppenderLineitem1MForcedCompression — USING COMPRESSION per column

static constexpr int32_t LINEITEM_ROW_COUNT = 1000000;
static constexpr int32_t LINEITEM_BASE_DATE = 8827; // 1994-01-01 as days since epoch

static const char CREATE_LINEITEM_AUTO[] =
    "CREATE TABLE lineitem("
    "l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER,"
    "l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE,"
    "l_returnflag VARCHAR, l_linestatus VARCHAR,"
    "l_shipdate INTEGER, l_commitdate INTEGER, l_receiptdate INTEGER,"
    "l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR)";

static const char CREATE_LINEITEM_FORCED[] =
    "CREATE TABLE lineitem("
    "l_orderkey    INTEGER  USING COMPRESSION BITPACKING,"
    "l_partkey     INTEGER  USING COMPRESSION BITPACKING,"
    "l_suppkey     INTEGER  USING COMPRESSION BITPACKING,"
    "l_linenumber  INTEGER  USING COMPRESSION BITPACKING,"
    "l_quantity    DOUBLE   USING COMPRESSION ALP,"
    "l_extendedprice DOUBLE USING COMPRESSION ALP,"
    "l_discount    DOUBLE   USING COMPRESSION ALP,"
    "l_tax         DOUBLE   USING COMPRESSION ALP,"
    "l_returnflag  VARCHAR  USING COMPRESSION DICTIONARY,"
    "l_linestatus  VARCHAR  USING COMPRESSION DICTIONARY,"
    "l_shipdate    INTEGER  USING COMPRESSION BITPACKING,"
    "l_commitdate  INTEGER  USING COMPRESSION BITPACKING,"
    "l_receiptdate INTEGER  USING COMPRESSION BITPACKING,"
    "l_shipinstruct VARCHAR USING COMPRESSION DICTIONARY,"
    "l_shipmode    VARCHAR  USING COMPRESSION DICTIONARY,"
    "l_comment     VARCHAR  USING COMPRESSION DICTIONARY)";

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
// Shared base class
// ---------------------------------------------------------------------------
class AppenderLineitemBenchmark : public DuckDBBenchmark {
protected:
	AppenderLineitemBenchmark(bool register_benchmark, const string &name)
	    : DuckDBBenchmark(register_benchmark, name, "[appender_lineitem]") {
	}

public:
	bool InMemory() override { return false; }
	size_t NRuns() override  { return 250; }

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

	string VerifyResult(QueryResult *result) override { return string(); }
};

// ---------------------------------------------------------------------------
// Variant 1: auto compression detection (baseline)
// ---------------------------------------------------------------------------
class AppenderLineitem1MBenchmark : public AppenderLineitemBenchmark {
	AppenderLineitem1MBenchmark(bool r) : AppenderLineitemBenchmark(r, "AppenderLineitem1M") {}
public:
	static AppenderLineitem1MBenchmark *GetInstance() {
		static AppenderLineitem1MBenchmark s(true);
		auto b = duckdb::unique_ptr<AppenderLineitem1MBenchmark>(new AppenderLineitem1MBenchmark(false));
		return &s;
	}
	void Load(DuckDBBenchmarkState *state) override { state->conn.Query(CREATE_LINEITEM_AUTO); }
	string BenchmarkInfo() override { return "1M lineitem rows — auto compression detection"; }
};
auto global_instance_AppenderLineitem1M = AppenderLineitem1MBenchmark::GetInstance();

// ---------------------------------------------------------------------------
// Variant 2: hard-coded per-column compression (no detect scan when fixed)
// ---------------------------------------------------------------------------
class AppenderLineitem1MForcedCompressionBenchmark : public AppenderLineitemBenchmark {
	AppenderLineitem1MForcedCompressionBenchmark(bool r)
	    : AppenderLineitemBenchmark(r, "AppenderLineitem1MForcedCompression") {}
public:
	static AppenderLineitem1MForcedCompressionBenchmark *GetInstance() {
		static AppenderLineitem1MForcedCompressionBenchmark s(true);
		auto b = duckdb::unique_ptr<AppenderLineitem1MForcedCompressionBenchmark>(
		    new AppenderLineitem1MForcedCompressionBenchmark(false));
		return &s;
	}
	void Load(DuckDBBenchmarkState *state) override { state->conn.Query(CREATE_LINEITEM_FORCED); }
	string BenchmarkInfo() override {
		return "1M lineitem rows — USING COMPRESSION per column (Bitpacking/ALP/Dictionary)";
	}
};
auto global_instance_AppenderLineitem1MForcedCompression =
    AppenderLineitem1MForcedCompressionBenchmark::GetInstance();
