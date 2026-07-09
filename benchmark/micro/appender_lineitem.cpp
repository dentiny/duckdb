#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

// Two variants of the TPC-H lineitem-inspired 16-column schema targeting the
// RLE early-exit optimisation in RLEAnalyze.
//
// HighCardInt — integer columns hold sequential / high-cardinality values
//               (distinct per row or cycling with a large period).  RLE
//               cannot compress them; the early-exit should fire and skip
//               the rest of the analyze scan.
//
// LowCardInt  — integer columns hold low-cardinality values in long runs
//               (RUN_LENGTH consecutive rows share the same value).  RLE IS
//               the best choice; the projection should say "RLE wins" so the
//               early-exit must NOT fire, preserving compression quality.
//
// Both variants share the same VARCHAR / DOUBLE columns so the only variable
// is the integer cardinality.  Both use file-based storage and run 80 hot
// runs (≈ 2 min wall-clock each).

static constexpr int32_t LINEITEM_ROW_COUNT = 1000000;
static constexpr int32_t LINEITEM_BASE_DATE = 8827; // 1994-01-01 as days since epoch
static constexpr int32_t RUN_LENGTH         = 1000; // rows per run for low-cardinality variant

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
// Shared base — VARCHAR and DOUBLE columns are the same for both variants
// ---------------------------------------------------------------------------
class AppenderLineitemBenchmark : public DuckDBBenchmark {
protected:
	AppenderLineitemBenchmark(bool register_benchmark, const string &name)
	    : DuckDBBenchmark(register_benchmark, name, "[appender_lineitem]") {
	}

	// Subclasses supply the 4 integer values per row (orderkey, partkey,
	// suppkey, linenumber) plus the 3 date integers.
	virtual void AppendIntColumns(Appender &appender, int32_t i) = 0;

public:
	bool InMemory() override {
		return false;
	}

	size_t NRuns() override {
		return 80;
	}

	void Load(DuckDBBenchmarkState *state) override {
		state->conn.Query(CREATE_LINEITEM);
	}

	void RunBenchmark(DuckDBBenchmarkState *state) override {
		Appender appender(state->conn, "lineitem");
		for (int32_t i = 0; i < LINEITEM_ROW_COUNT; i++) {
			const idx_t rf = idx_t(i) % 3;
			const idx_t ls = idx_t(i) % 2;
			const idx_t si = idx_t(i) % 4;
			const idx_t sm = idx_t(i) % 8;
			appender.BeginRow();
			AppendIntColumns(appender, i);
			appender.Append<double>(1.0 + double(i % 50));
			appender.Append<double>(900.0 + double(i % 104949) * 0.01);
			appender.Append<double>(double(i % 11) * 0.01);
			appender.Append<double>(double(i % 9) * 0.01);
			appender.Append<string_t>(string_t(RETURNFLAGS[rf], RETURNFLAG_LENS[rf]));
			appender.Append<string_t>(string_t(LINESTATUS[ls], LINESTATUS_LENS[ls]));
			AppendDateColumns(appender, i);
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
		return "Ingest 1M rows of TPC-H lineitem schema into persistent storage";
	}

private:
	void AppendDateColumns(Appender &appender, int32_t i) {
		// Dates are derived from i — high-cardinality regardless of variant
		appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);
		appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);
		appender.Append<int32_t>(LINEITEM_BASE_DATE + i % 2557);
	}
};

// ---------------------------------------------------------------------------
// Variant 1: HIGH-CARDINALITY integers
// l_orderkey, l_partkey, l_suppkey, l_linenumber span large ranges.
// RLE early-exit should fire — Bitpacking becomes the compressor.
// ---------------------------------------------------------------------------
class AppenderLineitemHighCardIntBenchmark : public AppenderLineitemBenchmark {
	AppenderLineitemHighCardIntBenchmark(bool register_benchmark)
	    : AppenderLineitemBenchmark(register_benchmark, "AppenderLineitemHighCardInt") {
	}

	void AppendIntColumns(Appender &appender, int32_t i) override {
		appender.Append<int32_t>(i / 7 + 1);          // slowly increases — long period
		appender.Append<int32_t>(i % 200000 + 1);     // 200 000 distinct values
		appender.Append<int32_t>(i % 10000 + 1);      // 10 000 distinct values
		appender.Append<int32_t>(i % 7 + 1);          // 7 distinct but cycling per-row
	}

public:
	static AppenderLineitemHighCardIntBenchmark *GetInstance() {
		static AppenderLineitemHighCardIntBenchmark singleton(true);
		auto b = duckdb::unique_ptr<AppenderLineitemHighCardIntBenchmark>(
		    new AppenderLineitemHighCardIntBenchmark(false));
		return &singleton;
	}

	string BenchmarkInfo() override {
		return "Lineitem 1M rows — high-cardinality INT cols: RLE early exit fires, Bitpacking chosen";
	}
};
auto global_instance_AppenderLineitemHighCardInt = AppenderLineitemHighCardIntBenchmark::GetInstance();

// ---------------------------------------------------------------------------
// Variant 2: LOW-CARDINALITY integers
// Integer columns cycle through a small value set with long runs.
// RLE projection says "RLE wins" — early exit must NOT fire.
// Compression quality should be identical to the no-fix baseline.
// ---------------------------------------------------------------------------
class AppenderLineitemLowCardIntBenchmark : public AppenderLineitemBenchmark {
	AppenderLineitemLowCardIntBenchmark(bool register_benchmark)
	    : AppenderLineitemBenchmark(register_benchmark, "AppenderLineitemLowCardInt") {
	}

	void AppendIntColumns(Appender &appender, int32_t i) override {
		// Each value repeats for RUN_LENGTH consecutive rows → long RLE runs
		appender.Append<int32_t>((i / RUN_LENGTH) % 4 + 1);
		appender.Append<int32_t>((i / RUN_LENGTH) % 8 + 1);
		appender.Append<int32_t>((i / RUN_LENGTH) % 5 + 1);
		appender.Append<int32_t>((i / (RUN_LENGTH * 2)) % 4 + 1);
	}

public:
	static AppenderLineitemLowCardIntBenchmark *GetInstance() {
		static AppenderLineitemLowCardIntBenchmark singleton(true);
		auto b = duckdb::unique_ptr<AppenderLineitemLowCardIntBenchmark>(
		    new AppenderLineitemLowCardIntBenchmark(false));
		return &singleton;
	}

	string BenchmarkInfo() override {
		return "Lineitem 1M rows — low-cardinality INT cols (runs of 1000): RLE wins, early exit silent";
	}
};
auto global_instance_AppenderLineitemLowCardInt = AppenderLineitemLowCardIntBenchmark::GetInstance();
