#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

// Integer-only ingestion benchmarks targeting RLE compression behaviour.
//
// HighCardinality  — 8 INTEGER columns filled with sequential / pseudo-random
//                   values (every row is distinct).  RLE cannot compress this
//                   data at all; the early-exit optimisation should fire after
//                   25 % of the row group and skip the remainder of the scan.
//
// LowCardinality   — 8 INTEGER columns filled with 4–8 distinct values in
//                   long runs (RUN_LENGTH rows each).  RLE IS the best choice;
//                   the early-exit projection should say "RLE wins" and NOT
//                   fire, so compression quality is unchanged.
//
// Both variants persist to disk (InMemory() = false) and run 80 hot runs each
// so the median is stable within a 3-minute wall-clock budget.

static constexpr int32_t INT_ROW_COUNT   = 1000000;
static constexpr int32_t INT_RUN_LENGTH  = 1000; // distinct value every N rows (low-cardinality)

static const char CREATE_HIGH_CARD[] =
    "CREATE TABLE int_bench("
    "c0 INTEGER, c1 INTEGER, c2 INTEGER, c3 INTEGER,"
    "c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER)";

static const char CREATE_LOW_CARD[] =
    "CREATE TABLE int_bench("
    "c0 INTEGER, c1 INTEGER, c2 INTEGER, c3 INTEGER,"
    "c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER)";

// ---------------------------------------------------------------------------
// Shared base
// ---------------------------------------------------------------------------
class AppenderIntBenchmark : public DuckDBBenchmark {
protected:
	AppenderIntBenchmark(bool register_benchmark, const string &name)
	    : DuckDBBenchmark(register_benchmark, name, "[appender_int]") {
	}

public:
	bool InMemory() override {
		return false;
	}

	size_t NRuns() override {
		return 80;
	}

	void Cleanup(DuckDBBenchmarkState *state) override {
		state->conn.Query("DROP TABLE int_bench");
		Load(state);
	}

	string VerifyResult(QueryResult *result) override {
		return string();
	}
};

// ---------------------------------------------------------------------------
// High-cardinality: every value is unique → RLE early exit should fire
// ---------------------------------------------------------------------------
class AppenderIntHighCardBenchmark : public AppenderIntBenchmark {
	AppenderIntHighCardBenchmark(bool register_benchmark)
	    : AppenderIntBenchmark(register_benchmark, "AppenderIntHighCard") {
	}

public:
	static AppenderIntHighCardBenchmark *GetInstance() {
		static AppenderIntHighCardBenchmark singleton(true);
		auto b = duckdb::unique_ptr<AppenderIntHighCardBenchmark>(new AppenderIntHighCardBenchmark(false));
		return &singleton;
	}

	void Load(DuckDBBenchmarkState *state) override {
		state->conn.Query(CREATE_HIGH_CARD);
	}

	void RunBenchmark(DuckDBBenchmarkState *state) override {
		Appender appender(state->conn, "int_bench");
		for (int32_t i = 0; i < INT_ROW_COUNT; i++) {
			appender.BeginRow();
			appender.Append<int32_t>(i);                        // sequential — all distinct
			appender.Append<int32_t>(i * 7 + 1);               // different stride
			appender.Append<int32_t>(i % 200000 + 1);          // cycling but high-cardinality
			appender.Append<int32_t>(i % 100000 + 1);
			appender.Append<int32_t>(i / 7 + 1);               // slowly increasing
			appender.Append<int32_t>(i % 10000 + 1);
			appender.Append<int32_t>(i % 50000 + 1);
			appender.Append<int32_t>(i % 7 + 1);               // low-cardinality cycling (7 values)
			appender.EndRow();
		}
		appender.Close();
	}

	string BenchmarkInfo() override {
		return "Ingest 1M rows of 8 high-cardinality INTEGER columns — RLE early exit should fire";
	}
};
auto global_instance_AppenderIntHighCard = AppenderIntHighCardBenchmark::GetInstance();

// ---------------------------------------------------------------------------
// Low-cardinality: long repeated runs → RLE should win, early exit must NOT fire
// ---------------------------------------------------------------------------
class AppenderIntLowCardBenchmark : public AppenderIntBenchmark {
	AppenderIntLowCardBenchmark(bool register_benchmark)
	    : AppenderIntBenchmark(register_benchmark, "AppenderIntLowCard") {
	}

public:
	static AppenderIntLowCardBenchmark *GetInstance() {
		static AppenderIntLowCardBenchmark singleton(true);
		auto b = duckdb::unique_ptr<AppenderIntLowCardBenchmark>(new AppenderIntLowCardBenchmark(false));
		return &singleton;
	}

	void Load(DuckDBBenchmarkState *state) override {
		state->conn.Query(CREATE_LOW_CARD);
	}

	void RunBenchmark(DuckDBBenchmarkState *state) override {
		Appender appender(state->conn, "int_bench");
		for (int32_t i = 0; i < INT_ROW_COUNT; i++) {
			// Each column cycles through a small set with runs of INT_RUN_LENGTH rows
			appender.BeginRow();
			appender.Append<int32_t>((i / INT_RUN_LENGTH) % 4 + 1);
			appender.Append<int32_t>((i / INT_RUN_LENGTH) % 5 + 1);
			appender.Append<int32_t>((i / INT_RUN_LENGTH) % 8 + 1);
			appender.Append<int32_t>((i / INT_RUN_LENGTH) % 3 + 1);
			appender.Append<int32_t>((i / INT_RUN_LENGTH) % 6 + 1);
			appender.Append<int32_t>((i / INT_RUN_LENGTH) % 4 + 10);
			appender.Append<int32_t>((i / (INT_RUN_LENGTH * 2)) % 5 + 1);
			appender.Append<int32_t>((i / (INT_RUN_LENGTH * 3)) % 4 + 1);
			appender.EndRow();
		}
		appender.Close();
	}

	string BenchmarkInfo() override {
		return "Ingest 1M rows of 8 low-cardinality INTEGER columns (runs of 1000) — RLE should win";
	}
};
auto global_instance_AppenderIntLowCard = AppenderIntLowCardBenchmark::GetInstance();
