#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

// Integer-only benchmarks targeting RLE compression behaviour.
//   HighCard — high-cardinality sequential values: RLE early exit fires
//   LowCard  — low-cardinality runs of 1000:       RLE wins, no early exit

static constexpr int32_t INT_ROW_COUNT  = 1000000;
static constexpr int32_t INT_RUN_LENGTH = 1000;

static const char CREATE_INT_BENCH[] =
    "CREATE TABLE int_bench("
    "c0 INTEGER, c1 INTEGER, c2 INTEGER, c3 INTEGER,"
    "c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER)";

class AppenderIntBenchmark : public DuckDBBenchmark {
protected:
	AppenderIntBenchmark(bool r, const string &name)
	    : DuckDBBenchmark(r, name, "[appender_int]") {}
public:
	bool InMemory() override { return false; }
	size_t NRuns() override  { return 250; }
	void Load(DuckDBBenchmarkState *state) override { state->conn.Query(CREATE_INT_BENCH); }
	void Cleanup(DuckDBBenchmarkState *state) override {
		state->conn.Query("DROP TABLE int_bench");
		Load(state);
	}
	string VerifyResult(QueryResult *result) override { return string(); }
};

// High-cardinality: all-distinct sequential values → RLE cannot win
class AppenderIntHighCardBenchmark : public AppenderIntBenchmark {
	AppenderIntHighCardBenchmark(bool r) : AppenderIntBenchmark(r, "AppenderIntHighCard") {}
public:
	static AppenderIntHighCardBenchmark *GetInstance() {
		static AppenderIntHighCardBenchmark s(true);
		auto b = duckdb::unique_ptr<AppenderIntHighCardBenchmark>(new AppenderIntHighCardBenchmark(false));
		return &s;
	}
	void RunBenchmark(DuckDBBenchmarkState *state) override {
		Appender appender(state->conn, "int_bench");
		for (int32_t i = 0; i < INT_ROW_COUNT; i++) {
			appender.BeginRow();
			appender.Append<int32_t>(i);
			appender.Append<int32_t>(i * 7 + 1);
			appender.Append<int32_t>(i % 200000 + 1);
			appender.Append<int32_t>(i % 100000 + 1);
			appender.Append<int32_t>(i / 7 + 1);
			appender.Append<int32_t>(i % 10000 + 1);
			appender.Append<int32_t>(i % 50000 + 1);
			appender.Append<int32_t>(i % 7 + 1);
			appender.EndRow();
		}
		appender.Close();
	}
	string BenchmarkInfo() override { return "1M rows 8×INTEGER high-cardinality — RLE early exit fires"; }
};
auto global_instance_AppenderIntHighCard = AppenderIntHighCardBenchmark::GetInstance();

// Low-cardinality: runs of 1000 → RLE wins, early exit must NOT fire
class AppenderIntLowCardBenchmark : public AppenderIntBenchmark {
	AppenderIntLowCardBenchmark(bool r) : AppenderIntBenchmark(r, "AppenderIntLowCard") {}
public:
	static AppenderIntLowCardBenchmark *GetInstance() {
		static AppenderIntLowCardBenchmark s(true);
		auto b = duckdb::unique_ptr<AppenderIntLowCardBenchmark>(new AppenderIntLowCardBenchmark(false));
		return &s;
	}
	void RunBenchmark(DuckDBBenchmarkState *state) override {
		Appender appender(state->conn, "int_bench");
		for (int32_t i = 0; i < INT_ROW_COUNT; i++) {
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
	string BenchmarkInfo() override { return "1M rows 8×INTEGER low-cardinality runs of 1000 — RLE wins"; }
};
auto global_instance_AppenderIntLowCard = AppenderIntLowCardBenchmark::GetInstance();
