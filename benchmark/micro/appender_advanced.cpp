#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

// Advanced workload: low-cardinality integers + long VARCHAR strings.
// Tests that FSST early-exit and RLE early-exit are transparent for
// workloads that actually benefit from those codecs.

static constexpr int32_t ADV_ROW_COUNT  = 1000000;
static constexpr int32_t ADV_RUN_LENGTH = 1000;

static const char CREATE_ADV_TABLE[] =
    "CREATE TABLE adv_workload("
    "status_code INTEGER, priority INTEGER, region_id INTEGER, category_id INTEGER,"
    "score DOUBLE, multiplier DOUBLE,"
    "request_url VARCHAR, log_line VARCHAR, description VARCHAR, notes VARCHAR)";

static const char *const ADV_LONG_STRINGS[] = {
    "https://example.com/api/v2/users/search?query=active&limit=100&offset=0&sort=created_at&order=desc",
    "https://example.com/api/v2/products/list?category=electronics&in_stock=true&min_price=10&max_price=500",
    "https://example.com/api/v2/orders/history?user_id=42&status=completed&from=2024-01-01&to=2024-12-31",
    "https://example.com/api/v2/reports/summary?type=monthly&department=sales&year=2024&format=json",
    "/var/log/app/service_2024_01.log: [INFO] Request processed successfully in 42ms for endpoint /health",
    "/var/log/app/service_2024_01.log: [WARN] Response time exceeded threshold: 1523ms for endpoint /search",
    "/var/log/app/service_2024_01.log: [ERROR] Database connection timeout after 30s, retrying attempt 3/5",
    "/var/log/app/service_2024_01.log: [DEBUG] Cache miss for key user_session_abc123, fetching from database",
};
static const uint32_t ADV_LONG_STRING_LENS[] = {97, 103, 101, 98, 101, 103, 101, 103};
static constexpr idx_t ADV_LONG_STRING_COUNT  = 8;

static const int32_t ADV_STATUS[]   = {1, 2, 3, 4};
static const int32_t ADV_PRIORITY[] = {1, 2, 3, 4, 5};
static const int32_t ADV_REGION[]   = {10, 20, 30, 40, 50, 60, 70, 80};

class AppenderAdvancedBenchmark : public DuckDBBenchmark {
	AppenderAdvancedBenchmark(bool r) : DuckDBBenchmark(r, "AppenderAdvanced1M", "[appender_advanced]") {}
public:
	static AppenderAdvancedBenchmark *GetInstance() {
		static AppenderAdvancedBenchmark s(true);
		auto b = duckdb::unique_ptr<AppenderAdvancedBenchmark>(new AppenderAdvancedBenchmark(false));
		return &s;
	}
	bool InMemory() override { return false; }
	size_t NRuns() override  { return 250; }

	void Load(DuckDBBenchmarkState *state) override { state->conn.Query(CREATE_ADV_TABLE); }

	void RunBenchmark(DuckDBBenchmarkState *state) override {
		Appender appender(state->conn, "adv_workload");
		for (int32_t i = 0; i < ADV_ROW_COUNT; i++) {
			const idx_t url_idx = idx_t(i)      % ADV_LONG_STRING_COUNT;
			const idx_t log_idx = (idx_t(i) + 2) % ADV_LONG_STRING_COUNT;
			const idx_t dsc_idx = (idx_t(i) + 4) % ADV_LONG_STRING_COUNT;
			const idx_t not_idx = (idx_t(i) + 6) % ADV_LONG_STRING_COUNT;
			appender.BeginRow();
			appender.Append<int32_t>(ADV_STATUS[(i / ADV_RUN_LENGTH) % 4]);
			appender.Append<int32_t>(ADV_PRIORITY[(i / ADV_RUN_LENGTH) % 5]);
			appender.Append<int32_t>(ADV_REGION[(i / ADV_RUN_LENGTH) % 8]);
			appender.Append<int32_t>(ADV_STATUS[(i / (ADV_RUN_LENGTH * 2)) % 4]);
			appender.Append<double>(1.0 + double((i / ADV_RUN_LENGTH) % 5) * 0.25);
			appender.Append<double>(1.0 + double((i / (ADV_RUN_LENGTH * 3)) % 4) * 0.5);
			appender.Append<string_t>(string_t(ADV_LONG_STRINGS[url_idx], ADV_LONG_STRING_LENS[url_idx]));
			appender.Append<string_t>(string_t(ADV_LONG_STRINGS[log_idx], ADV_LONG_STRING_LENS[log_idx]));
			appender.Append<string_t>(string_t(ADV_LONG_STRINGS[dsc_idx], ADV_LONG_STRING_LENS[dsc_idx]));
			appender.Append<string_t>(string_t(ADV_LONG_STRINGS[not_idx], ADV_LONG_STRING_LENS[not_idx]));
			appender.EndRow();
		}
		appender.Close();
	}

	void Cleanup(DuckDBBenchmarkState *state) override {
		state->conn.Query("DROP TABLE adv_workload");
		Load(state);
	}
	string VerifyResult(QueryResult *result) override { return string(); }
	string BenchmarkInfo() override {
		return "1M rows: low-cardinality INT (runs 1000) + long VARCHAR (~100 chars)";
	}
};
auto global_instance_AppenderAdvanced1M = AppenderAdvancedBenchmark::GetInstance();
