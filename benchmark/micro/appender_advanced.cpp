#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

// Benchmarks Appender ingestion with a workload designed to exercise the
// compression detection code paths that appender_lineitem.cpp does NOT cover:
//
//   - INTEGER columns with SMALL cardinality (only a handful of distinct
//     values with long repeated runs) — tests whether the RLE early-exit
//     is transparent when RLE is actually a good choice.
//
//   - VARCHAR columns with LONG strings (avg > 100 bytes) — tests whether
//     the FSST short-string skip is transparent when FSST can genuinely
//     compress the data.
//
// Expected behaviour after optimisations C and D:
//   - RLE early exit should NOT fire here (low cardinality → seen_count
//     stays small → projection says RLE is fine → no early exit).
//   - FSST short-string skip should NOT fire here (avg length > 10 bytes
//     → the threshold is not reached → FSST analysis runs normally).
//
// Therefore these benchmarks should show no regression vs baseline, proving
// the optimisations are transparent for workloads that DO benefit from RLE
// or FSST.

static constexpr int32_t ADV_ROW_COUNT = 1000000;

// ---------------------------------------------------------------------------
// Long VARCHAR strings (avg ~130 bytes) — representative of log lines,
// URLs, or structured text where FSST can exploit repeated sub-patterns.
// ---------------------------------------------------------------------------
static const char *const LONG_STRINGS[] = {
    // ~100–160 chars each; share common prefixes/suffixes so FSST can build
    // a useful symbol table
    "https://example.com/api/v2/users/search?query=active&limit=100&offset=0&sort=created_at&order=desc",
    "https://example.com/api/v2/products/list?category=electronics&in_stock=true&min_price=10&max_price=500",
    "https://example.com/api/v2/orders/history?user_id=42&status=completed&from=2024-01-01&to=2024-12-31",
    "https://example.com/api/v2/reports/summary?type=monthly&department=sales&year=2024&format=json",
    "/var/log/app/service_2024_01.log: [INFO] Request processed successfully in 42ms for endpoint /health",
    "/var/log/app/service_2024_01.log: [WARN] Response time exceeded threshold: 1523ms for endpoint /search",
    "/var/log/app/service_2024_01.log: [ERROR] Database connection timeout after 30s, retrying attempt 3/5",
    "/var/log/app/service_2024_01.log: [DEBUG] Cache miss for key user_session_abc123, fetching from database",
};
static constexpr idx_t LONG_STRING_COUNT = 8;

// Small pre-computed lengths to avoid calling strlen in the hot loop
static const uint32_t LONG_STRING_LENS[] = {97, 103, 101, 98, 101, 103, 101, 103};

// ---------------------------------------------------------------------------
// Low-cardinality INTEGER values (4 distinct values, long repeated runs)
// — representative of status codes, category IDs, priority levels, etc.
// RLE should be a strong candidate for these columns.
// ---------------------------------------------------------------------------
static constexpr int32_t STATUS_VALS[]   = {1, 2, 3, 4};          // 4 distinct
static constexpr int32_t PRIORITY_VALS[] = {1, 2, 3, 4, 5};       // 5 distinct
static constexpr int32_t REGION_VALS[]   = {10, 20, 30, 40, 50, 60, 70, 80}; // 8 distinct
// Each value repeats for a long run (1000 rows) to make RLE clearly worthwhile
static constexpr int32_t RUN_LENGTH = 1000;

static const char CREATE_ADV_TABLE[] =
    "CREATE TABLE adv_workload("
    // Low-cardinality integer columns (RLE-friendly)
    "status_code INTEGER, priority INTEGER, region_id INTEGER, category_id INTEGER,"
    // Low-cardinality doubles (also RLE-friendly: small set of values, long runs)
    "score DOUBLE, multiplier DOUBLE,"
    // Long VARCHAR columns (FSST-friendly)
    "request_url VARCHAR, log_line VARCHAR, description VARCHAR, notes VARCHAR)";

// ---------------------------------------------------------------------------
// Base benchmark class
// ---------------------------------------------------------------------------
class AppenderAdvancedBenchmark : public DuckDBBenchmark {
protected:
	AppenderAdvancedBenchmark(bool register_benchmark, const string &name)
	    : DuckDBBenchmark(register_benchmark, name, "[appender_advanced]") {
	}

public:
	bool InMemory() override {
		return false;
	}

	size_t NRuns() override {
		return 80;
	}

	void Load(DuckDBBenchmarkState *state) override {
		state->conn.Query(CREATE_ADV_TABLE);
	}

	void RunBenchmark(DuckDBBenchmarkState *state) override {
		Appender appender(state->conn, "adv_workload");
		for (int32_t i = 0; i < ADV_ROW_COUNT; i++) {
			// Low-cardinality integers: value changes every RUN_LENGTH rows
			// → long runs of the same value → RLE should win
			int32_t status   = STATUS_VALS[(i / RUN_LENGTH) % 4];
			int32_t priority = PRIORITY_VALS[(i / RUN_LENGTH) % 5];
			int32_t region   = REGION_VALS[(i / RUN_LENGTH) % 8];
			int32_t category = STATUS_VALS[(i / (RUN_LENGTH * 2)) % 4];

			// Low-cardinality doubles: same run pattern
			double score      = 1.0 + double((i / RUN_LENGTH) % 5) * 0.25;
			double multiplier = 1.0 + double((i / (RUN_LENGTH * 3)) % 4) * 0.5;

			// Long VARCHAR: cycle through the string pool
			const idx_t url_idx  = idx_t(i) % LONG_STRING_COUNT;
			const idx_t log_idx  = (idx_t(i) + 2) % LONG_STRING_COUNT;
			const idx_t desc_idx = (idx_t(i) + 4) % LONG_STRING_COUNT;
			const idx_t note_idx = (idx_t(i) + 6) % LONG_STRING_COUNT;

			appender.BeginRow();
			appender.Append<int32_t>(status);
			appender.Append<int32_t>(priority);
			appender.Append<int32_t>(region);
			appender.Append<int32_t>(category);
			appender.Append<double>(score);
			appender.Append<double>(multiplier);
			appender.Append<string_t>(string_t(LONG_STRINGS[url_idx], LONG_STRING_LENS[url_idx]));
			appender.Append<string_t>(string_t(LONG_STRINGS[log_idx], LONG_STRING_LENS[log_idx]));
			appender.Append<string_t>(string_t(LONG_STRINGS[desc_idx], LONG_STRING_LENS[desc_idx]));
			appender.Append<string_t>(string_t(LONG_STRINGS[note_idx], LONG_STRING_LENS[note_idx]));
			appender.EndRow();
		}
		appender.Close();
	}

	void Cleanup(DuckDBBenchmarkState *state) override {
		state->conn.Query("DROP TABLE adv_workload");
		Load(state);
	}

	string VerifyResult(QueryResult *result) override {
		return string();
	}

	string BenchmarkInfo() override {
		return "Ingest 1M rows: low-cardinality integers (RLE-friendly) + long VARCHAR strings (FSST-friendly)";
	}
};

// ---------------------------------------------------------------------------
// Variant 1: no primary key
// ---------------------------------------------------------------------------
class AppenderAdvanced1MBenchmark : public AppenderAdvancedBenchmark {
	AppenderAdvanced1MBenchmark(bool register_benchmark)
	    : AppenderAdvancedBenchmark(register_benchmark, "AppenderAdvanced1M") {
	}

public:
	static AppenderAdvanced1MBenchmark *GetInstance() {
		static AppenderAdvanced1MBenchmark singleton(true);
		auto b = duckdb::unique_ptr<AppenderAdvanced1MBenchmark>(new AppenderAdvanced1MBenchmark(false));
		return &singleton;
	}
};
auto global_instance_AppenderAdvanced1M = AppenderAdvanced1MBenchmark::GetInstance();

// ---------------------------------------------------------------------------
// Storage verification benchmark for the advanced workload.
// INTEGER columns have low cardinality (4-8 distinct values, runs of 1000
// rows) so RLE SHOULD win.  Verifies the early exit does NOT fire here.
// ---------------------------------------------------------------------------
class AppenderAdvancedStorageCheckBenchmark : public AppenderAdvancedBenchmark {
	AppenderAdvancedStorageCheckBenchmark(bool register_benchmark)
	    : AppenderAdvancedBenchmark(register_benchmark, "AppenderAdvancedStorageCheck") {
	}

public:
	static AppenderAdvancedStorageCheckBenchmark *GetInstance() {
		static AppenderAdvancedStorageCheckBenchmark singleton(true);
		auto b = duckdb::unique_ptr<AppenderAdvancedStorageCheckBenchmark>(
		    new AppenderAdvancedStorageCheckBenchmark(false));
		return &singleton;
	}

	void Load(DuckDBBenchmarkState *state) override {
		state->conn.Query(CREATE_ADV_TABLE);
		RunBenchmark(state);
		state->conn.Query("CHECKPOINT");
	}

	string GetQuery() override {
		return "SELECT compression, COUNT(*) AS cnt "
		       "FROM pragma_storage_info('adv_workload') "
		       "WHERE segment_type = 'INTEGER' "
		       "GROUP BY compression ORDER BY cnt DESC";
	}

	string VerifyResult(QueryResult *result) override {
		if (!result || result->HasError()) {
			return "storage info query failed";
		}
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			return "no INTEGER segments found";
		}
		// Low-cardinality integers (runs of 1000) — RLE must be the top compressor
		auto compression = chunk->data[0].GetValue(0).ToString();
		if (compression != "RLE") {
			return "INTEGER columns are not using RLE (got: " + compression + ") — "
			       "RLE early exit may have fired incorrectly for low-cardinality data";
		}
		return string();
	}

	string BenchmarkInfo() override {
		return "Verify INTEGER compression types after 1M-row advanced workload ingestion";
	}
};
auto global_instance_AppenderAdvancedStorageCheck = AppenderAdvancedStorageCheckBenchmark::GetInstance();

// ---------------------------------------------------------------------------
// Compression detection microbenchmark for the advanced workload.
//
// Times only COMMIT (= LocalStorage::Flush → ColumnDataCheckpointer) for
// exactly one row group (DEFAULT_ROW_GROUP_SIZE rows).  Compare with the
// *Uncompressed variant to isolate pure detection overhead.
// ---------------------------------------------------------------------------
class CompressionDetectAdvancedBenchmark : public DuckDBBenchmark {
protected:
	CompressionDetectAdvancedBenchmark(bool register_benchmark, const string &name)
	    : DuckDBBenchmark(register_benchmark, name, "[compression_detect_advanced]") {
	}

	virtual string ForceCompressionSQL() {
		return "";
	}

public:
	bool InMemory() override {
		return false;
	}

	void Load(DuckDBBenchmarkState *state) override {
		auto force = ForceCompressionSQL();
		if (!force.empty()) {
			state->conn.Query(force);
		}
		state->conn.Query(CREATE_ADV_TABLE);
		state->conn.Query("BEGIN TRANSACTION");
		AppendOneRowGroup(state);
	}

	void AppendOneRowGroup(DuckDBBenchmarkState *state) {
		Appender appender(state->conn, "adv_workload");
		for (int32_t i = 0; i < (int32_t)DEFAULT_ROW_GROUP_SIZE; i++) {
			int32_t status   = STATUS_VALS[(i / RUN_LENGTH) % 4];
			int32_t priority = PRIORITY_VALS[(i / RUN_LENGTH) % 5];
			int32_t region   = REGION_VALS[(i / RUN_LENGTH) % 8];
			int32_t category = STATUS_VALS[(i / (RUN_LENGTH * 2)) % 4];
			double score      = 1.0 + double((i / RUN_LENGTH) % 5) * 0.25;
			double multiplier = 1.0 + double((i / (RUN_LENGTH * 3)) % 4) * 0.5;
			const idx_t url_idx  = idx_t(i) % LONG_STRING_COUNT;
			const idx_t log_idx  = (idx_t(i) + 2) % LONG_STRING_COUNT;
			const idx_t desc_idx = (idx_t(i) + 4) % LONG_STRING_COUNT;
			const idx_t note_idx = (idx_t(i) + 6) % LONG_STRING_COUNT;
			appender.BeginRow();
			appender.Append<int32_t>(status);
			appender.Append<int32_t>(priority);
			appender.Append<int32_t>(region);
			appender.Append<int32_t>(category);
			appender.Append<double>(score);
			appender.Append<double>(multiplier);
			appender.Append<string_t>(string_t(LONG_STRINGS[url_idx], LONG_STRING_LENS[url_idx]));
			appender.Append<string_t>(string_t(LONG_STRINGS[log_idx], LONG_STRING_LENS[log_idx]));
			appender.Append<string_t>(string_t(LONG_STRINGS[desc_idx], LONG_STRING_LENS[desc_idx]));
			appender.Append<string_t>(string_t(LONG_STRINGS[note_idx], LONG_STRING_LENS[note_idx]));
			appender.EndRow();
		}
		appender.Close();
	}

	void RunBenchmark(DuckDBBenchmarkState *state) override {
		state->conn.Query("COMMIT");
	}

	void Cleanup(DuckDBBenchmarkState *state) override {
		state->conn.Query("DROP TABLE adv_workload");
		state->conn.Query(CREATE_ADV_TABLE);
		state->conn.Query("BEGIN TRANSACTION");
		AppendOneRowGroup(state);
	}

	string VerifyResult(QueryResult *result) override {
		return string();
	}

	string BenchmarkInfo() override {
		return "Compression detection for 1 row group of advanced workload (low-cardinality INT + long VARCHAR)";
	}
};

class CompressionDetectAdvancedAuto : public CompressionDetectAdvancedBenchmark {
	CompressionDetectAdvancedAuto(bool register_benchmark)
	    : CompressionDetectAdvancedBenchmark(register_benchmark, "CompressionDetectAdvancedAuto") {
	}

public:
	static CompressionDetectAdvancedAuto *GetInstance() {
		static CompressionDetectAdvancedAuto singleton(true);
		auto b = duckdb::unique_ptr<CompressionDetectAdvancedAuto>(new CompressionDetectAdvancedAuto(false));
		return &singleton;
	}
};
auto global_instance_CompressionDetectAdvancedAuto = CompressionDetectAdvancedAuto::GetInstance();

class CompressionDetectAdvancedUncompressed : public CompressionDetectAdvancedBenchmark {
	CompressionDetectAdvancedUncompressed(bool register_benchmark)
	    : CompressionDetectAdvancedBenchmark(register_benchmark, "CompressionDetectAdvancedUncompressed") {
	}

public:
	static CompressionDetectAdvancedUncompressed *GetInstance() {
		static CompressionDetectAdvancedUncompressed singleton(true);
		auto b = duckdb::unique_ptr<CompressionDetectAdvancedUncompressed>(
		    new CompressionDetectAdvancedUncompressed(false));
		return &singleton;
	}

	string ForceCompressionSQL() override {
		return "SET force_compression='uncompressed'";
	}

	string BenchmarkInfo() override {
		return "Compression write-only baseline for advanced workload (no detection)";
	}
};
auto global_instance_CompressionDetectAdvancedUncompressed = CompressionDetectAdvancedUncompressed::GetInstance();
