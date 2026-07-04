#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/storage_info.hpp"

using namespace duckdb;

// Microbenchmark for ColumnDataCheckpointer::DetectBestCompressionMethod.
//
// Pattern: RunBenchmark times ONLY the COMMIT step, which triggers
//   LocalStorage::Flush → OptimisticDataWriter::WriteUnflushedRowGroups
//     → RowGroup::WriteToDisk → ColumnDataCheckpointer::DetectBestCompressionMethod
//
// Since detection accounts for ~98% of WriteToDisk cost, this effectively
// benchmarks DetectBestCompressionMethod in isolation.
//
// Each run commits exactly one row group (DEFAULT_ROW_GROUP_SIZE = 122,880 rows).
// Cleanup re-arms the next run: drops the table, recreates it, opens a transaction,
// and appends one row group without committing so RunBenchmark can commit it.
//
// Variants:
//   *Auto         — force_compression='auto' (all analyzers compete; default)
//   *Uncompressed — force_compression='uncompressed' (single trivial candidate;
//                   measures write-only baseline, no real detection work)
//   *Cached       — force_compression='auto' but detection cache warm from run 1
//                   (TODO: needs cache to persist across COMMIT boundaries)
//
// Column type suites:
//   IntOnly    — 16 x INTEGER  (Bitpacking, RLE, Uncompressed)
//   DoubleOnly — 16 x DOUBLE   (ALP, RLE, Uncompressed)
//   VarcharOnly— 16 x VARCHAR  (Dictionary, FSST, Uncompressed)
//   Mixed      — same 16-column TPC-H lineitem schema as appender_lineitem.cpp

static constexpr idx_t DETECTION_ROW_COUNT = DEFAULT_ROW_GROUP_SIZE; // 122,880 rows = exactly 1 row group

// ---------------------------------------------------------------------------
// Shared string data for VARCHAR columns
// ---------------------------------------------------------------------------
static const char *const CD_RETURNFLAGS[] = {"A", "N", "R"};
static const uint32_t CD_RETURNFLAG_LENS[] = {1, 1, 1};
static const char *const CD_SHIPMODE[] = {"AIR", "FOB", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "AIR REG"};
static const uint32_t CD_SHIPMODE_LENS[] = {3, 3, 4, 4, 4, 5, 7, 7};
static const char CD_COMMENT[] = "regular annual package";
static constexpr uint32_t CD_COMMENT_LEN = 22;

// ---------------------------------------------------------------------------
// Base class: manages the COMMIT-only RunBenchmark + table reset in Cleanup
// ---------------------------------------------------------------------------
class CompressionDetectBenchmark : public DuckDBBenchmark {
protected:
	CompressionDetectBenchmark(bool register_benchmark, const string &name)
	    : DuckDBBenchmark(register_benchmark, name, "[compression_detect]") {
	}

	// Subclasses fill the table schema.
	virtual string CreateTableSQL() = 0;
	// Subclasses append exactly DETECTION_ROW_COUNT rows via DirectAppender.
	virtual void AppendRows(DuckDBBenchmarkState *state) = 0;
	// Subclasses may set force_compression before the table is created.
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
		state->conn.Query(CreateTableSQL());
		// Open a transaction and append one row group without committing.
		// RunBenchmark will commit it, triggering DetectBestCompressionMethod.
		state->conn.Query("BEGIN TRANSACTION");
		AppendRows(state);
	}

	void RunBenchmark(DuckDBBenchmarkState *state) override {
		// This COMMIT is the hot path: it triggers LocalStorage::Flush
		// → ColumnDataCheckpointer::DetectBestCompressionMethod for every column.
		state->conn.Query("COMMIT");
	}

	void Cleanup(DuckDBBenchmarkState *state) override {
		state->conn.Query("DROP TABLE detect_bench");
		state->conn.Query(CreateTableSQL());
		// Re-arm: begin a new transaction and append one row group so the
		// next RunBenchmark call has something to commit.
		state->conn.Query("BEGIN TRANSACTION");
		AppendRows(state);
	}

	string VerifyResult(QueryResult *result) override {
		return string();
	}
};

// ---------------------------------------------------------------------------
// Integer-only suite (16 x INTEGER) — exercises Bitpacking, RLE, Uncompressed
// ---------------------------------------------------------------------------
#define INT_ONLY_SCHEMA                                                                                                \
	"CREATE TABLE detect_bench("                                                                                       \
	"c0 INTEGER, c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER,"                  \
	"c8 INTEGER, c9 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER)"

class CompressionDetectIntOnlyBase : public CompressionDetectBenchmark {
protected:
	CompressionDetectIntOnlyBase(bool register_benchmark, const string &name)
	    : CompressionDetectBenchmark(register_benchmark, name) {
	}

	string CreateTableSQL() override {
		return INT_ONLY_SCHEMA;
	}

	void AppendRows(DuckDBBenchmarkState *state) override {
		DirectAppender appender(state->conn, "detect_bench");
		for (idx_t i = 0; i < DETECTION_ROW_COUNT; i++) {
			appender.BeginRow();
			for (int col = 0; col < 16; col++) {
				appender.Append<int32_t>(NumericCast<int32_t>(i % 10000 + col));
			}
			appender.EndRow();
		}
		appender.Close();
	}

	string BenchmarkInfo() override {
		return "Compression detection for 1 row group of 16 x INTEGER columns";
	}
};

class CompressionDetectIntOnlyAuto : public CompressionDetectIntOnlyBase {
	CompressionDetectIntOnlyAuto(bool register_benchmark)
	    : CompressionDetectIntOnlyBase(register_benchmark, "CompressionDetectIntOnlyAuto") {
	}

public:
	static CompressionDetectIntOnlyAuto *GetInstance() {
		static CompressionDetectIntOnlyAuto singleton(true);
		auto b = duckdb::unique_ptr<CompressionDetectIntOnlyAuto>(new CompressionDetectIntOnlyAuto(false));
		return &singleton;
	}
};
auto global_instance_CompressionDetectIntOnlyAuto = CompressionDetectIntOnlyAuto::GetInstance();

class CompressionDetectIntOnlyUncompressed : public CompressionDetectIntOnlyBase {
	CompressionDetectIntOnlyUncompressed(bool register_benchmark)
	    : CompressionDetectIntOnlyBase(register_benchmark, "CompressionDetectIntOnlyUncompressed") {
	}

public:
	static CompressionDetectIntOnlyUncompressed *GetInstance() {
		static CompressionDetectIntOnlyUncompressed singleton(true);
		auto b =
		    duckdb::unique_ptr<CompressionDetectIntOnlyUncompressed>(new CompressionDetectIntOnlyUncompressed(false));
		return &singleton;
	}

	string ForceCompressionSQL() override {
		return "SET force_compression='uncompressed'";
	}

	string BenchmarkInfo() override {
		return "Compression write (no detection) for 1 row group of 16 x INTEGER — forced uncompressed baseline";
	}
};
auto global_instance_CompressionDetectIntOnlyUncompressed = CompressionDetectIntOnlyUncompressed::GetInstance();

// ---------------------------------------------------------------------------
// Double-only suite (16 x DOUBLE) — exercises ALP, RLE, Uncompressed
// ---------------------------------------------------------------------------
#define DOUBLE_ONLY_SCHEMA                                                                                             \
	"CREATE TABLE detect_bench("                                                                                       \
	"c0 DOUBLE, c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 DOUBLE, c5 DOUBLE, c6 DOUBLE, c7 DOUBLE,"                         \
	"c8 DOUBLE, c9 DOUBLE, c10 DOUBLE, c11 DOUBLE, c12 DOUBLE, c13 DOUBLE, c14 DOUBLE, c15 DOUBLE)"

class CompressionDetectDoubleOnlyBase : public CompressionDetectBenchmark {
protected:
	CompressionDetectDoubleOnlyBase(bool register_benchmark, const string &name)
	    : CompressionDetectBenchmark(register_benchmark, name) {
	}

	string CreateTableSQL() override {
		return DOUBLE_ONLY_SCHEMA;
	}

	void AppendRows(DuckDBBenchmarkState *state) override {
		DirectAppender appender(state->conn, "detect_bench");
		for (idx_t i = 0; i < DETECTION_ROW_COUNT; i++) {
			appender.BeginRow();
			for (int col = 0; col < 16; col++) {
				appender.Append<double>(1.0 + double(i % 50) + col * 0.01);
			}
			appender.EndRow();
		}
		appender.Close();
	}

	string BenchmarkInfo() override {
		return "Compression detection for 1 row group of 16 x DOUBLE columns";
	}
};

class CompressionDetectDoubleOnlyAuto : public CompressionDetectDoubleOnlyBase {
	CompressionDetectDoubleOnlyAuto(bool register_benchmark)
	    : CompressionDetectDoubleOnlyBase(register_benchmark, "CompressionDetectDoubleOnlyAuto") {
	}

public:
	static CompressionDetectDoubleOnlyAuto *GetInstance() {
		static CompressionDetectDoubleOnlyAuto singleton(true);
		auto b =
		    duckdb::unique_ptr<CompressionDetectDoubleOnlyAuto>(new CompressionDetectDoubleOnlyAuto(false));
		return &singleton;
	}
};
auto global_instance_CompressionDetectDoubleOnlyAuto = CompressionDetectDoubleOnlyAuto::GetInstance();

class CompressionDetectDoubleOnlyUncompressed : public CompressionDetectDoubleOnlyBase {
	CompressionDetectDoubleOnlyUncompressed(bool register_benchmark)
	    : CompressionDetectDoubleOnlyBase(register_benchmark, "CompressionDetectDoubleOnlyUncompressed") {
	}

public:
	static CompressionDetectDoubleOnlyUncompressed *GetInstance() {
		static CompressionDetectDoubleOnlyUncompressed singleton(true);
		auto b = duckdb::unique_ptr<CompressionDetectDoubleOnlyUncompressed>(
		    new CompressionDetectDoubleOnlyUncompressed(false));
		return &singleton;
	}

	string ForceCompressionSQL() override {
		return "SET force_compression='uncompressed'";
	}

	string BenchmarkInfo() override {
		return "Compression write (no detection) for 1 row group of 16 x DOUBLE — forced uncompressed baseline";
	}
};
auto global_instance_CompressionDetectDoubleOnlyUncompressed = CompressionDetectDoubleOnlyUncompressed::GetInstance();

// ---------------------------------------------------------------------------
// VARCHAR-only suite (16 x VARCHAR) — exercises Dictionary, FSST, Uncompressed
// ---------------------------------------------------------------------------
#define VARCHAR_ONLY_SCHEMA                                                                                            \
	"CREATE TABLE detect_bench("                                                                                       \
	"c0 VARCHAR, c1 VARCHAR, c2 VARCHAR, c3 VARCHAR, c4 VARCHAR, c5 VARCHAR, c6 VARCHAR, c7 VARCHAR,"                   \
	"c8 VARCHAR, c9 VARCHAR, c10 VARCHAR, c11 VARCHAR, c12 VARCHAR, c13 VARCHAR, c14 VARCHAR, c15 VARCHAR)"

class CompressionDetectVarcharOnlyBase : public CompressionDetectBenchmark {
protected:
	CompressionDetectVarcharOnlyBase(bool register_benchmark, const string &name)
	    : CompressionDetectBenchmark(register_benchmark, name) {
	}

	string CreateTableSQL() override {
		return VARCHAR_ONLY_SCHEMA;
	}

	void AppendRows(DuckDBBenchmarkState *state) override {
		DirectAppender appender(state->conn, "detect_bench");
		for (idx_t i = 0; i < DETECTION_ROW_COUNT; i++) {
			const idx_t rf = i % 3;
			const idx_t sm = i % 8;
			appender.BeginRow();
			// Mix of low-cardinality short strings (like returnflag) and longer strings
			for (int col = 0; col < 8; col++) {
				appender.Append<string_t>(string_t(CD_RETURNFLAGS[rf], CD_RETURNFLAG_LENS[rf]));
			}
			for (int col = 0; col < 7; col++) {
				appender.Append<string_t>(string_t(CD_SHIPMODE[sm], CD_SHIPMODE_LENS[sm]));
			}
			appender.Append<string_t>(string_t(CD_COMMENT, CD_COMMENT_LEN));
			appender.EndRow();
		}
		appender.Close();
	}

	string BenchmarkInfo() override {
		return "Compression detection for 1 row group of 16 x VARCHAR columns";
	}
};

class CompressionDetectVarcharOnlyAuto : public CompressionDetectVarcharOnlyBase {
	CompressionDetectVarcharOnlyAuto(bool register_benchmark)
	    : CompressionDetectVarcharOnlyBase(register_benchmark, "CompressionDetectVarcharOnlyAuto") {
	}

public:
	static CompressionDetectVarcharOnlyAuto *GetInstance() {
		static CompressionDetectVarcharOnlyAuto singleton(true);
		auto b =
		    duckdb::unique_ptr<CompressionDetectVarcharOnlyAuto>(new CompressionDetectVarcharOnlyAuto(false));
		return &singleton;
	}
};
auto global_instance_CompressionDetectVarcharOnlyAuto = CompressionDetectVarcharOnlyAuto::GetInstance();

class CompressionDetectVarcharOnlyUncompressed : public CompressionDetectVarcharOnlyBase {
	CompressionDetectVarcharOnlyUncompressed(bool register_benchmark)
	    : CompressionDetectVarcharOnlyBase(register_benchmark, "CompressionDetectVarcharOnlyUncompressed") {
	}

public:
	static CompressionDetectVarcharOnlyUncompressed *GetInstance() {
		static CompressionDetectVarcharOnlyUncompressed singleton(true);
		auto b = duckdb::unique_ptr<CompressionDetectVarcharOnlyUncompressed>(
		    new CompressionDetectVarcharOnlyUncompressed(false));
		return &singleton;
	}

	string ForceCompressionSQL() override {
		return "SET force_compression='uncompressed'";
	}

	string BenchmarkInfo() override {
		return "Compression write (no detection) for 1 row group of 16 x VARCHAR — forced uncompressed baseline";
	}
};
auto global_instance_CompressionDetectVarcharOnlyUncompressed =
    CompressionDetectVarcharOnlyUncompressed::GetInstance();

// ---------------------------------------------------------------------------
// Mixed suite — 16-column TPC-H lineitem schema (same as appender_lineitem.cpp)
// ---------------------------------------------------------------------------
#define LINEITEM_DETECT_SCHEMA                                                                                         \
	"CREATE TABLE detect_bench("                                                                                       \
	"l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER,"                                   \
	"l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE,"                                      \
	"l_returnflag VARCHAR, l_linestatus VARCHAR,"                                                                       \
	"l_shipdate INTEGER, l_commitdate INTEGER, l_receiptdate INTEGER,"                                                  \
	"l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR)"

static const char *const CD_LINESTATUS[] = {"O", "F"};
static const uint32_t CD_LINESTATUS_LENS[] = {1, 1};
static const char *const CD_SHIPINSTRUCT[] = {"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"};
static const uint32_t CD_SHIPINSTRUCT_LENS[] = {17, 11, 4, 16};
static constexpr int32_t CD_BASE_DATE = 8827;

class CompressionDetectLineitemBase : public CompressionDetectBenchmark {
protected:
	CompressionDetectLineitemBase(bool register_benchmark, const string &name)
	    : CompressionDetectBenchmark(register_benchmark, name) {
	}

	string CreateTableSQL() override {
		return LINEITEM_DETECT_SCHEMA;
	}

	void AppendRows(DuckDBBenchmarkState *state) override {
		DirectAppender appender(state->conn, "detect_bench");
		for (idx_t i = 0; i < DETECTION_ROW_COUNT; i++) {
			const idx_t rf = i % 3;
			const idx_t ls = i % 2;
			const idx_t si = i % 4;
			const idx_t sm = i % 8;
			appender.BeginRow();
			appender.Append<int32_t>(NumericCast<int32_t>(i / 7 + 1));
			appender.Append<int32_t>(NumericCast<int32_t>(i % 200000 + 1));
			appender.Append<int32_t>(NumericCast<int32_t>(i % 10000 + 1));
			appender.Append<int32_t>(NumericCast<int32_t>(i % 7 + 1));
			appender.Append<double>(1.0 + double(i % 50));
			appender.Append<double>(900.0 + double(i % 104949) * 0.01);
			appender.Append<double>(double(i % 11) * 0.01);
			appender.Append<double>(double(i % 9) * 0.01);
			appender.Append<string_t>(string_t(CD_RETURNFLAGS[rf], CD_RETURNFLAG_LENS[rf]));
			appender.Append<string_t>(string_t(CD_LINESTATUS[ls], CD_LINESTATUS_LENS[ls]));
			appender.Append<int32_t>(CD_BASE_DATE + NumericCast<int32_t>(i % 2557));
			appender.Append<int32_t>(CD_BASE_DATE + NumericCast<int32_t>(i % 2557));
			appender.Append<int32_t>(CD_BASE_DATE + NumericCast<int32_t>(i % 2557));
			appender.Append<string_t>(string_t(CD_SHIPINSTRUCT[si], CD_SHIPINSTRUCT_LENS[si]));
			appender.Append<string_t>(string_t(CD_SHIPMODE[sm], CD_SHIPMODE_LENS[sm]));
			appender.Append<string_t>(string_t(CD_COMMENT, CD_COMMENT_LEN));
			appender.EndRow();
		}
		appender.Close();
	}

	string BenchmarkInfo() override {
		return "Compression detection for 1 row group of TPC-H lineitem (16 mixed columns)";
	}
};

class CompressionDetectLineitemAuto : public CompressionDetectLineitemBase {
	CompressionDetectLineitemAuto(bool register_benchmark)
	    : CompressionDetectLineitemBase(register_benchmark, "CompressionDetectLineitemAuto") {
	}

public:
	static CompressionDetectLineitemAuto *GetInstance() {
		static CompressionDetectLineitemAuto singleton(true);
		auto b = duckdb::unique_ptr<CompressionDetectLineitemAuto>(new CompressionDetectLineitemAuto(false));
		return &singleton;
	}
};
auto global_instance_CompressionDetectLineitemAuto = CompressionDetectLineitemAuto::GetInstance();

class CompressionDetectLineitemUncompressed : public CompressionDetectLineitemBase {
	CompressionDetectLineitemUncompressed(bool register_benchmark)
	    : CompressionDetectLineitemBase(register_benchmark, "CompressionDetectLineitemUncompressed") {
	}

public:
	static CompressionDetectLineitemUncompressed *GetInstance() {
		static CompressionDetectLineitemUncompressed singleton(true);
		auto b = duckdb::unique_ptr<CompressionDetectLineitemUncompressed>(
		    new CompressionDetectLineitemUncompressed(false));
		return &singleton;
	}

	string ForceCompressionSQL() override {
		return "SET force_compression='uncompressed'";
	}

	string BenchmarkInfo() override {
		return "Compression write (no detection) for 1 row group of TPC-H lineitem — forced uncompressed baseline";
	}
};
auto global_instance_CompressionDetectLineitemUncompressed = CompressionDetectLineitemUncompressed::GetInstance();

// ---------------------------------------------------------------------------
// Sample-rate variants for CompressionDetectLineitemAuto — tuning the new
// compression_analyze_sample_size setting.
// These show the speed/quality trade-off of analyzing fewer vectors.
// ---------------------------------------------------------------------------
class CompressionDetectLineitemAutoSampleBase : public CompressionDetectLineitemBase {
	string sample_sql;

protected:
	CompressionDetectLineitemAutoSampleBase(bool register_benchmark, const string &name, const string &sql)
	    : CompressionDetectLineitemBase(register_benchmark, name), sample_sql(sql) {
	}

	string ForceCompressionSQL() override {
		return sample_sql;
	}
};

// 50% sample: analyze every other vector (stride=2)
class CompressionDetectLineitemAutoSample50 : public CompressionDetectLineitemAutoSampleBase {
	CompressionDetectLineitemAutoSample50(bool register_benchmark)
	    : CompressionDetectLineitemAutoSampleBase(register_benchmark, "CompressionDetectLineitemAutoSample50",
	                                              "SET compression_analyze_sample_size=0.5") {
	}

public:
	static CompressionDetectLineitemAutoSample50 *GetInstance() {
		static CompressionDetectLineitemAutoSample50 singleton(true);
		auto b = duckdb::unique_ptr<CompressionDetectLineitemAutoSample50>(
		    new CompressionDetectLineitemAutoSample50(false));
		return &singleton;
	}

	string BenchmarkInfo() override {
		return "Detection for 1 row group of TPC-H lineitem, 50% vector sample (stride=2)";
	}
};
auto global_instance_CompressionDetectLineitemAutoSample50 = CompressionDetectLineitemAutoSample50::GetInstance();

// 25% sample: analyze every 4th vector (stride=4) — matches FSST's string sample rate
class CompressionDetectLineitemAutoSample25 : public CompressionDetectLineitemAutoSampleBase {
	CompressionDetectLineitemAutoSample25(bool register_benchmark)
	    : CompressionDetectLineitemAutoSampleBase(register_benchmark, "CompressionDetectLineitemAutoSample25",
	                                              "SET compression_analyze_sample_size=0.25") {
	}

public:
	static CompressionDetectLineitemAutoSample25 *GetInstance() {
		static CompressionDetectLineitemAutoSample25 singleton(true);
		auto b = duckdb::unique_ptr<CompressionDetectLineitemAutoSample25>(
		    new CompressionDetectLineitemAutoSample25(false));
		return &singleton;
	}

	string BenchmarkInfo() override {
		return "Detection for 1 row group of TPC-H lineitem, 25% vector sample (stride=4)";
	}
};
auto global_instance_CompressionDetectLineitemAutoSample25 = CompressionDetectLineitemAutoSample25::GetInstance();