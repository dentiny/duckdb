// index_bench load <db>
// index_bench run <db> <case> <iters> [threads]   (READY_FILE=<file> is created after warm-up, for `sample`)
#include "duckdb.hpp"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <random>
#include <string>

using namespace duckdb;
using Clock = std::chrono::steady_clock;

static constexpr int64_t ROWS = 4000000;
static constexpr int64_t K_MOD = 100000000;

// k is a permutation of [0, K_MOD) restricted to the first ROWS ids, so it is unique and uncorrelated with id.
static int64_t KeyOf(int64_t id) {
	return id * 7919 % K_MOD;
}

struct Case {
	std::string sql;
	std::function<vector<Value>(std::mt19937_64 &)> params;
};

int main(int argc, char **argv) {
	if (argc < 3) {
		fprintf(stderr, "usage: index_bench load <db> | run <db> <case> <iters> [threads]\n");
		return 1;
	}
	std::string mode = argv[1];
	DBConfig config;
	if (mode == "run" && argc > 5) {
		config.options.maximum_threads = std::stoull(argv[5]);
	}
	DuckDB db(argv[2], &config);
	Connection con(db);

	if (mode == "load") {
		auto r = con.Query("CREATE TABLE t AS SELECT i AS id, i * 7919 % 100000000 AS k, i % 100 AS v, 'pad' || i AS pad "
		                   "FROM range(4000000) r(i)");
		if (r->HasError()) {
			fprintf(stderr, "%s\n", r->GetError().c_str());
			return 1;
		}
		con.Query("CREATE INDEX idx_id ON t(id)");
		con.Query("CREATE INDEX idx_k ON t(k)");
		con.Query("CHECKPOINT");
		return 0;
	}

	std::uniform_int_distribution<int64_t> any_id(0, ROWS - 1000);
	std::uniform_int_distribution<int64_t> any_v(0, 99);
	std::uniform_int_distribution<int64_t> any_k(0, K_MOD - 2000);
	std::map<std::string, Case> cases {
	    // Range on the indexed column plus a predicate the ART cannot use.
	    {"k_range_ne",
	     {"SELECT count(*), sum(v) FROM t WHERE k > $1 AND k < $2 AND k <> $3",
	      [&](std::mt19937_64 &rng) {
		      auto lo = any_k(rng);
		      return vector<Value> {Value::BIGINT(lo), Value::BIGINT(lo + 1000), Value::BIGINT(lo + 500)};
	      }}},
	    // Point lookup on the indexed column plus a filter on another column.
	    {"k_point_other_col",
	     {"SELECT id, pad FROM t WHERE k = $1 AND v <> $2",
	      [&](std::mt19937_64 &rng) {
		      return vector<Value> {Value::BIGINT(KeyOf(any_id(rng))), Value::BIGINT(any_v(rng))};
	      }}},
	    {"id_point_other_col",
	     {"SELECT pad FROM t WHERE id = $1 AND v <> $2",
	      [&](std::mt19937_64 &rng) {
		      return vector<Value> {Value::BIGINT(any_id(rng)), Value::BIGINT(any_v(rng))};
	      }}},
	    // Sysbench-like 100-row range on the clustered key plus a filter on another column.
	    {"id_range_other_col",
	     {"SELECT sum(v) FROM t WHERE id BETWEEN $1 AND $2 AND v <> $3",
	      [&](std::mt19937_64 &rng) {
		      auto lo = any_id(rng);
		      return vector<Value> {Value::BIGINT(lo), Value::BIGINT(lo + 99), Value::BIGINT(any_v(rng))};
	      }}},
	    // Control: already served by the index on main.
	    {"k_range_exact",
	     {"SELECT count(*), sum(v) FROM t WHERE k > $1 AND k < $2",
	      [&](std::mt19937_64 &rng) {
		      auto lo = any_k(rng);
		      return vector<Value> {Value::BIGINT(lo), Value::BIGINT(lo + 1000)};
	      }}},
	};
	auto it = cases.find(argv[3]);
	if (it == cases.end()) {
		fprintf(stderr, "unknown case %s\n", argv[3]);
		return 1;
	}
	auto iters = std::stoll(argv[4]);
	auto stmt = con.Prepare(it->second.sql);
	if (stmt->HasError()) {
		fprintf(stderr, "%s\n", stmt->GetError().c_str());
		return 1;
	}
	std::mt19937_64 rng(42);
	int64_t checksum = 0;
	auto run = [&](int64_t n) {
		for (int64_t i = 0; i < n; i++) {
			auto params = it->second.params(rng);
			auto result = stmt->Execute(params);
			if (result->HasError()) {
				fprintf(stderr, "%s\n", result->GetError().c_str());
				exit(1);
			}
			while (auto chunk = result->Fetch()) {
				checksum += static_cast<int64_t>(chunk->size());
				for (idx_t row = 0; row < chunk->size(); row++) {
					checksum += static_cast<int64_t>(chunk->GetValue(0, row).Hash() % 1000003);
				}
			}
		}
	};
	run(iters / 10);
	if (auto ready = getenv("READY_FILE")) {
		std::ofstream(ready) << "ready";
	}
	checksum = 0;
	auto start = Clock::now();
	run(iters);
	double us = std::chrono::duration<double, std::micro>(Clock::now() - start).count() / double(iters);
	printf("%s,%.2f,%lld\n", argv[3], us, static_cast<long long>(checksum));
	return 0;
}
