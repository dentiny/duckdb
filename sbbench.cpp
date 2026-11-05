// Sysbench OLTP read workloads (oltp_point_select / oltp_read_only / single range types) for DuckDB and PostgreSQL.
// usage: sbbench <duck|pg> <workload> <clients> <seconds> [duck_threads: 0=default]
// workloads: point_select, read_only, simple_ranges, sum_ranges, order_ranges, distinct_ranges
// env: SB_TABLES (8), SB_ROWS (1000000), SB_DB (duckdb file), SB_PGCONN, SB_WARMUP (s, default 3)
#include "duckdb.hpp"
#include "libpq-fe.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace duckdb;
using clk = std::chrono::steady_clock;

static int NT = 8;
static int64_t ROWS = 1000000;
static const int RANGE = 100;
static const int POINT_SELECTS = 10;

enum Q { POINT = 0, SIMPLE, SUM, ORDER, DISTINCT, NQ };
static const char *QSQL[NQ] = {"SELECT c FROM sbtest%d WHERE id=$1",
                               "SELECT c FROM sbtest%d WHERE id BETWEEN $1 AND $2",
                               "SELECT SUM(k) FROM sbtest%d WHERE id BETWEEN $1 AND $2",
                               "SELECT c FROM sbtest%d WHERE id BETWEEN $1 AND $2 ORDER BY c",
                               "SELECT DISTINCT c FROM sbtest%d WHERE id BETWEEN $1 AND $2 ORDER BY c"};

static std::string Fmt(const char *f, int t) {
	char buf[256];
	snprintf(buf, sizeof(buf), f, t);
	return buf;
}

struct Rng {
	uint64_t s;
	uint64_t Next() {
		s ^= s << 13;
		s ^= s >> 7;
		s ^= s << 17;
		return s;
	}
	int64_t Uniform(int64_t lo, int64_t hi) {
		return lo + (int64_t)(Next() % (uint64_t)(hi - lo + 1));
	}
};

// One client = one connection. Run(q, table, a, b) executes a query and consumes the result; Begin/Commit.
struct Client {
	virtual ~Client() = default;
	virtual int64_t Run(int q, int table, int64_t a, int64_t b) = 0;
	virtual void Begin() = 0;
	virtual void Commit() = 0;
};

struct DuckClient : Client {
	Connection con;
	vector<vector<unique_ptr<PreparedStatement>>> stmts; // [table][q]
	unique_ptr<PreparedStatement> begin, commit;
	vector<Value> vals;
	explicit DuckClient(DuckDB &db) : con(db) {
		stmts.resize(NT + 1);
		for (int t = 1; t <= NT; t++) {
			for (int q = 0; q < NQ; q++) {
				auto p = con.Prepare(Fmt(QSQL[q], t));
				if (p->HasError()) {
					fprintf(stderr, "prepare: %s\n", p->GetError().c_str());
					exit(1);
				}
				stmts[t].push_back(std::move(p));
			}
		}
		begin = con.Prepare("BEGIN TRANSACTION");
		commit = con.Prepare("COMMIT");
	}
	static void Check(QueryResult &r) {
		if (r.HasError()) {
			fprintf(stderr, "ERROR: %s\n", r.GetError().c_str());
			exit(1);
		}
	}
	int64_t Run(int q, int table, int64_t a, int64_t b) override {
		vals.clear();
		vals.push_back(Value::INTEGER((int32_t)a));
		if (q != POINT) {
			vals.push_back(Value::INTEGER((int32_t)b));
		}
		auto r = stmts[table][q]->Execute(vals);
		Check(*r);
		int64_t rows = 0;
		while (auto chunk = r->Fetch()) {
			rows += chunk->size();
			if (chunk->size() > 0 && q != SUM) {
				rows += FlatVector::GetData<string_t>(chunk->data[0])[0].GetSize() > 0 ? 0 : 1;
			}
		}
		return rows;
	}
	void Begin() override {
		vals.clear();
		auto r = begin->Execute(vals);
		Check(*r);
	}
	void Commit() override {
		vals.clear();
		auto r = commit->Execute(vals);
		Check(*r);
	}
};

struct PgClient : Client {
	PGconn *conn;
	explicit PgClient(const char *conninfo) {
		conn = PQconnectdb(conninfo);
		if (PQstatus(conn) != CONNECTION_OK) {
			fprintf(stderr, "pg connect: %s\n", PQerrorMessage(conn));
			exit(1);
		}
		for (int t = 1; t <= NT; t++) {
			for (int q = 0; q < NQ; q++) {
				auto name = "s" + std::to_string(t) + "_" + std::to_string(q);
				PGresult *r = PQprepare(conn, name.c_str(), Fmt(QSQL[q], t).c_str(), q == POINT ? 1 : 2, nullptr);
				if (PQresultStatus(r) != PGRES_COMMAND_OK) {
					fprintf(stderr, "pg prepare: %s\n", PQerrorMessage(conn));
					exit(1);
				}
				PQclear(r);
			}
		}
		for (auto s : {"BEGIN", "COMMIT"}) {
			PGresult *r = PQprepare(conn, s, s, 0, nullptr);
			if (PQresultStatus(r) != PGRES_COMMAND_OK) {
				fprintf(stderr, "pg prepare: %s\n", PQerrorMessage(conn));
				exit(1);
			}
			PQclear(r);
		}
	}
	~PgClient() override {
		PQfinish(conn);
	}
	void ExecNamed(const char *name, int n, const char *const *params) {
		PGresult *r = PQexecPrepared(conn, name, n, params, nullptr, nullptr, 0);
		auto st = PQresultStatus(r);
		if (st != PGRES_TUPLES_OK && st != PGRES_COMMAND_OK) {
			fprintf(stderr, "pg exec: %s\n", PQerrorMessage(conn));
			exit(1);
		}
		PQclear(r);
	}
	int64_t Run(int q, int table, int64_t a, int64_t b) override {
		char name[32], pa[24], pb[24];
		snprintf(name, sizeof(name), "s%d_%d", table, q);
		snprintf(pa, sizeof(pa), "%lld", (long long)a);
		snprintf(pb, sizeof(pb), "%lld", (long long)b);
		const char *params[2] = {pa, pb};
		PGresult *r = PQexecPrepared(conn, name, q == POINT ? 1 : 2, params, nullptr, nullptr, 0);
		if (PQresultStatus(r) != PGRES_TUPLES_OK) {
			fprintf(stderr, "pg exec: %s\n", PQerrorMessage(conn));
			exit(1);
		}
		int64_t rows = PQntuples(r);
		if (rows > 0 && PQgetlength(r, 0, 0) == 0) {
			rows++;
		}
		PQclear(r);
		return rows;
	}
	void Begin() override {
		ExecNamed("BEGIN", 0, nullptr);
	}
	void Commit() override {
		ExecNamed("COMMIT", 0, nullptr);
	}
};

int main(int argc, char **argv) {
	if (argc < 5) {
		fprintf(stderr, "usage: sbbench <duck|pg> <workload> <clients> <seconds> [duck_threads]\n");
		return 1;
	}
	std::string engine = argv[1], workload = argv[2];
	int clients = atoi(argv[3]);
	double seconds = atof(argv[4]);
	int duck_threads = argc > 5 ? atoi(argv[5]) : 0;
	if (getenv("SB_TABLES")) {
		NT = atoi(getenv("SB_TABLES"));
	}
	if (getenv("SB_ROWS")) {
		ROWS = atoll(getenv("SB_ROWS"));
	}
	double warmup = getenv("SB_WARMUP") ? atof(getenv("SB_WARMUP")) : 3.0;
	const char *dbpath = getenv("SB_DB") ? getenv("SB_DB") : "/Users/haojiang/Desktop/oltp_bench_study/02_sysbench_read/sb.duckdb";
	const char *pgconn = getenv("SB_PGCONN") ? getenv("SB_PGCONN") : "host=/tmp port=55441 user=postgres dbname=postgres";

	int single = -1; // -1 = read_only mix, -2 = point_select (autocommit)
	if (workload == "point_select") {
		single = -2;
	} else if (workload == "read_only") {
		single = -1;
	} else if (workload == "simple_ranges") {
		single = SIMPLE;
	} else if (workload == "sum_ranges") {
		single = SUM;
	} else if (workload == "order_ranges") {
		single = ORDER;
	} else if (workload == "distinct_ranges") {
		single = DISTINCT;
	} else {
		fprintf(stderr, "unknown workload\n");
		return 1;
	}

	unique_ptr<DuckDB> db;
	std::string thr_str = "-";
	if (engine == "duck") {
		DBConfig config;
		db = make_uniq<DuckDB>(dbpath, &config);
		Connection con(*db);
		if (duck_threads > 0) {
			con.Query("SET threads=" + std::to_string(duck_threads));
		}
		if (getenv("DUCK_PRE_SQL")) {
			auto r = con.Query(getenv("DUCK_PRE_SQL"));
			if (r->HasError()) {
				fprintf(stderr, "pre sql: %s\n", r->GetError().c_str());
				return 1;
			}
		}
		thr_str = con.Query("SELECT current_setting('threads')")->FetchRaw()->GetValue(0, 0).ToString();
	}

	std::vector<std::unique_ptr<Client>> cl;
	for (int i = 0; i < clients; i++) {
		if (engine == "duck") {
			cl.emplace_back(new DuckClient(*db));
		} else {
			cl.emplace_back(new PgClient(pgconn));
		}
	}

	std::atomic<int> phase {0}; // 0 warmup, 1 measure, 2 stop
	std::vector<std::vector<float>> lat(clients);
	std::vector<int64_t> txns(clients, 0), queries(clients, 0), rowsum(clients, 0);
	std::vector<std::thread> th;
	for (int i = 0; i < clients; i++) {
		th.emplace_back([&, i]() {
			Client &c = *cl[i];
			Rng rng {0x9E3779B97F4A7C15ULL * (i + 1)};
			lat[i].reserve(4000000);
			while (true) {
				int ph = phase.load(std::memory_order_relaxed);
				if (ph == 2) {
					break;
				}
				auto t0 = clk::now();
				int nq = 0;
				int64_t rows = 0;
				if (single == -2) {
					rows += c.Run(POINT, (int)rng.Uniform(1, NT), rng.Uniform(1, ROWS), 0);
					nq = 1;
				} else if (single == -1) {
					c.Begin();
					for (int k = 0; k < POINT_SELECTS; k++) {
						rows += c.Run(POINT, (int)rng.Uniform(1, NT), rng.Uniform(1, ROWS), 0);
					}
					for (int q = SIMPLE; q <= DISTINCT; q++) {
						int64_t a = rng.Uniform(1, ROWS - RANGE + 1);
						rows += c.Run(q, (int)rng.Uniform(1, NT), a, a + RANGE - 1);
					}
					c.Commit();
					nq = POINT_SELECTS + 4 + 2;
				} else {
					int64_t a = rng.Uniform(1, ROWS - RANGE + 1);
					rows += c.Run(single, (int)rng.Uniform(1, NT), a, a + RANGE - 1);
					nq = 1;
				}
				auto t1 = clk::now();
				if (ph == 1) {
					lat[i].push_back(std::chrono::duration<float, std::micro>(t1 - t0).count());
					txns[i]++;
					queries[i] += nq;
					rowsum[i] += rows;
				}
			}
		});
	}
	fprintf(stderr, "pid %d %s %s clients=%d threads=%s\n", getpid(), engine.c_str(), workload.c_str(), clients,
	        thr_str.c_str());
	std::this_thread::sleep_for(std::chrono::duration<double>(warmup));
	auto start = clk::now();
	phase = 1;
	if (getenv("SB_READY_FILE")) {
		FILE *f = fopen(getenv("SB_READY_FILE"), "w");
		fprintf(f, "%d\n", getpid());
		fclose(f);
	}
	std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
	phase = 2;
	auto el = std::chrono::duration<double>(clk::now() - start).count();
	for (auto &t : th) {
		t.join();
	}
	std::vector<float> all;
	int64_t T = 0, Qn = 0, R = 0;
	for (int i = 0; i < clients; i++) {
		all.insert(all.end(), lat[i].begin(), lat[i].end());
		T += txns[i];
		Qn += queries[i];
		R += rowsum[i];
	}
	std::sort(all.begin(), all.end());
	auto pct = [&](double p) {
		return all.empty() ? 0.0 : (double)all[std::min(all.size() - 1, (size_t)(p * all.size()))];
	};
	double avg = 0;
	for (auto v : all) {
		avg += v;
	}
	avg /= std::max<size_t>(1, all.size());
	// engine,workload,clients,threads,tps,qps,avg_us,p50_us,p99_us,txns,seconds,rows_per_txn
	printf("%s,%s,%d,%s,%.1f,%.1f,%.1f,%.1f,%.1f,%lld,%.2f,%.2f\n", engine.c_str(), workload.c_str(), clients,
	       thr_str.c_str(), T / el, Qn / el, avg, pct(0.5), pct(0.99), (long long)T, el, T ? double(R) / T : 0.0);
	fflush(stdout);
	_exit(0);
}
