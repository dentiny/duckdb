# Reproducing duckdb-internal#9877 Deadlock Locally

## Prerequisites

- DuckDB built from `main` branch (`aee41bf438`)
- httpfs extension built against the same DuckDB version
- Python 3 (for the HTTP server)

## 1. Build DuckDB (main branch, reldebug)

```bash
cd ~/Desktop/duckdb
git checkout main
CMAKE_BUILD_PARALLEL_LEVEL=10 make reldebug
```

## 2. Build httpfs extension

```bash
cd ~/Desktop/duckdb-httpfs
git checkout main

# Create a branch and apply patches from duckdb
git checkout -b patched-for-repro
for p in ~/Desktop/duckdb/.github/patches/extensions/httpfs/*.patch; do
  git apply "$p" 2>/dev/null || true
done

# Symlink duckdb submodule to local repo
rm -rf duckdb
ln -s ~/Desktop/duckdb duckdb

# Build
GEN=ninja BUILD_FLAGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo" CMAKE_BUILD_PARALLEL_LEVEL=10 make
```

The built binary with httpfs: `~/Desktop/duckdb-httpfs/build/reldebug/duckdb`

## 3. Generate test CSV (131 MB, 5M rows)

```bash
mkdir -p /tmp/pyhttp
echo "COPY (SELECT i AS a, i*2 AS b, ('row_'||i::VARCHAR) AS c FROM range(5000000) t(i)) TO '/tmp/pyhttp/big.csv' (HEADER);" \
  | ~/Desktop/duckdb-httpfs/build/reldebug/duckdb
```

## 4. Start HTTP server (no Range support)

Python's `http.server` returns 200 (not 206) for Range requests, forcing httpfs
into the full-file-download fallback path:

```bash
cd /tmp/pyhttp && python3 -m http.server 8008 &
```

Verify:
```bash
curl -s -o /dev/null -w "%{http_code}\n" -H "Range: bytes=0-99" http://localhost:8008/big.csv
# Should print: 200
```

## 5. Repro SQL (`/tmp/pyhttp/repro.sql`)

```sql
LOAD httpfs;
SET threads=2;
SET async_threads=2;
SET enable_external_file_cache=true;
CREATE VIEW v AS FROM 'http://localhost:8008/big.csv';
SELECT sum(a) FILTER (WHERE b % 3 = 0) FROM v;
```

Key settings:
- `threads=2` — small scan worker pool
- `async_threads=2` — ASYNC pool threads that steal FetchBlockTasks from the scan thread
- `enable_external_file_cache=true` — triggers the nested TaskExecutor path

## 6. Run the deadlock loop

```bash
DUCKDB=~/Desktop/duckdb-httpfs/build/reldebug/duckdb
for i in $(seq 1 20); do
  $DUCKDB < /tmp/pyhttp/repro.sql >/dev/null 2>&1 &
  P=$!
  for t in $(seq 1 12); do kill -0 $P 2>/dev/null || break; sleep 1; done
  if kill -0 $P 2>/dev/null; then
    echo "DEADLOCK on iter $i (pid $P), CPU=$(ps -o %cpu= -p $P | tr -d ' ')"
    # Grab stack trace on macOS:
    sample $P 1 2>/dev/null || true
    kill $P 2>/dev/null
    break
  else
    wait $P
    echo "iter $i: ok"
  fi
done
```

## Results

| `async_threads` | Outcome | CPU when stuck | Explanation |
|---|---|---|---|
| 0 | No deadlock | — | Thread 1 does all FetchBlockTasks itself via `GetTaskFromProducer`, no contention |
| 1 | Deadlocks (iter 1) | ~100% | Thread 1 spins in `WorkOnTasks` yield loop; ASYNC thread holds `CachedFile::lock` |
| 2 | Deadlocks (iter 1) | 0% | All threads parked on mutexes — classic deadlock (matches CI report) |
| default (high) | No deadlock | — | Enough ASYNC workers to complete tasks without starving |

## Root Cause

1. `ClaimNextJob` holds `gstate.lock` (multi-file scan global mutex)
2. CSV `TryInitializeScan` (called under that lock) constructs `StringValueScanner`
3. Scanner constructor reads the first buffer → external file cache → `TaskExecutor::WorkOnTasks()`
4. `WorkOnTasks()` is a synchronous barrier: won't return until all FetchBlockTasks complete
5. ASYNC pool threads steal FetchBlockTasks → call httpfs `FullDownload` → contend on `CachedFile::lock`
6. Thread 1 can't get its tasks back (stolen from producer queue) → waits for `completed_tasks`
7. ASYNC threads are blocked (on `CachedFile::lock` or on the HTTP server) and can't complete
8. Nobody can make progress → deadlock

## Fix (branch `hjiang/fix-csv-external-file-cache-deadlock`)

Move scanner construction out of the lock:
- `TryInitializeScan` (lock-held): only claims metadata into `CSVScanClaim`
- `PrepareScan` (lock-free): constructs `StringValueScanner` (triggers I/O)

This ensures `gstate.lock` is never held across any I/O or nested executor invocation.
