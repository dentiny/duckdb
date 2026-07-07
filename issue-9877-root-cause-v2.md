# duckdb-internal#9877: Root Cause Analysis

## Summary

A deadlock occurs when the external file cache's `WorkOnTasks()` is called inline
while ASYNC pool threads steal the scheduled `FetchBlockTask`s. Multiple ASYNC
threads then concurrently call `HTTPFileHandle::FullDownload` on the **same**
`HTTPFileHandle` instance, causing a permanent block on `CachedFile::lock`.

## Reproduction

```sql
LOAD httpfs;
SET threads=2;
SET async_threads=2;
SET enable_external_file_cache=true;
CREATE VIEW v AS FROM 'http://localhost:8008/big.csv';  -- python3 -m http.server (no Range support)
SELECT sum(a) FILTER (WHERE b % 3 = 0) FROM v;
```

Deadlocks on first iteration, 0% CPU.

## Thread Dump (from `sample` on macOS)

### Thread 1 (main) — spinning in WorkOnTasks

```
CSVSchemaDiscovery::SchemaDiscovery
  → CSVBufferManager::Initialize → CSVBuffer ctor → CSVFileHandle::Read
    → CachingFileSystemWrapper::Read → CachingFileHandle::Read (caching_file_system.cpp:306)
      → TaskExecutor::WorkOnTasks (task_executor.cpp:57)
        → yield / GetTaskFromProducer  [ALL TASKS STOLEN — spinning]
```

Thread 1 scheduled FetchBlockTasks to the ASYNC queue and called `WorkOnTasks()`.
Both ASYNC pool threads stole the tasks. `GetTaskFromProducer` returns false
(items consumed by other consumers). Thread 1 spins waiting for
`completed_tasks == total_tasks` — which never happens.

### Thread 2 (ASYNC pool) — blocked on CachedFile::lock

```
TaskScheduler::ExecuteForever(ASYNC)
  → FetchBlockTask::ExecuteTask (caching_file_system.cpp:82)
    → ReadAndRecord (caching_file_system.cpp:480)
      → HTTPFileSystem::Read (httpfs.cpp:692)
        → HTTPFileHandle::FullDownload (httpfs.cpp:794)
          → CachedFile::GetHandle → CachedFileHandle ctor (http_state.cpp:9)
            → std::mutex::lock → __psynch_mutexwait  [BLOCKED]
```

### Thread 3 (ASYNC pool) — blocked on CachedFile::lock

```
(identical stack to Thread 2)
→ CachedFileHandle::CachedFileHandle → std::mutex::lock → __psynch_mutexwait  [BLOCKED]
```

### Thread 4 — idle (semaphore_wait, regular pool worker)

## The Deadlock Mechanism

### Step 1: External file cache schedules parallel block fetches

`CachingFileHandle::Read` (caching_file_system.cpp:296-306):
```cpp
TaskExecutor executor(scheduler, TaskSchedulerType::ASYNC);
for (idx_t idx = 0; idx < num_blocks; idx++) {
    executor.ScheduleTask(make_uniq<FetchBlockTask>(...));
}
executor.WorkOnTasks();
```

Multiple FetchBlockTasks are scheduled to the ASYNC queue for the same file.

### Step 2: ASYNC pool threads steal all tasks

With `async_threads=2`, both pool threads wake up (signaled by ScheduleTask) and
dequeue the FetchBlockTasks before Thread 1's `GetTaskFromProducer` can retrieve
them. Thread 1 enters a spin-yield loop.

### Step 3: Both ASYNC threads hit FullDownload concurrently

Each FetchBlockTask does:
1. `FetchBlockTask::ExecuteTask` → sets block state to LOADING, unlocks block mutex
2. Calls `caching_file_handle.ReadAndRecord(...)` → underlying `HTTPFileSystem::Read`
3. `ReadInternal` tries a range request → server returns 200 (no range support) → returns false
4. Falls back to `FullDownload` on the **shared** `HTTPFileHandle`

### Step 4: Data race + mutex deadlock on CachedFile::lock

`HTTPFileHandle::FullDownload` (httpfs.cpp:791-812):
```cpp
void HTTPFileHandle::FullDownload(HTTPFileSystem &hfs, bool &should_write_cache) {
    const auto &cache_entry = http_params.state->GetCachedFile(path);
    cached_file_handle = cache_entry->GetHandle();  // CachedFileHandle ctor → takes CachedFile::lock
    if (!cached_file_handle->Initialized()) {
        hfs.GetRequest(*this, path, {});             // HTTP download
        cached_file_handle->SetInitialized(length);  // releases lock
    }
}
```

`CachedFileHandle` constructor (http_state.cpp:6-10):
```cpp
CachedFileHandle::CachedFileHandle(shared_ptr<CachedFile> &file_p) {
    if (!file_p->initialized) {
        lock = make_uniq<lock_guard<mutex>>(file_p->lock);  // BLOCKS if already held
    }
    file = file_p;
}
```

**Critical issues:**
1. `cached_file_handle` is a member of `HTTPFileHandle` — multiple ASYNC threads
   write to it concurrently (DATA RACE / UB)
2. Both threads call `GetHandle()` which constructs `CachedFileHandle` and tries
   to acquire `CachedFile::lock`
3. Combined with the single-threaded Python HTTP server (which serializes requests),
   the first thread's range request OR full download blocks the server, preventing
   the second thread's request from completing

### Step 5: Permanent deadlock

- **Thread 1**: in `WorkOnTasks()`, waiting for `completed_tasks == total_tasks`.
  Cannot get any tasks (all stolen). Cannot make progress.
- **Thread 2 + 3**: both parked on `CachedFile::lock` (mutex futex wait).
  The lock is either held by a download that can't complete (server busy) or
  corrupted by the data race on `cached_file_handle`.
- **Nobody can break the cycle** → all threads permanently stuck.

## Why `async_threads` matters

| Value | Behavior |
|-------|----------|
| 0 | Thread 1 executes all FetchBlockTasks itself (sequential, no contention) → no deadlock |
| 1 | One ASYNC thread steals a task, Thread 1 gets the other → contention on CachedFile::lock, but Thread 1 spins (CPU 100%) |
| 2 | Both ASYNC threads steal all tasks → Thread 1 can't self-serve, both ASYNC threads block → deadlock (CPU 0%) |
| high | Pool has slack, tasks complete before contention → no deadlock |

## Why the fix works

The fix on branch `hjiang/fix-csv-external-file-cache-deadlock`:
- `TryInitializeScan` (lock-held): only claims scan metadata into `CSVScanClaim`
- `PrepareScan` (lock-free): constructs `StringValueScanner` → triggers buffer read → external file cache

This prevents `gstate.lock` from being held across the nested `WorkOnTasks()` call in
the **scan phase**. However, the repro also triggers during **schema discovery** (bind
phase), where no `gstate.lock` is involved — the deadlock is in the external file cache
+ httpfs interaction itself.

## Deeper root causes (for complete fix)

1. **External file cache calls `WorkOnTasks()` synchronously** — the caller thread
   becomes dependent on ASYNC pool threads to complete work. If those threads block,
   the caller is stuck.

2. **`HTTPFileHandle::cached_file_handle` has no synchronization** — multiple threads
   calling `FullDownload` on the same handle is a data race.

3. **`CachedFile::lock` is held for the duration of a full HTTP download** — any
   contention on this lock blocks for the entire download time (seconds for large files).

4. **Single-threaded HTTP server serializes requests** — combined with (3), creates
   a scenario where the lock holder can't complete its download because the server
   is busy serving another request from the same process.

## Code References

| What | Where |
|------|-------|
| External file cache creates TaskExecutor(ASYNC) | `src/storage/external_file_cache/caching_file_system.cpp:299` |
| Calls WorkOnTasks() inline | `src/storage/external_file_cache/caching_file_system.cpp:306` |
| FetchBlockTask reads via underlying FS | `src/storage/external_file_cache/caching_file_system.cpp:82` |
| httpfs Read fallback to FullDownload | `~/Desktop/duckdb-httpfs/src/httpfs.cpp:692` |
| FullDownload takes CachedFile::lock | `~/Desktop/duckdb-httpfs/src/http_state.cpp:9` |
| CachedFile::lock definition | `~/Desktop/duckdb-httpfs/src/include/http_state.hpp:32` |
| cached_file_handle member (shared unsafely) | `~/Desktop/duckdb-httpfs/src/include/httpfs.hpp` (HTTPFileHandle) |
