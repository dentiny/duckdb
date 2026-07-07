# duckdb-internal#9877: CSV/httpfs deadlock root cause

## Summary

`CSVFileScan::TryInitializeScan` runs under the multi-file scan global lock. Before the fix, CSV constructed
`StringValueScanner` from that method. The `StringValueScanner` constructor enters `BaseScanner`, which immediately
reads the first CSV buffer. When the file is wrapped by the external file cache and backed by httpfs, that read can
schedule cache fetch tasks and synchronously re-enter task execution while the multi-file lock is still held.

That creates the deadlock shape from the issue: one worker holds the multi-file lock and waits inside nested cache/httpfs
work, while another scan task needs the same multi-file lock to start the work that would let the nested executor make
progress.

## Code References

| Area | Code reference | Why it matters |
| --- | --- | --- |
| Multi-file lock boundary | `src/include/duckdb/common/multi_file/multi_file_function.hpp:453` | `ClaimNextJob` takes `gstate.lock`. |
| Lock-held scan initialization | `src/include/duckdb/common/multi_file/multi_file_function.hpp:468` | `TryInitializeScan` is called while that lock is still held. |
| Lock released before prepare | `src/include/duckdb/common/multi_file/multi_file_function.hpp:478` | The lock is dropped only after `TryInitializeScan` succeeds. |
| Lock-free prepare hook | `src/include/duckdb/common/multi_file/multi_file_function.hpp:479` | `PrepareScan` runs after unlocking. |
| Reader contract | `src/include/duckdb/common/multi_file/base_file_reader.hpp:94` | The interface documents that `TryInitializeScan` runs under the global lock. |
| Reader contract | `src/include/duckdb/common/multi_file/base_file_reader.hpp:97` | The interface documents that `PrepareScan` runs without any lock held. |
| CSV old hazard / new split point | `src/execution/operator/csv_scanner/table_function/csv_multi_file_info.cpp:358` | CSV `TryInitializeScan` now only finishes the previous scanner and claims metadata. |
| CSV lock-free construction | `src/execution/operator/csv_scanner/table_function/csv_multi_file_info.cpp:371` | CSV `PrepareScan` now constructs `StringValueScanner`. |
| First-buffer read | `src/execution/operator/csv_scanner/scanner/base_scanner.cpp:21` | `BaseScanner` immediately calls `buffer_manager->GetBuffer(...)`. |
| External cache nested executor | `src/storage/external_file_cache/caching_file_system.cpp:301` | Cache reads schedule `FetchBlockTask`s. |
| External cache nested executor | `src/storage/external_file_cache/caching_file_system.cpp:306` | Cache reads call `executor.WorkOnTasks()` inline. |
| httpfs full download | `/Users/hjiang/Desktop/duckdb-httpfs/src/httpfs.cpp:791` | httpfs fallback enters `HTTPFileHandle::FullDownload`. |
| httpfs cache lock | `/Users/hjiang/Desktop/duckdb-httpfs/src/http_state.cpp:6` | `CachedFileHandle` takes the per-file cache lock when the full download is not initialized. |

## Fix

Keep `TryInitializeScan` lock-safe. It should only claim the next CSV scan unit and update shared scan metadata.

The read-triggering work moved to `PrepareScan`, which the multi-file framework already calls after releasing
`gstate.lock`:

| Fix part | Code reference |
| --- | --- |
| Claim metadata into `CSVScanClaim` | `src/include/duckdb/execution/operator/csv_scanner/global_csv_state.hpp:23` |
| Store claim in local state | `src/execution/operator/csv_scanner/table_function/csv_multi_file_info.cpp:283` |
| Claim without constructing scanner | `src/execution/operator/csv_scanner/table_function/global_csv_state.cpp:45` |
| Construct scanner after unlock | `src/execution/operator/csv_scanner/table_function/csv_multi_file_info.cpp:371` |

## Regression Test

Added `test/common/test_csv_external_file_cache.cpp`.

The test uses a local fake filesystem instead of a live HTTP server. It maps two fake remote-looking paths to local CSV
files, enables the external file cache, and blocks the first underlying read until a second file read begins. Before the
fix, the second read cannot begin because the first scan is still holding the multi-file lock; the test times out and
fails. After the fix, scanner construction happens outside the lock, the second scan starts, and the test passes.

Relevant test reference: `test/common/test_csv_external_file_cache.cpp:126`.

## Verification

```sh
CMAKE_BUILD_PARALLEL_LEVEL=10 make reldebug
build/reldebug/test/unittest "CSV external cache reads are not launched while holding the multi-file scan lock"
build/reldebug/test/unittest test/sql/copy/csv/parallel/test_multiple_files.test
```
