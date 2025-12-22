//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/caching_file_system_io_task.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/external_file_cache.hpp"

namespace duckdb {

struct CachingFileHandle;

//! Shared state for parallel IO operations for caching file handle
struct ParallelIOState {
	// Invariant: if buffer handle is valid, it's pinned in buffer manager.
	vector<BufferHandle> buffer_handles;
	// Used to protect `pins`.
	mutex lock;

	explicit ParallelIOState(idx_t count) : buffer_handles(count) {}
};

//! Task for performing a single block IO operation in parallel
class BlockIOTask : public BaseExecutorTask {
public:
	BlockIOTask(TaskExecutor &executor, CachingFileHandle &handle_p, shared_ptr<ExternalFileCache::CachedFileRange> block_p,
	            ParallelIOState &state_p, idx_t index_p);

	void ExecuteTask() override;

	string TaskType() const override {
		return "BlockIOTask";
	}

private:
	CachingFileHandle &handle;
	shared_ptr<ExternalFileCache::CachedFileRange> block;
	ParallelIOState &state;
	// Used to indicate index-th state's buffer handle.
	idx_t index;
};

} // namespace duckdb
