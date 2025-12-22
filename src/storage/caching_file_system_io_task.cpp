//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/caching_file_system_io_task.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/storage/caching_file_system_io_task.hpp"

#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

BlockIOTask::BlockIOTask(TaskExecutor &executor, CachingFileHandle &handle,
                         shared_ptr<ExternalFileCache::CachedFileRange> block, ParallelIOState &state, idx_t index)
    : BaseExecutorTask(executor), handle(handle), block(std::move(block)), state(state), index(index) {
}

void BlockIOTask::ExecuteTask() {
	auto io_buffer = handle.external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE,
	                                                                         block->nr_bytes);
	handle.GetFileHandle().Read(handle.context, io_buffer.Ptr(), block->nr_bytes, block->location);
	block->io_in_progress.store(false);
	D_ASSERT(!block->IsComplete());
	block->Complete(io_buffer.GetBlockHandle(), /*offset_in_block=*/0);

	// Pin the block and store in shared state.
	{
		const lock_guard<mutex> guard(state.lock);
		state.pins[index] = handle.external_file_cache.GetBufferManager().Pin(block->block_handle);
	}
}

} // namespace duckdb

