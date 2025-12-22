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
	unique_lock<mutex> lock(block->completion_mutex);
	
	// Check if already complete.
	if (block->IsComplete()) {
		// Pin immediately when we find it's already complete
		lock.unlock();
		{
			const lock_guard<mutex> state_guard(state.lock);
			state.pins[index] = handle.external_file_cache.GetBufferManager().Pin(block->block_handle);
		}
		return;
	}
	
	// If another thread is already doing IO, wait for its completion
	if (block->io_in_progress) {
		block->WaitForCompletion(lock);
		// Now it should be complete, pin it
		D_ASSERT(block->IsComplete());
		lock.unlock();
		{
			const lock_guard<mutex> state_guard(state.lock);
			state.pins[index] = handle.external_file_cache.GetBufferManager().Pin(block->block_handle);
		}
		return;
	}

	// We're the one doing the IO, mark read in progress
	block->io_in_progress = true;
	lock.unlock();
	
	// Perform the actual IO
	auto io_buffer = handle.external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE,
	                                                                         block->nr_bytes);
	handle.GetFileHandle().Read(handle.context, io_buffer.Ptr(), block->nr_bytes, block->location);
	
	// Complete the block (this will set io_in_progress = false inside Complete())
	D_ASSERT(!block->IsComplete());
	block->Complete(io_buffer.GetBlockHandle(), /*offset_in_block=*/0);

	// Pin the block and store in shared state.
	{
		const lock_guard<mutex> state_guard(state.lock);
		state.pins[index] = handle.external_file_cache.GetBufferManager().Pin(block->block_handle);
	}
}

} // namespace duckdb

