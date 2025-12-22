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

BlockIOTask::BlockIOTask(TaskExecutor &executor, CachingFileHandle &handle_p,
                         shared_ptr<ExternalFileCache::CachedFileRange> block_p, ParallelIOState &state_p, idx_t index_p)
    : BaseExecutorTask(executor), handle(handle_p), block(std::move(block_p)), state(state_p), index(index_p) {
}

void BlockIOTask::ExecuteTask() {
	unique_lock<mutex> lock(block->completion_mutex);
	
	// Check if already complete.
	if (block->IsComplete()) {
		// Pin immediately when we find it's already complete
		lock.unlock();
		{
			const lock_guard<mutex> state_guard(state.lock);
			state.buffer_handles[index] = handle.external_file_cache.GetBufferManager().Pin(block->block_handle);
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
			state.buffer_handles[index] = handle.external_file_cache.GetBufferManager().Pin(block->block_handle);
		}
		return;
	}

	// No need to release block here, all accessors are waiting for the IO operation completion anyway.
	block->io_in_progress = true;
	lock.unlock();

	auto io_buffer = handle.external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE,
	                                                                         block->nr_bytes);
	// TODO(hjiang): I think we could do better on error propagation, for example, capture IO error and record in the request struct, so other requesters could retry.
	// Currently we simply throw exception on the IO thread.
	handle.GetFileHandle().Read(handle.context, io_buffer.Ptr(), block->nr_bytes, block->location);
	block->MarkIoComplete(io_buffer.GetBlockHandle());

	// Pin the block and store in shared state.
	{
		const lock_guard<mutex> state_guard(state.lock);
		state.buffer_handles[index] = handle.external_file_cache.GetBufferManager().Pin(block->block_handle);
	}
}

} // namespace duckdb

