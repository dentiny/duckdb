#include "duckdb/storage/caching_file_system.hpp"

#include <algorithm>
#include <numeric>

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/cache_validation_mode.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/caching_file_system_io_task.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include "duckdb/storage/external_file_cache_util.hpp"
#include "duckdb/storage/read_policy.hpp"
#include "duckdb/storage/read_policy_registry.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

namespace {

// Return whether validation should occur for a specific file
bool ShouldValidate(const OpenFileInfo &info, optional_ptr<ClientContext> client_context, DatabaseInstance &db,
                    const string &filepath) {
	const CacheValidationMode mode = ExternalFileCacheUtil::GetCacheValidationMode(info, client_context, db);
	switch (mode) {
	case CacheValidationMode::VALIDATE_ALL:
		return true;
	case CacheValidationMode::VALIDATE_REMOTE:
		return FileSystem::IsRemoteFile(filepath);
	case CacheValidationMode::NO_VALIDATION:
		return false;
	default:
		return true;
	}
}

} // namespace

CachingFileSystem::CachingFileSystem(FileSystem &file_system_p, DatabaseInstance &db_p)
    : file_system(file_system_p), external_file_cache(ExternalFileCache::Get(db_p)), db(db_p) {
}

CachingFileSystem::~CachingFileSystem() {
}

CachingFileSystem CachingFileSystem::Get(ClientContext &context) {
	return CachingFileSystem(FileSystem::GetFileSystem(context), *context.db);
}

unique_ptr<CachingFileHandle> CachingFileSystem::OpenFile(const OpenFileInfo &path, FileOpenFlags flags) {
	return make_uniq<CachingFileHandle>(QueryContext(), *this, path, flags,
	                                    external_file_cache.GetOrCreateCachedFile(path.path));
}

unique_ptr<CachingFileHandle> CachingFileSystem::OpenFile(QueryContext context, const OpenFileInfo &path,
                                                          FileOpenFlags flags) {
	return make_uniq<CachingFileHandle>(context, *this, path, flags,
	                                    external_file_cache.GetOrCreateCachedFile(path.path));
}

CachingFileHandle::CachingFileHandle(QueryContext context, CachingFileSystem &caching_file_system_p,
                                     const OpenFileInfo &path_p, FileOpenFlags flags_p, CachedFile &cached_file_p)
    : context(context), caching_file_system(caching_file_system_p),
      external_file_cache(caching_file_system.external_file_cache), path(path_p), flags(flags_p),
      validate(
          ExternalFileCacheUtil::GetCacheValidationMode(path_p, context.GetClientContext(), caching_file_system_p.db)),
      cached_file(cached_file_p), position(0) {
	// Create the appropriate read policy based on the current setting
	auto &config = DBConfig::GetConfig(caching_file_system_p.db);
	auto &registry = ReadPolicyRegistry::Get(caching_file_system_p.db);
	const auto &policy_name = config.options.external_file_cache_read_policy_name;
	read_policy = registry.CreatePolicy(policy_name);

	if (!external_file_cache.IsEnabled() || Validate()) {
		// If caching is disabled, or if we must validate cache entries, we always have to open the file
		GetFileHandle();
		return;
	}
	// If we don't have any cached file ranges, we must also open the file.
	auto guard = cached_file.lock.GetSharedLock();
	if (cached_file.Ranges(guard).empty()) {
		guard.reset();
		GetFileHandle();
	}
}

CachingFileHandle::~CachingFileHandle() {
}

FileHandle &CachingFileHandle::GetFileHandle() {
	if (!file_handle) {
		file_handle = caching_file_system.file_system.OpenFile(path, flags);
		last_modified = caching_file_system.file_system.GetLastModifiedTime(*file_handle);
		version_tag = caching_file_system.file_system.GetVersionTag(*file_handle);

		auto guard = cached_file.lock.GetExclusiveLock();
		if (!cached_file.IsValid(guard, Validate(), version_tag, last_modified)) {
			cached_file.Ranges(guard).clear(); // Invalidate entire cache
		}
		cached_file.FileSize(guard) = file_handle->GetFileSize();
		cached_file.LastModified(guard) = last_modified;
		cached_file.VersionTag(guard) = version_tag;
		cached_file.CanSeek(guard) = file_handle->CanSeek();
		cached_file.OnDiskFile(guard) = file_handle->OnDiskFile();
	}
	return *file_handle;
}

BufferHandle CachingFileHandle::Read(data_ptr_t &buffer, const idx_t nr_bytes, const idx_t location) {
	BufferHandle result;
	if (!external_file_cache.IsEnabled()) {
		result = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
		buffer = result.Ptr();
		GetFileHandle().Read(context, buffer, nr_bytes, location);
		return result;
	}

	// Get read policy ranges
	const idx_t file_size = GetFileSize();
	const ReadPolicyRanges policy_ranges = read_policy->CalculateRangesToRead(nr_bytes, location, file_size);
	const idx_t actual_read_location = policy_ranges.total_location;
	const idx_t actual_read_bytes = policy_ranges.total_bytes;

	// Get or create cache blocks for each range
	// Read policy guarantees: ranges are sorted by location, non-overlapping, and cover the entire requested range
	vector<shared_ptr<CachedFileRange>> cache_blocks;
	vector<shared_ptr<CachedFileRange>> pending_blocks;
	vector<BufferHandle> pinned_buffer_handles;
	cache_blocks.reserve(policy_ranges.ranges.size());
	pinned_buffer_handles.reserve(policy_ranges.ranges.size());
	{
		auto guard = cached_file.lock.GetExclusiveLock();
		for (auto &policy_range : policy_ranges.ranges) {
			auto block = GetOrCreatePendingRangeWithLock(guard, policy_range);
			cache_blocks.emplace_back(block);
			if (block->IsComplete()) {
				// Pin immediately when found in cache so it won't get evicted.
				pinned_buffer_handles.emplace_back(external_file_cache.GetBufferManager().Pin(block->block_handle));
			} else {
				pending_blocks.emplace_back(block);
				// Reserve space for pinned block handle, which will be addressed after IO completion.
				pinned_buffer_handles.emplace_back();
			}
		}
	}

	// Perform ALL IO for pending blocks in parallel
	if (!pending_blocks.empty()) {
		auto pending_pins = PerformParallelBlockIO(pending_blocks);
		idx_t pending_idx = 0;
		for (idx_t idx = 0; idx < cache_blocks.size(); ++idx) {
			// Fill in the uncompleted buffer handle.
			if (!pinned_buffer_handles[idx].IsValid()) {
				pinned_buffer_handles[idx] = std::move(pending_pins[pending_idx++]);
			}
		}
		D_ASSERT(pending_idx == pending_blocks.size());
	}

	// Compose all blocks into a single buffer
	// TODO(hjiang): Optimize for cases with one single block.
	result = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, actual_read_bytes);
	buffer = result.Ptr();

	// Copy data from cache blocks into the result buffer (NO MORE IO)
	CopyCacheBlocksToResultBuffer(buffer, std::move(cache_blocks), std::move(pinned_buffer_handles), actual_read_location, actual_read_bytes);

	// Set buffer pointer to requested location
	const idx_t buffer_offset = location - actual_read_location;
	buffer = buffer + buffer_offset;

	return result;
}

BufferHandle CachingFileHandle::Read(data_ptr_t &buffer, idx_t &nr_bytes) {
	auto buffer_handle = Read(buffer, nr_bytes, position);
	position += nr_bytes;
	Seek(position);
	return buffer_handle;
}

string CachingFileHandle::GetPath() const {
	return cached_file.path;
}

idx_t CachingFileHandle::GetFileSize() {
	if (file_handle || Validate()) {
		return GetFileHandle().GetFileSize();
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.FileSize(guard);
}

timestamp_t CachingFileHandle::GetLastModifiedTime() {
	if (file_handle || Validate()) {
		GetFileHandle();
		return last_modified;
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.LastModified(guard);
}

string CachingFileHandle::GetVersionTag() {
	if (file_handle || Validate()) {
		GetFileHandle();
		return version_tag;
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.VersionTag(guard);
}

bool CachingFileHandle::Validate() const {
	return ShouldValidate(path, context.GetClientContext(), caching_file_system.db, cached_file.path);
}

bool CachingFileHandle::CanSeek() {
	if (file_handle || Validate()) {
		return GetFileHandle().CanSeek();
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.CanSeek(guard);
}

bool CachingFileHandle::IsRemoteFile() const {
	return FileSystem::IsRemoteFile(cached_file.path);
}

bool CachingFileHandle::OnDiskFile() {
	if (file_handle || Validate()) {
		return GetFileHandle().OnDiskFile();
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.OnDiskFile(guard);
}

const string &CachingFileHandle::GetVersionTag(const unique_ptr<StorageLockKey> &guard) {
	if (file_handle || Validate()) {
		GetFileHandle();
		return version_tag;
	}
	return cached_file.VersionTag(guard);
}

idx_t CachingFileHandle::SeekPosition() {
	return position;
}

void CachingFileHandle::Seek(idx_t location) {
	position = location;
	if (file_handle != nullptr) {
		file_handle->Seek(location);
	}
}

shared_ptr<CachingFileHandle::CachedFileRange>
CachingFileHandle::GetOrCreatePendingRangeWithLock(unique_ptr<StorageLockKey> &guard, const ReadPolicyResult &range) {
	auto &ranges = cached_file.Ranges(guard);

	// Check if this exact range already exists, which could be pending or completed.
	auto it = ranges.find(range.read_location);
	if (it != ranges.end() && it->second->nr_bytes == range.read_bytes) {
		return it->second;
	}

	// Check if any existing range fully covers this range
	const idx_t range_end = range.read_location + range.read_bytes;
	it = ranges.lower_bound(range.read_location);
	if (it != ranges.begin()) {
		--it;
	}
	while (it != ranges.end() && it->first < range_end) {
		auto overlap = it->second->GetOverlap(range.read_bytes, range.read_location);
		if (overlap == CachedFileRangeOverlap::FULL) {
			return it->second; // Return existing block that fully covers this range
		}
		// For partial overlap or no overlap, create the exact block requested by the read policy
		// Don't return partially overlapping blocks as they create gaps
		++it;
	}

	// Check if there's already a pending range at this location (created by another thread)
	it = ranges.find(range.read_location);
	if (it != ranges.end() && it->second->nr_bytes == range.read_bytes && !it->second->IsComplete()) {
		return it->second; // Return existing pending range
	}

	const string &version_tag = GetVersionTag(guard);
	// No suitable cached or pending range found, create a new pending range
	// block_handle = nullptr indicates this range needs IO
	auto pending_range = make_shared_ptr<CachedFileRange>(nullptr, range.read_bytes, range.read_location, version_tag);
	ranges[range.read_location] = pending_range;

	return pending_range;
}

vector<BufferHandle> CachingFileHandle::PerformParallelBlockIO(const vector<shared_ptr<CachedFileRange>> &pending_blocks) {
	ParallelIOState state(pending_blocks.size());	
	TaskExecutor executor(TaskScheduler::GetScheduler(caching_file_system.db));
	for (idx_t idx = 0; idx < pending_blocks.size(); ++idx) {
		auto task = make_uniq<BlockIOTask>(executor, *this, pending_blocks[idx], state, idx);
		executor.ScheduleTask(std::move(task));
	}
	executor.WorkOnTasks();	
	return std::move(state.buffer_handles);
}

void CachingFileHandle::CopyCacheBlocksToResultBuffer(data_ptr_t buffer, vector<shared_ptr<CachedFileRange>> cache_blocks,
                                                vector<BufferHandle> pinned_buffer_handles, idx_t actual_read_location, idx_t actual_read_bytes) {
	D_ASSERT(cache_blocks.size() == pinned_buffer_handles.size());
	
	// Verify that cache_blocks are sorted by location
	for (idx_t i = 1; i < cache_blocks.size(); ++i) {
		D_ASSERT(cache_blocks[i - 1]->location < cache_blocks[i]->location);
	}

	const idx_t end_pos = actual_read_location + actual_read_bytes;
	
	// Process blocks in order and copy data from each block
	for (idx_t idx = 0; idx < cache_blocks.size(); ++idx) {
		auto &block = cache_blocks[idx];
		auto &cur_buffer_handle = pinned_buffer_handles[idx];

		// Calculate the overlap between this block and the range we need to copy
		const idx_t block_start = block->location;
		const idx_t block_end = block->location + block->nr_bytes;
		const idx_t copy_start = MaxValue(block_start, actual_read_location);
		const idx_t copy_end = MinValue(block_end, end_pos);
		
		// Copy from this block if it overlaps
		if (copy_start < copy_end) {
			const idx_t bytes_to_copy = copy_end - copy_start;
			const idx_t offset_in_block = copy_start - block_start;
			const idx_t offset_in_buffer = copy_start - actual_read_location;

			D_ASSERT(cur_buffer_handle.IsValid());
			memcpy(buffer + offset_in_buffer, cur_buffer_handle.Ptr() + block->block_offset + offset_in_block, bytes_to_copy);
		}
	}
}

} // namespace duckdb
