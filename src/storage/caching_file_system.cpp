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
#include "duckdb/storage/external_file_cache.hpp"
#include "duckdb/storage/external_file_cache_util.hpp"
#include "duckdb/storage/read_policy.hpp"
#include "duckdb/storage/read_policy_registry.hpp"

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

	// Get or create cache blocks for each range and identify pending blocks that need IO
	// Pin complete blocks immediately to prevent eviction
	vector<shared_ptr<CachedFileRange>> cache_blocks;
	vector<shared_ptr<CachedFileRange>> pending_blocks;
	vector<BufferHandle> pins;
	cache_blocks.reserve(policy_ranges.ranges.size());
	pins.reserve(policy_ranges.ranges.size());
	{
		auto guard = cached_file.lock.GetExclusiveLock();
		for (auto &policy_range : policy_ranges.ranges) {
			auto block = CheckOrCreatePendingRangeWithLock(guard, policy_range);
			cache_blocks.emplace_back(block);
			if (block->IsComplete()) {
				// Pin immediately when found in cache.
				pins.emplace_back(external_file_cache.GetBufferManager().Pin(block->block_handle));
				continue;
			}

			pending_blocks.emplace_back(block);
			// Reserve space for pinned block handle, which will be addressed after IO completion.
			pins.emplace_back();
		}
	}

	// Perform IO for pending blocks and pin them as they complete
	// TODO(hjiang): Current IO operations are still sequential.
	if (!pending_blocks.empty()) {
		auto pending_pins = PerformParallelBlockIO(pending_blocks);
		idx_t pending_idx = 0;
		for (idx_t idx = 0; idx < cache_blocks.size(); ++idx) {
			// Fill in the uncompleted buffer handle.
			if (!pins[idx].IsValid()) {
				pins[idx] = std::move(pending_pins[pending_idx++]);
			}
		}
		D_ASSERT(pending_idx == pending_blocks.size());
	}

	// Compose all blocks into a single buffer
	result = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, actual_read_bytes);
	buffer = result.Ptr();

	// Copy data from cache blocks into the result buffer
	CopyCacheBlocksToResultBuffer(buffer, cache_blocks, pins, actual_read_location, actual_read_bytes);

	// Set buffer pointer to requested location
	const idx_t buffer_offset = location - actual_read_location;
	buffer = buffer + buffer_offset;

	return result;
}

BufferHandle CachingFileHandle::Read(data_ptr_t &buffer, idx_t &nr_bytes) {
	BufferHandle result;

	// If we can't seek, we can't use the cache for these calls,
	// because we won't be able to seek over any parts we skipped by reading from the cache
	if (!external_file_cache.IsEnabled() || !CanSeek()) {
		result = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
		buffer = result.Ptr();
		nr_bytes = NumericCast<idx_t>(GetFileHandle().Read(context, buffer, nr_bytes));
		position += NumericCast<idx_t>(nr_bytes);
		return result;
	}

	// Try to read from the cache first
	vector<shared_ptr<CachedFileRange>> overlapping_ranges;
	{
		optional_idx start_location_of_next_range;
		result = TryReadFromCache(buffer, nr_bytes, position, overlapping_ranges, start_location_of_next_range);
		// start_location_of_next_range is in this case discarded
	}

	if (result.IsValid()) {
		position += nr_bytes;
		return result; // Success
	}

	// Finally, if we weren't able to find the file range in the cache, we have to create a new file range
	result = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
	buffer = result.Ptr();

	GetFileHandle().Seek(position);
	nr_bytes = NumericCast<idx_t>(GetFileHandle().Read(context, buffer, nr_bytes));
	auto new_file_range = make_shared_ptr<CachedFileRange>(result.GetBlockHandle(), nr_bytes, position, version_tag);

	result = TryInsertFileRange(result, buffer, nr_bytes, position, new_file_range);
	position += NumericCast<idx_t>(nr_bytes);

	return result;
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

BufferHandle CachingFileHandle::TryReadFromCache(data_ptr_t &buffer, idx_t nr_bytes, idx_t location,
                                                 vector<shared_ptr<CachedFileRange>> &overlapping_ranges,
                                                 optional_idx &start_location_of_next_range) {
	BufferHandle result;

	// Get read lock for cached ranges
	auto guard = cached_file.lock.GetSharedLock();
	auto &ranges = cached_file.Ranges(guard);

	// First, try to see if we've read from the exact same location before
	auto it = ranges.find(location);
	if (it != ranges.end()) {
		// We have read from the exact same location before
		if (it->second->GetOverlap(nr_bytes, location) == CachedFileRangeOverlap::FULL) {
			// The file range contains the requested file range
			// FIXME: if we ever start persisting this stuff, this read needs to happen outside of the lock
			result = TryReadFromFileRange(guard, *it->second, buffer, nr_bytes, location);
			if (result.IsValid()) {
				return result;
			}
		}
	}

	// Second, loop through file ranges (ordered by location) to see if any contain the requested file range
	const auto this_end = location + nr_bytes;

	// Start at lower_bound (first range with location not less than location of requested range) minus one
	// This works because we don't allow fully overlapping ranges in the files
	it = ranges.lower_bound(location);
	if (it != ranges.begin()) {
		--it;
	}
	while (it != ranges.end()) {
		if (it->second->location >= this_end) {
			// We're past the requested location, we are going to bail out, save start_location_of_next_range
			start_location_of_next_range = it->second->location;
			break;
		}
		// Check if the cached range overlaps the requested one
		switch (it->second->GetOverlap(nr_bytes, location)) {
		case CachedFileRangeOverlap::NONE:
			// No overlap at all
			break;
		case CachedFileRangeOverlap::PARTIAL:
			// Partial overlap, store for potential use later
			overlapping_ranges.push_back(it->second);
			break;
		case CachedFileRangeOverlap::FULL:
			// The file range fully contains the requested file range, if the buffer is still valid we're done
			// FIXME: if we ever start persisting this stuff, this read needs to happen outside of the lock
			result = TryReadFromFileRange(guard, *it->second, buffer, nr_bytes, location);
			if (result.IsValid()) {
				return result;
			}
			break;
		default:
			throw InternalException("Unknown CachedFileRangeOverlap");
		}
		++it;
	}

	return result;
}

BufferHandle CachingFileHandle::TryReadFromFileRange(const unique_ptr<StorageLockKey> &guard,
                                                     CachedFileRange &file_range, data_ptr_t &buffer, idx_t nr_bytes,
                                                     idx_t location) {
	D_ASSERT(file_range.GetOverlap(nr_bytes, location) == CachedFileRangeOverlap::FULL);
	auto result = external_file_cache.GetBufferManager().Pin(file_range.block_handle);
	if (result.IsValid()) {
		buffer = result.Ptr() + (location - file_range.location);
	}
	return result;
}

BufferHandle CachingFileHandle::TryInsertFileRange(BufferHandle &pin, data_ptr_t &buffer, idx_t nr_bytes,
                                                   idx_t location, shared_ptr<CachedFileRange> &new_file_range) {
	// Grab the lock again (write lock this time) to insert the newly created buffer into the ranges
	auto guard = cached_file.lock.GetExclusiveLock();
	auto &ranges = cached_file.Ranges(guard);

	// Start at lower_bound (first range with location not less than location of newly created range)
	const auto this_end = location + nr_bytes;
	auto it = ranges.lower_bound(location);
	if (it != ranges.begin()) {
		--it;
	}
	while (it != ranges.end()) {
		if (it->second->location >= this_end) {
			// We're past the requested location
			break;
		}
		if (it->second->GetOverlap(*new_file_range) == CachedFileRangeOverlap::FULL) {
			// Another thread has read a range that fully contains the requested range in the meantime
			auto other_pin = TryReadFromFileRange(guard, *it->second, buffer, nr_bytes, location);
			if (other_pin.IsValid()) {
				return other_pin;
			}
			it = ranges.erase(it);
			continue;
		}
		// Check if the new range overlaps with a cached one
		switch (new_file_range->GetOverlap(*it->second)) {
		case CachedFileRangeOverlap::NONE:
			break; // No overlap, still useful
		case CachedFileRangeOverlap::PARTIAL:
			break; // The newly created range does not fully contain this range, so it is still useful
		case CachedFileRangeOverlap::FULL:
			// Full overlap, this range will be obsolete when we insert the current one
			// Since we have the write lock here, we can do some cleanup
			it = ranges.erase(it);
			continue;
		default:
			throw InternalException("Unknown CachedFileRangeOverlap");
		}

		++it;
	}
	D_ASSERT(pin.IsValid());

	// Finally, insert newly created buffer into the map
	new_file_range->AddCheckSum();
	ranges[location] = std::move(new_file_range);
	cached_file.Verify(guard);

	return std::move(pin);
}

idx_t CachingFileHandle::ReadAndCopyInterleaved(const vector<shared_ptr<CachedFileRange>> &overlapping_ranges,
                                                const shared_ptr<CachedFileRange> &new_file_range, data_ptr_t buffer,
                                                const idx_t nr_bytes, const idx_t location, const bool actually_read) {
	idx_t non_cached_read_count = 0;

	idx_t current_location = location;
	idx_t remaining_bytes = nr_bytes;
	for (auto &overlapping_range : overlapping_ranges) {
		D_ASSERT(new_file_range->GetOverlap(*overlapping_range) != CachedFileRangeOverlap::NONE);

		if (remaining_bytes == 0) {
			break; // All requested bytes were read
		}

		if (overlapping_range->location > current_location) {
			// We need to read from the file until we're at the location of the current overlapping file range
			const auto buffer_offset = nr_bytes - remaining_bytes;
			const auto bytes_to_read = overlapping_range->location - current_location;
			D_ASSERT(bytes_to_read < remaining_bytes);
			if (actually_read) {
				GetFileHandle().Read(context, buffer + buffer_offset, bytes_to_read, current_location);
			}
			current_location += bytes_to_read;
			remaining_bytes -= bytes_to_read;
			non_cached_read_count++;
		}

		if (overlapping_range->GetOverlap(remaining_bytes, current_location) == CachedFileRangeOverlap::NONE) {
			continue; // Remainder does not overlap with the current overlapping file range
		}

		// Try to pin the current overlapping file range
		auto overlapping_file_range_pin = external_file_cache.GetBufferManager().Pin(overlapping_range->block_handle);
		if (!overlapping_file_range_pin.IsValid()) {
			continue; // No longer valid
		}

		// Finally, we can copy the data over
		D_ASSERT(current_location >= overlapping_range->location);
		const auto buffer_offset = nr_bytes - remaining_bytes;
		const auto overlapping_range_offset = current_location - overlapping_range->location;
		D_ASSERT(overlapping_range->nr_bytes > overlapping_range_offset);
		const auto bytes_to_read = MinValue(overlapping_range->nr_bytes - overlapping_range_offset, remaining_bytes);
		if (actually_read) {
			memcpy(buffer + buffer_offset, overlapping_file_range_pin.Ptr() + overlapping_range_offset, bytes_to_read);
		}
		current_location += bytes_to_read;
		remaining_bytes -= bytes_to_read;
	}

	// Read the remaining bytes (if any)
	if (remaining_bytes != 0) {
		const auto buffer_offset = nr_bytes - remaining_bytes;
		if (actually_read) {
			GetFileHandle().Read(context, buffer + buffer_offset, remaining_bytes, current_location);
		}
		non_cached_read_count++;
	}

	return non_cached_read_count;
}

shared_ptr<CachingFileHandle::CachedFileRange>
CachingFileHandle::CheckOrCreatePendingRangeWithLock(unique_ptr<StorageLockKey> &guard, const ReadPolicyResult &range) {
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
		if (overlap == CachedFileRangeOverlap::PARTIAL) {
			// Partial overlap - we should not create a new overlapping block
			// Instead, return the existing block and let the composition logic handle the gap
			// Note: This means the read policy should ideally give us non-overlapping ranges
			// For now, we'll return the existing block and the caller will need to handle gaps
			return it->second;
		}
		++it;
	}

	// Check if there's already a pending range at this location (created by another thread)
	it = ranges.find(range.read_location);
	if (it != ranges.end() && it->second->nr_bytes == range.read_bytes && !it->second->IsComplete()) {
		return it->second; // Return existing pending range
	}

	// No suitable cached or pending range found, create a new pending range
	// block_handle = nullptr indicates this range needs IO
	auto pending_range = make_shared_ptr<CachedFileRange>(nullptr, range.read_bytes, range.read_location, version_tag);
	ranges[range.read_location] = pending_range;
	
	return pending_range;
}

vector<BufferHandle> CachingFileHandle::PerformParallelBlockIO(vector<shared_ptr<CachedFileRange>> &pending_blocks) {
	vector<BufferHandle> pins;
	if (pending_blocks.empty()) {
		return pins;
	}

	pins.reserve(pending_blocks.size());

	// Perform IO for each pending block
	// If not already requested, issue IO operation and mark as complete
	// If already requested, block and wait for its completion
	for (auto &block : pending_blocks) {
		unique_lock<mutex> lock(block->completion_mutex);
		
		// Check if already complete (another thread finished it)
		if (block->IsComplete()) {
			// Pin immediately when we find it's already complete
			pins.emplace_back(external_file_cache.GetBufferManager().Pin(block->block_handle));
			continue;
		}
		
		// If another thread is already doing IO, block wait its completion.
		if (block->io_in_progress.load()) {
			block->WaitForCompletion(lock);
			// Now it should be complete, pin it
			D_ASSERT(block->IsComplete());
			pins.emplace_back(external_file_cache.GetBufferManager().Pin(block->block_handle));
			continue;
		}

		// We're the one doing the IO, mark read in progress.
		block->io_in_progress.store(true);
		BufferHandle io_buffer = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, block->nr_bytes);

		lock.unlock();
		GetFileHandle().Read(context, io_buffer.Ptr(), block->nr_bytes, block->location);
		lock.lock();
		
		// Mark IO as no longer in progress
		block->io_in_progress.store(false);
		D_ASSERT(!block->IsComplete());
		// Mark as complete and notify waiting threads
		block->Complete(io_buffer.GetBlockHandle(), /*offset_in_block=*/0);
		
		// Pin immediately after completing IO
		pins.emplace_back(external_file_cache.GetBufferManager().Pin(block->block_handle));
	}
	
	return pins;
}

void CachingFileHandle::CopyCacheBlocksToResultBuffer(data_ptr_t buffer, const vector<shared_ptr<CachedFileRange>> &cache_blocks,
                                                const vector<BufferHandle> &pins, idx_t actual_read_location, idx_t actual_read_bytes) {
	D_ASSERT(cache_blocks.size() == pins.size());

	const idx_t end_pos = actual_read_location + actual_read_bytes;
	
	for (idx_t idx = 0; idx < cache_blocks.size(); ++idx) {
		auto &block = cache_blocks[idx];
		auto &pin = pins[idx];

		// Calculate the overlap between this block and the range we need to copy
		const idx_t block_start = block->location;
		const idx_t block_end = block->location + block->nr_bytes;
		const idx_t copy_start = MaxValue(block_start, actual_read_location);
		const idx_t copy_end = MinValue(block_end, end_pos);
		
		// Skip if no overlap
		if (copy_start >= copy_end) {
			continue;
		}

		// Calculate offsets and bytes to copy
		const idx_t bytes_to_copy = copy_end - copy_start;
		const idx_t offset_in_block = copy_start - block_start;
		const idx_t offset_in_buffer = copy_start - actual_read_location;

		// Copy block data
		D_ASSERT(pin.IsValid());
		memcpy(buffer + offset_in_buffer, pin.Ptr() + block->block_offset + offset_in_block, bytes_to_copy);
	}
}

} // namespace duckdb
