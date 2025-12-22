#include "duckdb/storage/read_policy.hpp"

namespace duckdb {

namespace {

// Default block size for aligned read, which is made for object storage access.
constexpr idx_t ALIGNED_READ_BLOCK_SIZE = 2ULL * 1024 * 1024; // 2MiB

// Align a value down to the nearest multiple of ALIGNED_READ_BLOCK_SIZE.
idx_t AlignDown(idx_t value) {
	return (value / ALIGNED_READ_BLOCK_SIZE) * ALIGNED_READ_BLOCK_SIZE;
}

// Align a value up to the nearest multiple of ALIGNED_READ_BLOCK_SIZE.
idx_t AlignUp(idx_t value) {
	return ((value + ALIGNED_READ_BLOCK_SIZE - 1) / ALIGNED_READ_BLOCK_SIZE) * ALIGNED_READ_BLOCK_SIZE;
}

// Util function for default read policy.
bool ShouldExpandToFillGap(const idx_t current_length, const idx_t added_length) {
	const idx_t MAX_BOUND_TO_BE_ADDED_LENGTH = 1048576;

	if (added_length > MAX_BOUND_TO_BE_ADDED_LENGTH) {
		// Absolute value of what would be needed to added is too high
		return false;
	}
	if (added_length > current_length) {
		// Relative value of what would be needed to added is too high
		return false;
	}

	return true;
}

} // namespace

ReadPolicyRanges DefaultReadPolicy::CalculateRangesToRead(idx_t nr_bytes, idx_t location, idx_t file_size) {
	// Default implementation: single range covering the requested bytes
	idx_t read_bytes = nr_bytes;
	// Make sure we don't read past the end of the file
	if (location + read_bytes > file_size) {
		read_bytes = file_size - location;
	}

	ReadPolicyRanges ranges;
	ranges.ranges.push_back({location, read_bytes});
	ranges.total_location = location;
	ranges.total_bytes = read_bytes;
	return ranges;
}

ReadPolicyRanges AlignedReadPolicy::CalculateRangesToRead(idx_t nr_bytes, idx_t location, idx_t file_size) {
	const idx_t aligned_start = AlignDown(location);
	const idx_t requested_end = location + nr_bytes;
	idx_t aligned_end = AlignUp(requested_end);

	// Ensure we don't read past the end of the file
	if (aligned_end > file_size) {
		aligned_end = file_size;
	}

	// Split into individual block ranges
	ReadPolicyRanges result;
	result.total_location = aligned_start;
	result.total_bytes = aligned_end - aligned_start;

	for (idx_t block_start = aligned_start; block_start < aligned_end; block_start += ALIGNED_READ_BLOCK_SIZE) {
		idx_t block_end = MinValue(block_start + ALIGNED_READ_BLOCK_SIZE, aligned_end);
		result.ranges.push_back({block_start, block_end - block_start});
	}

	return result;
}

} // namespace duckdb
