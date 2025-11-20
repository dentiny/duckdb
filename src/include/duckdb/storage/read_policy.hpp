//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/read_policy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

//! Result of read policy calculation
struct ReadPolicyResult {
	// The actual file location to read from.
	idx_t read_location;
	// The actual number of bytes to read
	idx_t read_bytes;
};

//! Multiple read ranges from read policy calculation
struct ReadPolicyRanges {
	// The ranges to read (one per block/gap)
	vector<ReadPolicyResult> ranges;
	// The total span of all ranges (for buffer allocation)
	idx_t total_location;  // Start of first range
	idx_t total_bytes;     // Total bytes from start to end
};

//! Base class for read policies that determine how many bytes to read and cache
class ReadPolicy {
public:
	virtual ~ReadPolicy() = default;
	//! Calculate the number of bytes to read and cache given the requested bytes, location, and next range location.
	virtual ReadPolicyResult CalculateBytesToRead(idx_t nr_bytes, idx_t location, idx_t file_size,
	                                              optional_idx start_location_of_next_range) = 0;
	
	//! Calculate multiple ranges to read (for block-aligned policies that may span multiple blocks)
	//! Default implementation returns a single range using CalculateBytesToRead
	virtual ReadPolicyRanges CalculateRangesToRead(idx_t nr_bytes, idx_t location, idx_t file_size,
	                                               optional_idx start_location_of_next_range);
};

//! Default read policy that fills gaps between cached ranges
class DefaultReadPolicy : public ReadPolicy {
public:
	ReadPolicyResult CalculateBytesToRead(idx_t nr_bytes, idx_t location, idx_t file_size,
	                                      optional_idx start_location_of_next_range) override;
};

//! Read policy that aligns reads to block boundaries (hardcoded to 2MiB)
class AlignedReadPolicy : public ReadPolicy {
public:
	AlignedReadPolicy() = default;
	ReadPolicyResult CalculateBytesToRead(idx_t nr_bytes, idx_t location, idx_t file_size,
	                                      optional_idx start_location_of_next_range) override;
	
	//! Returns multiple ranges, one for each block that needs to be read
	ReadPolicyRanges CalculateRangesToRead(idx_t nr_bytes, idx_t location, idx_t file_size,
	                                       optional_idx start_location_of_next_range) override;
};

} // namespace duckdb
