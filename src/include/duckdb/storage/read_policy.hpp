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

// Indicates a range read request
struct ReadPolicyResult {
	// The actual file location to read from.
	idx_t read_location;
	// The actual number of bytes to read
	idx_t read_bytes;
};

// Multiple read ranges from read policy calculation
struct ReadPolicyRanges {
	// The ranges to read, which are sorted by location in ascending order
	vector<ReadPolicyResult> ranges;
	// Start of first range
	idx_t total_location;
	// Total bytes from start to end
	idx_t total_bytes;
};

//! Base class for read policies that determine how many bytes to read and cache
class ReadPolicy {
public:
	virtual ~ReadPolicy() = default;

	//! Calculate multiple ranges to read
	//! Ranges are returned sorted by location in ascending order, and they don't overlap
	virtual ReadPolicyRanges CalculateRangesToRead(idx_t nr_bytes, idx_t location, idx_t file_size) = 0;
};

//! Default read policy that returns a single range covering the requested bytes
class DefaultReadPolicy : public ReadPolicy {
public:
	ReadPolicyRanges CalculateRangesToRead(idx_t nr_bytes, idx_t location, idx_t file_size) override;
};

//! Read policy that aligns reads to block boundaries (hardcoded to 2MiB)
class AlignedReadPolicy : public ReadPolicy {
public:
	AlignedReadPolicy() = default;

	//! Returns multiple ranges, one for each block that needs to be read
	ReadPolicyRanges CalculateRangesToRead(idx_t nr_bytes, idx_t location, idx_t file_size) override;
};

} // namespace duckdb
