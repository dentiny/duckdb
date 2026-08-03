//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/database_memory_config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Configuration shared by every database in a memory-management domain.
struct DatabaseMemoryConfig {
	static constexpr idx_t DEFAULT_MAXIMUM_MEMORY = DConstants::INVALID_INDEX;
	static constexpr idx_t DEFAULT_BLOCK_ALLOCATOR_SIZE = 0;
	static constexpr bool DEFAULT_BUFFER_MANAGER_TRACK_EVICTION_TIMESTAMPS = false;
	static constexpr idx_t DEFAULT_ALLOCATOR_BULK_DEALLOCATION_FLUSH_THRESHOLD = 536870912ULL;

	DatabaseMemoryConfig() = default;
	DUCKDB_API DatabaseMemoryConfig(const DatabaseMemoryConfig &other);
	DUCKDB_API DatabaseMemoryConfig &operator=(const DatabaseMemoryConfig &other);
	DUCKDB_API bool IsDefault() const;

	//! The maximum memory used by the database system (in bytes). Default: 80% of available system memory.
	atomic<idx_t> maximum_memory {DEFAULT_MAXIMUM_MEMORY};
	//! Physical memory that the block allocator is allowed to use.
	atomic<idx_t> block_allocator_size {DEFAULT_BLOCK_ALLOCATOR_SIZE};
	//! Record timestamps of buffer manager unpin() events. Usable by custom eviction policies.
	bool buffer_manager_track_eviction_timestamps = DEFAULT_BUFFER_MANAGER_TRACK_EVICTION_TIMESTAMPS;
	//! If bulk deallocation larger than this occurs, flush outstanding allocations.
	atomic<idx_t> allocator_bulk_deallocation_flush_threshold {DEFAULT_ALLOCATOR_BULK_DEALLOCATION_FLUSH_THRESHOLD};
};

} // namespace duckdb
