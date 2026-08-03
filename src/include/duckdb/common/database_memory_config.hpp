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
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class Allocator;
class BlockAllocator;

//! Configuration and initialization state for a memory-management domain.
struct DatabaseMemoryConfig {
	static constexpr idx_t DEFAULT_MAXIMUM_MEMORY = DConstants::INVALID_INDEX;
	static constexpr idx_t DEFAULT_BLOCK_ALLOCATOR_SIZE = 0;
	static constexpr bool DEFAULT_BUFFER_MANAGER_TRACK_EVICTION_TIMESTAMPS = false;
	static constexpr idx_t DEFAULT_ALLOCATOR_BULK_DEALLOCATION_FLUSH_THRESHOLD = 536870912ULL;

	DUCKDB_API DatabaseMemoryConfig() = default;
	DUCKDB_API DatabaseMemoryConfig(DatabaseMemoryConfig &&other) noexcept;
	DUCKDB_API DatabaseMemoryConfig &operator=(DatabaseMemoryConfig &&other) noexcept;
	DatabaseMemoryConfig(const DatabaseMemoryConfig &other) = delete;
	DatabaseMemoryConfig &operator=(const DatabaseMemoryConfig &other) = delete;
	// Whether this configuration has been explicitly set.
	DUCKDB_API bool IsDefault() const;

	//! The maximum memory used by the database system (in bytes). Initially unset; when available system memory can be
	//! detected, database initialization defaults this to 80% of that value.
	atomic<idx_t> maximum_memory {DEFAULT_MAXIMUM_MEMORY};
	//! Physical memory that the block allocator is allowed to use.
	atomic<idx_t> block_allocator_size {DEFAULT_BLOCK_ALLOCATOR_SIZE};
	//! Record timestamps of buffer manager unpin() events. Usable by custom eviction policies.
	bool buffer_manager_track_eviction_timestamps = DEFAULT_BUFFER_MANAGER_TRACK_EVICTION_TIMESTAMPS;
	//! If bulk deallocation larger than this occurs, flush outstanding allocations.
	atomic<idx_t> allocator_bulk_deallocation_flush_threshold {DEFAULT_ALLOCATOR_BULK_DEALLOCATION_FLUSH_THRESHOLD};
	//! Initialization-only allocator ownership, consumed when the memory manager is created.
	unique_ptr<Allocator> allocator;
	//! Initialization-only block allocator ownership, consumed when the memory manager is created.
	unique_ptr<BlockAllocator> block_allocator;
};

} // namespace duckdb
