#include "duckdb/common/database_memory_config.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/storage/block_allocator.hpp"

namespace duckdb {

DatabaseMemoryConfig::DatabaseMemoryConfig() = default;

DatabaseMemoryConfig::~DatabaseMemoryConfig() = default;

DatabaseMemoryConfig::DatabaseMemoryConfig(DatabaseMemoryConfig &&other) noexcept
    : maximum_memory(other.maximum_memory.load()), block_allocator_size(other.block_allocator_size.load()),
      buffer_manager_track_eviction_timestamps(other.buffer_manager_track_eviction_timestamps),
      allocator_bulk_deallocation_flush_threshold(other.allocator_bulk_deallocation_flush_threshold.load()),
      allocator(std::move(other.allocator)), block_allocator(std::move(other.block_allocator)) {
}

DatabaseMemoryConfig &DatabaseMemoryConfig::operator=(DatabaseMemoryConfig &&other) noexcept {
	if (this == &other) {
		return *this;
	}
	maximum_memory = other.maximum_memory.load();
	block_allocator_size = other.block_allocator_size.load();
	buffer_manager_track_eviction_timestamps = other.buffer_manager_track_eviction_timestamps;
	allocator_bulk_deallocation_flush_threshold = other.allocator_bulk_deallocation_flush_threshold.load();
	allocator = std::move(other.allocator);
	block_allocator = std::move(other.block_allocator);
	return *this;
}

bool DatabaseMemoryConfig::IsDefault() const {
	return !allocator && !block_allocator && maximum_memory == DEFAULT_MAXIMUM_MEMORY &&
	       block_allocator_size == DEFAULT_BLOCK_ALLOCATOR_SIZE &&
	       buffer_manager_track_eviction_timestamps == DEFAULT_BUFFER_MANAGER_TRACK_EVICTION_TIMESTAMPS &&
	       allocator_bulk_deallocation_flush_threshold == DEFAULT_ALLOCATOR_BULK_DEALLOCATION_FLUSH_THRESHOLD;
}

} // namespace duckdb
