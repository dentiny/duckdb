#include "duckdb/main/database_memory_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

DatabaseMemoryManager::DatabaseMemoryManager(unique_ptr<Allocator> allocator_p,
                                             unique_ptr<BlockAllocator> block_allocator_p,
                                             DatabaseMemoryConfig config_p)
    : config(std::move(config_p)), allocator(std::move(allocator_p)), block_allocator(std::move(block_allocator_p)) {
	if (!allocator || !block_allocator) {
		throw InternalException("DatabaseMemoryManager cannot contain null allocator components");
	}
	if (&block_allocator->GetAllocator() != allocator.get()) {
		throw InternalException("DatabaseMemoryManager allocator components belong to different memory domains");
	}
	temporary_memory_manager = make_uniq<TemporaryMemoryManager>();
	buffer_pool = make_uniq<BufferPool>(*block_allocator, *temporary_memory_manager, config);
	object_cache = make_uniq<ObjectCache>(*buffer_pool);
	buffer_pool->RegisterObjectCache(*object_cache);
}

DatabaseMemoryManager::~DatabaseMemoryManager() {
	buffer_pool->UnregisterObjectCache(*object_cache);
	block_allocator->FlushAll();
	Allocator::SetBackgroundThreads(false);
}

shared_ptr<DatabaseMemoryManager> DatabaseMemoryManager::Create(DatabaseMemoryConfig memory_config,
                                                                DBConfig &db_config) {
	auto allocator = std::move(memory_config.allocator);
	auto block_allocator = std::move(memory_config.block_allocator);
	if (!allocator) {
		allocator = make_uniq<Allocator>();
	}
	if (!block_allocator) {
		auto default_block_size = Settings::Get<DefaultBlockSizeSetting>(db_config);
		idx_t maximum_memory = memory_config.maximum_memory;
		if (maximum_memory == DConstants::INVALID_INDEX) {
			maximum_memory = memory_config.block_allocator_size;
		}
		block_allocator = make_uniq<BlockAllocator>(*allocator, default_block_size, maximum_memory,
		                                            memory_config.block_allocator_size);
	}
	return make_shared_ptr<DatabaseMemoryManager>(std::move(allocator), std::move(block_allocator),
	                                              std::move(memory_config));
}

Allocator &DatabaseMemoryManager::GetAllocator() const {
	return *allocator;
}

BlockAllocator &DatabaseMemoryManager::GetBlockAllocator() const {
	return *block_allocator;
}

TemporaryMemoryManager &DatabaseMemoryManager::GetTemporaryMemoryManager() const {
	return *temporary_memory_manager;
}

BufferPool &DatabaseMemoryManager::GetBufferPool() const {
	return *buffer_pool;
}

ObjectCache &DatabaseMemoryManager::GetSharedObjectCache() const {
	return *object_cache;
}

const DatabaseMemoryConfig &DatabaseMemoryManager::GetConfig() const {
	return config;
}

void DatabaseMemoryManager::SetMaximumMemory(idx_t maximum_memory, const char *exception_postscript) {
	buffer_pool->SetLimit(maximum_memory, exception_postscript);
}

void DatabaseMemoryManager::SetBlockAllocatorSize(idx_t block_allocator_size) {
	block_allocator->Resize(block_allocator_size);
	config.block_allocator_size = block_allocator_size;
}

void DatabaseMemoryManager::SetAllocatorBulkDeallocationFlushThreshold(idx_t threshold) {
	buffer_pool->SetAllocatorBulkDeallocationFlushThreshold(threshold);
}

} // namespace duckdb
