//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_memory_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {

class Allocator;
class BlockAllocator;
class BufferPool;
class TemporaryMemoryManager;
class ObjectCache;

//! Owns the memory-management components shared by one or more DatabaseInstances.
class DatabaseMemoryManager {
public:
	DatabaseMemoryManager(shared_ptr<Allocator> allocator, shared_ptr<BlockAllocator> block_allocator,
	                      shared_ptr<TemporaryMemoryManager> temporary_memory_manager,
	                      shared_ptr<BufferPool> buffer_pool, shared_ptr<ObjectCache> object_cache);
	~DatabaseMemoryManager();

	DUCKDB_API Allocator &GetAllocator() const;
	DUCKDB_API BlockAllocator &GetBlockAllocator() const;
	DUCKDB_API TemporaryMemoryManager &GetTemporaryMemoryManager() const;
	DUCKDB_API BufferPool &GetBufferPool() const;
	DUCKDB_API ObjectCache &GetObjectCache() const;

	DUCKDB_API const shared_ptr<Allocator> &GetAllocatorHandle() const;
	DUCKDB_API const shared_ptr<BlockAllocator> &GetBlockAllocatorHandle() const;
	DUCKDB_API const shared_ptr<TemporaryMemoryManager> &GetTemporaryMemoryManagerHandle() const;
	DUCKDB_API const shared_ptr<BufferPool> &GetBufferPoolHandle() const;
	DUCKDB_API const shared_ptr<ObjectCache> &GetObjectCacheHandle() const;

private:
	shared_ptr<Allocator> allocator;
	shared_ptr<BlockAllocator> block_allocator;
	shared_ptr<TemporaryMemoryManager> temporary_memory_manager;
	shared_ptr<BufferPool> buffer_pool;
	shared_ptr<ObjectCache> object_cache;
};

} // namespace duckdb
