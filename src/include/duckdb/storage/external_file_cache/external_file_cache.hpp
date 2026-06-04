//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache/external_file_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"
#include "duckdb/storage/external_file_cache/external_file_cache_block.hpp"

namespace duckdb {

// Forward declaration.
class BlockMemory;
class ClientContext;
class DatabaseInstance;
class BlockHandle;
class BufferManager;

class ExternalFileCache {
public:
	//! Get the cache block size for a given file path.
	DUCKDB_API idx_t GetCacheBlockSize(const string &path) const;

	//! Cached files
	struct CachedFile {
	public:
		CachedFile(string path_p, idx_t generation_p);

	public:
		//! Whether the CachedFile is still valid given the current modified/version tag
		bool IsValid(bool validate, const string &current_version_tag, timestamp_t current_last_modified);

		const string path;
		const idx_t generation;

		mutable annotated_mutex map_lock;
		//! The block size used to index the current block map. Invalid if no blocks have been cached yet.
		optional_idx cached_block_size DUCKDB_GUARDED_BY(map_lock);
		//! Maps from block index to cached block.
		unordered_map<idx_t, shared_ptr<CacheBlock>> blocks DUCKDB_GUARDED_BY(map_lock);

		mutable annotated_mutex meta_lock;
		idx_t file_size DUCKDB_GUARDED_BY(meta_lock) = 0;
		timestamp_t last_modified DUCKDB_GUARDED_BY(meta_lock) = timestamp_t(0);
		string version_tag DUCKDB_GUARDED_BY(meta_lock);
		bool can_seek DUCKDB_GUARDED_BY(meta_lock) = false;
		bool on_disk_file DUCKDB_GUARDED_BY(meta_lock) = false;
	};

public:
	ExternalFileCache(DatabaseInstance &db, bool enable);

public:
	static ExternalFileCache &Get(DatabaseInstance &db);
	static ExternalFileCache &Get(ClientContext &context);

	bool IsEnabled() const;
	void SetEnabled(bool enable);
	idx_t GetGeneration() const;
	vector<CachedFileInformation> GetCachedFileInformation() const;
	//! Number of files tracked in the cache map, expose for testing purpose.
	idx_t GetCachedFileCount() const;

	//! Re-index to `current_block_size` if it differs from the cache block size.
	//! Return the blocks cached for the given range.
	vector<shared_ptr<CacheBlock>> ReindexAndAcquireBlocks(const shared_ptr<CachedFile> &cached_file,
	                                                       idx_t current_block_size, idx_t first_block,
	                                                       idx_t num_blocks);

	BufferManager &GetBufferManager() const;
	//! Gets the shared cached file for the given path, creating it if not yet present.
	//! When caching is disabled, returns a transient CachedFile that is not tracked in the cached file map.
	shared_ptr<CachedFile> GetOrCreateCachedFile(const string &path);

	//! Register a cached block with the buffer pool block backing it.
	void RegisterBlock(const shared_ptr<BlockHandle> &block_handle, const shared_ptr<CachedFile> &cached_file,
	                   idx_t block_idx);
	//! Called by the buffer pool when an external file cache block is evicted.
	void Evict(BlockMemory &memory);

	DUCKDB_API static bool IsValid(bool validate, const string &cached_version_tag, timestamp_t cached_last_modified,
	                               const string &current_version_tag, timestamp_t current_last_modified);

private:
	struct ExternalFileCacheBlockKey {
		shared_ptr<CachedFile> cached_file;
		idx_t block_idx;
	};

	//! Remove a block from the reverse lookup map without updating cached file state.
	void UnregisterBlock(BlockMemory &memory) DUCKDB_REQUIRES(block_keys_lock);

	//! Re-index blocks of a single cached file.
	void ReindexCachedFileCore(const shared_ptr<CachedFile> &cached_file, idx_t file_size, idx_t old_block_size,
	                           idx_t new_block_size) DUCKDB_REQUIRES(cached_file->map_lock);

	//! The BufferManager used to cache files
	BufferManager &buffer_manager;
	//! Whether or not file caching is enabled
	atomic<bool> enable;
	//! Generation counter, incremented whenever cache enablement changes.
	atomic<idx_t> generation;
	//! Mapping from file path to cached file with cached blocks
	unordered_map<string, shared_ptr<CachedFile>> cached_files;
	//! Reverse lookup from buffer pool block memory to cached file block
	unordered_map<BlockMemory *, ExternalFileCacheBlockKey> block_keys;
	//! Lock for accessing block_keys
	mutable mutex block_keys_lock;
	//! Lock for accessing the cached files
	mutable mutex lock;
};

} // namespace duckdb
