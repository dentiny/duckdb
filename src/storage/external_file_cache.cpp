#include "duckdb/storage/external_file_cache.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

ExternalFileCache::CachedFileRange::CachedFileRange(shared_ptr<BlockHandle> block_handle_p, idx_t nr_bytes_p,
                                                    idx_t location_p, string version_tag_p)
    : block_handle(std::move(block_handle_p)), nr_bytes(nr_bytes_p), location(location_p),
      version_tag(std::move(version_tag_p)) {
}

ExternalFileCache::CachedFileRange::~CachedFileRange() {
	VerifyCheckSum();
}

ExternalFileCache::CachedFileRangeOverlap
ExternalFileCache::CachedFileRange::GetOverlap(const idx_t other_nr_bytes, const idx_t other_location) const {
	const auto this_end = this->location + this->nr_bytes;
	const auto other_end = other_nr_bytes + other_location;
	if (this->location <= other_location && this_end >= other_end) {
		return CachedFileRangeOverlap::FULL;
	}
	if (this->location < other_end && other_location < this_end) {
		return CachedFileRangeOverlap::PARTIAL;
	}
	return CachedFileRangeOverlap::NONE;
}

ExternalFileCache::CachedFileRangeOverlap
ExternalFileCache::CachedFileRange::GetOverlap(const CachedFileRange &other) const {
	return GetOverlap(other.nr_bytes, other.location);
}

void ExternalFileCache::CachedFileRange::AddCheckSum() {
#ifdef DEBUG
	D_ASSERT(checksum == 0);
	auto buffer_handle = block_handle->GetMemory().GetBufferManager().Pin(block_handle);
	checksum = Checksum(buffer_handle.Ptr(), nr_bytes);
#endif
}

void ExternalFileCache::CachedFileRange::VerifyCheckSum() {
#ifdef DEBUG
	if (checksum == 0) {
		return;
	}
	auto buffer_handle = block_handle->GetMemory().GetBufferManager().Pin(block_handle);
	if (!buffer_handle.IsValid()) {
		return;
	}
	D_ASSERT(checksum == Checksum(buffer_handle.Ptr(), nr_bytes));
#endif
}

ExternalFileCache::CachedFile::CachedFile(string path_p)
    : path(std::move(path_p)), file_size(0), last_modified(0), can_seek(false), on_disk_file(false) {
}

ExternalFileCache::CachedFileRangeCleanup::CachedFileRangeCleanup(ExternalFileCache &cache_p, string path_p,
                                                                  weak_ptr<CachedFile> cached_file_p)
    : cache(cache_p), path(std::move(path_p)), cached_file(std::move(cached_file_p)), completed(false) {
}

void ExternalFileCache::CachedFileRangeCleanup::Run() {
	if (completed.exchange(true)) {
		return;
	}
	cache.ReleaseCachedFileRange(path, cached_file);
}

void ExternalFileCache::CachedFile::Verify(const unique_ptr<StorageLockKey> &guard) const {
#ifdef DEBUG
	for (const auto &range1 : ranges) {
		for (const auto &range2 : ranges) {
			if (range1.first == range2.first) {
				continue;
			}
			D_ASSERT(range1.second->GetOverlap(*range2.second) != CachedFileRangeOverlap::FULL);
		}
	}
#endif
}

bool ExternalFileCache::IsValid(bool validate, const string &cached_version_tag, timestamp_t cached_last_modified,
                                const string &current_version_tag, timestamp_t current_last_modified) {
	if (!validate) {
		return true; // Assume valid
	}
	if (!current_version_tag.empty() || !cached_version_tag.empty()) {
		return cached_version_tag == current_version_tag; // Validity checked by version tag (httpfs)
	}
	if (cached_last_modified != current_last_modified) {
		return false; // The file has certainly been modified
	}
	// The last modified time matches. However, we cannot blindly trust this,
	// because some file systems use a low resolution clock to set the last modified time.
	// So, we will require that the last modified time is more than 10 seconds ago.
	static constexpr int64_t LAST_MODIFIED_THRESHOLD = 10LL * 1000LL * 1000LL;
	const auto access_time = Timestamp::GetCurrentTimestamp();
	if (access_time < current_last_modified) {
		return false; // Last modified in the future?
	}
	return access_time - current_last_modified > LAST_MODIFIED_THRESHOLD;
}

bool ExternalFileCache::CachedFile::IsValid(const unique_ptr<StorageLockKey> &guard, bool validate,
                                            const string &current_version_tag, timestamp_t current_last_modified) {
	if (!validate) {
		return true; // Assume valid
	}
	return ExternalFileCache::IsValid(validate, VersionTag(guard), LastModified(guard), current_version_tag,
	                                  current_last_modified);
}

idx_t &ExternalFileCache::CachedFile::FileSize(const unique_ptr<StorageLockKey> &guard) {
	return file_size;
}

timestamp_t &ExternalFileCache::CachedFile::LastModified(const unique_ptr<StorageLockKey> &guard) {
	return last_modified;
}

string &ExternalFileCache::CachedFile::VersionTag(const unique_ptr<StorageLockKey> &guard) {
	return version_tag;
}

bool &ExternalFileCache::CachedFile::CanSeek(const unique_ptr<StorageLockKey> &guard) {
	return can_seek;
}

bool &ExternalFileCache::CachedFile::OnDiskFile(const unique_ptr<StorageLockKey> &guard) {
	return on_disk_file;
}

map<idx_t, shared_ptr<ExternalFileCache::CachedFileRange>> &
ExternalFileCache::CachedFile::Ranges(const unique_ptr<StorageLockKey> &guard) {
	return ranges;
}

ExternalFileCache::ExternalFileCache(DatabaseInstance &db, bool enable_p)
    : buffer_manager(BufferManager::GetBufferManager(db)), enable(enable_p) {
}

bool ExternalFileCache::IsEnabled() const {
	return enable;
}

void ExternalFileCache::SetEnabled(bool enable_p) {
	lock_guard<mutex> guard(lock);
	enable = enable_p;
	if (!enable) {
		cached_files.clear();
	}
}

vector<CachedFileInformation> ExternalFileCache::GetCachedFileInformation() const {
	unique_lock<mutex> files_guard(lock);
	vector<CachedFileInformation> result;
	for (const auto &file : cached_files) {
		auto ranges_guard = file.second->lock.GetSharedLock();
		for (const auto &range_entry : file.second->Ranges(ranges_guard)) {
			const auto &range = *range_entry.second;
			result.push_back(
			    {file.first, range.nr_bytes, range.location, !range.block_handle->GetMemory().IsUnloaded()});
		}
	}
	return result;
}

idx_t ExternalFileCache::GetCachedFileCount() const {
	lock_guard<mutex> guard(lock);
	return cached_files.size();
}

ExternalFileCache &ExternalFileCache::Get(DatabaseInstance &db) {
	return db.GetExternalFileCache();
}

ExternalFileCache &ExternalFileCache::Get(ClientContext &context) {
	return context.db->GetExternalFileCache();
}

BufferManager &ExternalFileCache::GetBufferManager() const {
	return buffer_manager;
}

shared_ptr<ExternalFileCache::CachedFile> ExternalFileCache::GetOrCreateCachedFile(const string &path) {
	lock_guard<mutex> guard(lock);
	auto &entry = cached_files[path];
	if (!entry) {
		entry = make_shared_ptr<CachedFile>(path);
	}
	entry->active_handle_count++;
	return entry;
}

void ExternalFileCache::ReleaseCachedFileHandle(const shared_ptr<CachedFile> &cached_file) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(cached_file->active_handle_count > 0);
	if (cached_file->active_handle_count > 0) {
		cached_file->active_handle_count--;
	}
	TryEraseFileLocked(cached_file);
}

shared_ptr<ExternalFileCache::CachedFileRangeCleanup>
ExternalFileCache::RegisterCachedFileRange(const shared_ptr<CachedFile> &cached_file,
                                           const shared_ptr<BlockHandle> &block_handle) {
	auto cleanup = make_shared_ptr<CachedFileRangeCleanup>(*this, cached_file->path, weak_ptr<CachedFile>(cached_file));
	{
		lock_guard<mutex> guard(lock);
		cached_file->loaded_range_count++;
	}
	block_handle->GetMemory().SetUnloadCallback([cleanup]() { cleanup->Run(); });
	return cleanup;
}

void ExternalFileCache::ReleaseCachedFileRange(const string &path, const weak_ptr<CachedFile> &cached_file) {
	auto locked_file = cached_file.lock();
	if (!locked_file) {
		return;
	}
	lock_guard<mutex> guard(lock);
	D_ASSERT(locked_file->loaded_range_count > 0);
	if (locked_file->loaded_range_count > 0) {
		locked_file->loaded_range_count--;
	}
	TryEraseFileLocked(locked_file);
}

void ExternalFileCache::TryEraseFile(const shared_ptr<CachedFile> &cached_file) {
	lock_guard<mutex> guard(lock);
	TryEraseFileLocked(cached_file);
}

void ExternalFileCache::TryEraseFileLocked(const shared_ptr<CachedFile> &cached_file) {
	if (cached_file->active_handle_count != 0 || cached_file->loaded_range_count != 0) {
		return;
	}
	auto entry = cached_files.find(cached_file->path);
	if (entry == cached_files.end()) {
		return;
	}
	if (entry->second.get() != cached_file.get()) {
		return;
	}
	cached_files.erase(entry);
}

} // namespace duckdb
