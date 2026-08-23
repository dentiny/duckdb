#include "duckdb/storage/external_file_cache/caching_file_system_layer.hpp"

#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"
#include "duckdb/storage/caching_mode.hpp"

namespace duckdb {

string CachingFileSystemLayer::GetName() const {
	return NAME;
}

shared_ptr<FileSystem> CachingFileSystemLayer::Wrap(shared_ptr<FileSystem> file_system,
                                                    const FileSystemLayerOptions &layer_options, const OpenFileInfo &,
                                                    FileOpenFlags, optional_ptr<FileOpener> opener) const {
	auto mode = CachingMode::CACHE_REMOTE_ONLY;
	auto cache_local_files = layer_options.options.find(CACHE_LOCAL_FILES);
	if (cache_local_files != layer_options.options.end() && BooleanValue::Get(cache_local_files->second)) {
		mode = CachingMode::ALWAYS_CACHE;
	}
	return make_shared_ptr<CachingFileSystemWrapper>(std::move(file_system), opener, mode);
}

} // namespace duckdb
