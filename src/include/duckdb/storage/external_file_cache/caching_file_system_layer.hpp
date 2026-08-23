//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache/caching_file_system_layer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system_layer.hpp"

namespace duckdb {

class DUCKDB_API CachingFileSystemLayer : public FileSystemLayer {
public:
	static constexpr const char *NAME = "external_file_cache";
	static constexpr const char *CACHE_LOCAL_FILES = "cache_local_files";

	string GetName() const override;
	shared_ptr<FileSystem> Wrap(shared_ptr<FileSystem> file_system, const FileSystemLayerOptions &layer_options,
	                            const OpenFileInfo &file, FileOpenFlags flags,
	                            optional_ptr<FileOpener> opener) const override;

	static OpenFileInfo AddTo(OpenFileInfo file, bool cache_local_files = false) {
		unordered_map<string, Value> options;
		if (cache_local_files) {
			options[CACHE_LOCAL_FILES] = Value::BOOLEAN(true);
		}
		return file.WithFileSystemLayer(NAME, std::move(options));
	}
};

} // namespace duckdb
