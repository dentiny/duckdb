//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_system_layer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

//! A FileSystemLayer wraps the filesystem selected by VirtualFileSystem for an OpenFile call.
class DUCKDB_API FileSystemLayer {
public:
	virtual ~FileSystemLayer() = default;
	//! Return the name used by OpenFileInfo to request this layer.
	virtual string GetName() const = 0;

	//! Layers run in file option order, with later layers on the outside.
	//! A wrapper must retain its inner filesystem and keep itself alive while handles dispatch through it.
	virtual shared_ptr<FileSystem> Wrap(shared_ptr<FileSystem> file_system, const FileSystemLayerOptions &layer_options,
	                                    const OpenFileInfo &file, FileOpenFlags flags,
	                                    optional_ptr<FileOpener> opener) const = 0;
};

} // namespace duckdb
