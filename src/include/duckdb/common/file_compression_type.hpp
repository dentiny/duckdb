//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_compression_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

// Alias
using FileCompressionType = string;

// DuckDB internally supported compression types.
extern const FileCompressionType UNCOMPRESSED_COMPRESSION_TYPE;
extern const FileCompressionType ZSTD_COMPRESSION_TYPE;
extern const FileCompressionType GZIP_COMPRESSION_TYPE;
// Used at read, which automatically detects applicable decompression.
extern const FileCompressionType AUTO_COMPRESSION_TYPE;

// Return whether the given filepath is compressed with the given type.
bool IsFileCompressed(string path, const FileCompressionType& compression_type);

// Return file extension for a given compression type (e.g. ".gz" for gzip).
string CompressionExtensionFromType(const FileCompressionType& type);

} // namespace duckdb
