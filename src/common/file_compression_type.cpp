#include "duckdb/common/file_compression_type.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

const FileCompressionType FILE_UNCOMPRESSED_TYPE = "uncompressed";
const FileCompressionType FILE_ZSTD_COMPRESSION_TYPE = "zstd";
const FileCompressionType FILE_GZIP_COMPRESSION_TYPE = "gzip";
const FileCompressionType FILE_AUTO_COMPRESSION_TYPE = "auto";

string CompressionExtensionFromType(const FileCompressionType& type) {
	if (StringUtil::CIEquals(type, FILE_GZIP_COMPRESSION_TYPE)) {
		return ".gz";
	}
	if (StringUtil::CIEquals(type, FILE_ZSTD_COMPRESSION_TYPE)) {
		return ".zst";
	}
	throw NotImplementedException("Compression Extension of file compression type %s is not implemented", type);
}

bool IsFileCompressed(string path, const FileCompressionType& compression_type) {
	std::size_t question_mark_pos = std::string::npos;
	if (!StringUtil::StartsWith(path, "\\\\?\\")) {
		question_mark_pos = path.find('?');
	}
	path = path.substr(0, question_mark_pos);
	if (StringUtil::EndsWith(path, CompressionExtensionFromType(compression_type))) {
		return true;
	}
	return false;
}

} // namespace duckdb
