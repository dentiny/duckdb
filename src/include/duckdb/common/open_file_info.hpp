//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/open_file_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct FileSystemLayerOptions {
	FileSystemLayerOptions(string name_p, unordered_map<string, Value> options_p)
	    : name(std::move(name_p)), options(std::move(options_p)) {
	}

	string name;
	unordered_map<string, Value> options;
};

struct ExtendedOpenFileInfo {
	unordered_map<string, Value> options;
	//! Filesystem layers to apply in inner-to-outer order.
	vector<FileSystemLayerOptions> file_system_layers;
};

struct OpenFileInfo {
	OpenFileInfo() = default;
	OpenFileInfo(string path_p) // NOLINT: allow implicit conversion from string
	    : path(std::move(path_p)) {
	}

	string path;
	shared_ptr<ExtendedOpenFileInfo> extended_info;

public:
	//! Return a copy with an additional filesystem layer request.
	OpenFileInfo WithFileSystemLayer(string name, unordered_map<string, Value> options = {}) const {
		auto result = *this;
		result.extended_info = extended_info ? make_shared_ptr<ExtendedOpenFileInfo>(*extended_info)
		                                     : make_shared_ptr<ExtendedOpenFileInfo>();
		result.extended_info->file_system_layers.emplace_back(std::move(name), std::move(options));
		return result;
	}

	bool operator<(const OpenFileInfo &rhs) const {
		return path < rhs.path;
	}
};

} // namespace duckdb
