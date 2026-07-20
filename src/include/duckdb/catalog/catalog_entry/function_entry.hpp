//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/function_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"

namespace duckdb {

//! A function in the catalog
class FunctionEntry : public StandardEntry {
public:
	FunctionEntry(CatalogType type, Catalog &catalog, SchemaCatalogEntry &schema, CreateFunctionInfo &info)
	    : StandardEntry(type, schema, catalog, info.GetFunctionName()) {
		descriptions = std::move(info.descriptions);
		alias_of = std::move(info.alias_of);
		this->blocking_dependencies = info.blocking_dependencies;
		this->recreation_only_dependencies = info.recreation_only_dependencies;
		this->internal = info.internal;
		this->extension_name = info.extension_name;
	}

	Identifier alias_of;
	vector<FunctionDescription> descriptions;
};
} // namespace duckdb
