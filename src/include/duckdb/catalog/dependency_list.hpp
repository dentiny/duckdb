//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry_map.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/dependency.hpp"

namespace duckdb {
class Catalog;
class CatalogEntry;
struct CreateInfo;
class SchemaCatalogEntry;
struct CatalogTransaction;
class LogicalDependencyList;

//! Controls how a dependency affects the dependent catalog entry.
enum class LogicalDependencyType : uint8_t {
	//! Blocks DROP/ALTER operations and ensures the dependency is recreated first.
	BLOCKING,
	//! Only ensures the dependency is recreated first; does not block DROP/ALTER operations.
	RECREATION_ONLY
};

//! A minimal representation of a CreateInfo / CatalogEntry
//! enough to look up the entry inside SchemaCatalogEntry::GetEntry
struct LogicalDependency {
public:
	CatalogEntryInfo entry;
	Identifier catalog;
	LogicalDependencyType dependency_type;

public:
	explicit LogicalDependency(CatalogEntry &entry,
	                           LogicalDependencyType dependency_type = LogicalDependencyType::BLOCKING);
	LogicalDependency();
	LogicalDependency(optional_ptr<Catalog> catalog, CatalogEntryInfo entry, Identifier catalog_str,
	                  LogicalDependencyType dependency_type);
	bool operator==(const LogicalDependency &other) const;

public:
	void Serialize(Serializer &serializer) const;
	static LogicalDependency Deserialize(Deserializer &deserializer);
};

struct LogicalDependencyHashFunction {
	uint64_t operator()(const LogicalDependency &a) const;
};

struct LogicalDependencyEquality {
	bool operator()(const LogicalDependency &a, const LogicalDependency &b) const;
};

//! The LogicalDependencyList containing LogicalDependency objects, not looked up in the catalog yet
class LogicalDependencyList {
	using create_info_set_t =
	    unordered_set<LogicalDependency, LogicalDependencyHashFunction, LogicalDependencyEquality>;

public:
	DUCKDB_API void AddDependency(CatalogEntry &entry);
	DUCKDB_API void AddDependency(const LogicalDependency &entry);
	DUCKDB_API void AddRecreationDependency(CatalogEntry &entry);
	DUCKDB_API void AddDependencies(const LogicalDependencyList &dependencies);
	DUCKDB_API bool ContainsBlockingDependency(CatalogEntry &entry);

public:
	DUCKDB_API void VerifyDependencies(Catalog &catalog, const Identifier &name);
	void Serialize(Serializer &serializer) const;
	static LogicalDependencyList Deserialize(Deserializer &deserializer);
	bool operator==(const LogicalDependencyList &other) const;
	const create_info_set_t &Set() const;
	create_info_set_t GetSetForSerialization(Serializer &serializer) const;

private:
	void AddDependencyInternal(LogicalDependency dependency);

private:
	create_info_set_t set;
};

} // namespace duckdb
