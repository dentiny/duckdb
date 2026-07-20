//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_set.hpp
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
class LogicalDependencySet;

//! A minimal representation of a CreateInfo / CatalogEntry
//! enough to look up the entry inside SchemaCatalogEntry::GetEntry
struct LogicalDependency {
public:
	CatalogEntryInfo entry;
	Identifier catalog;

public:
	explicit LogicalDependency(CatalogEntry &entry);
	LogicalDependency();
	LogicalDependency(optional_ptr<Catalog> catalog, CatalogEntryInfo entry, Identifier catalog_str);
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

//! A set of LogicalDependency objects that have not been looked up in the catalog yet.
class LogicalDependencySet {
	using dependency_set_t =
	    unordered_set<LogicalDependency, LogicalDependencyHashFunction, LogicalDependencyEquality>;

public:
	DUCKDB_API void Add(CatalogEntry &entry);
	DUCKDB_API void Add(const LogicalDependency &entry);
	DUCKDB_API void AddAll(const LogicalDependencySet &dependencies);
	DUCKDB_API bool Contains(CatalogEntry &entry) const;

public:
	DUCKDB_API void VerifyDependencies(Catalog &catalog, const Identifier &name);
	void Serialize(Serializer &serializer) const;
	static LogicalDependencySet Deserialize(Deserializer &deserializer);
	bool operator==(const LogicalDependencySet &other) const;
	const dependency_set_t &Entries() const;

private:
	dependency_set_t set;
};

} // namespace duckdb
