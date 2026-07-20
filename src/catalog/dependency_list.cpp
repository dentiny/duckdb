#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {

uint64_t LogicalDependencyHashFunction::operator()(const LogicalDependency &a) const {
	auto &name = a.entry.name;
	auto &type = a.entry.type;
	auto &catalog = a.catalog;

	hash_t hash = name.Hash();
	for (auto &schema : a.entry.schema_path) {
		hash = CombineHash(hash, schema.Hash());
	}
	hash = CombineHash(hash, a.entry.parent_name.Hash());
	hash = CombineHash(hash, catalog.Hash());
	hash = CombineHash(hash, duckdb::Hash<uint8_t>(static_cast<uint8_t>(type)));
	return hash;
}

bool LogicalDependencyEquality::operator()(const LogicalDependency &a, const LogicalDependency &b) const {
	if (a.entry.type != b.entry.type) {
		return false;
	}
	if (a.entry.name != b.entry.name) {
		return false;
	}
	if (a.entry.schema_path != b.entry.schema_path) {
		return false;
	}
	if (a.entry.parent_name != b.entry.parent_name) {
		return false;
	}
	if (a.catalog != b.catalog) {
		return false;
	}
	return true;
}

LogicalDependency::LogicalDependency() : entry(), catalog() {
}

LogicalDependency::LogicalDependency(CatalogEntry &entry) {
	catalog = Identifier::InvalidCatalog();
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyEntry>();

		this->entry = dependency_entry.EntryInfo();
	} else {
		this->entry = DependencyManager::GetLookupProperties(entry);
		catalog = entry.ParentCatalog().GetName();
	}
}

LogicalDependency::LogicalDependency(optional_ptr<Catalog> catalog_p, CatalogEntryInfo entry_p, Identifier catalog_str)
    : entry(std::move(entry_p)), catalog(std::move(catalog_str)) {
	if (catalog_p) {
		catalog = catalog_p->GetName();
	}
}

bool LogicalDependency::operator==(const LogicalDependency &other) const {
	return other.entry.name == entry.name && other.entry.schema_path == entry.schema_path &&
	       other.entry.type == entry.type && other.entry.parent_name == entry.parent_name && other.catalog == catalog;
}

void LogicalDependencyList::AddDependency(CatalogEntry &entry) {
	set.insert(LogicalDependency(entry));
}

void LogicalDependencyList::AddDependency(const LogicalDependency &entry) {
	set.insert(entry);
}

void LogicalDependencyList::AddAll(const LogicalDependencyList &dependencies) {
	for (auto &dependency : dependencies.Set()) {
		AddDependency(dependency);
	}
}

bool LogicalDependencyList::Contains(CatalogEntry &entry_p) const {
	return set.count(LogicalDependency(entry_p));
}

void LogicalDependencyList::VerifyDependencies(Catalog &catalog, const Identifier &name) {
	for (auto &dep : set) {
		if (dep.catalog != catalog.GetName()) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    name, dep.entry.name, dep.catalog, catalog.GetName());
		}
	}
}

const LogicalDependencyList::dependency_set_t &LogicalDependencyList::Set() const {
	return set;
}

bool LogicalDependencyList::operator==(const LogicalDependencyList &other) const {
	if (set.size() != other.set.size()) {
		return false;
	}

	for (auto &entry : set) {
		if (!other.set.count(entry)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
