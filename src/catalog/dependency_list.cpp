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

	hash_t hash = duckdb::Hash(name.c_str());
	for (auto &schema : a.entry.schema_path) {
		hash = CombineHash(hash, duckdb::Hash(schema.c_str()));
	}
	hash = CombineHash(hash, duckdb::Hash(catalog.c_str()));
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
	if (a.catalog != b.catalog) {
		return false;
	}
	return true;
}

LogicalDependency::LogicalDependency()
    : entry(), catalog(), dependency_type(LogicalDependencyType::BLOCKING) {
}

LogicalDependency::LogicalDependency(CatalogEntry &entry, LogicalDependencyType dependency_type_p)
    : dependency_type(dependency_type_p) {
	catalog = Identifier::InvalidCatalog();
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyEntry>();

		this->entry = dependency_entry.EntryInfo();
	} else {
		// use the same schema path as the dependency manager (the containing schema chain) so subjects and dependents
		// resolve to the same mangled name
		this->entry.schema_path = DependencyManager::GetSchemaPath(entry);
		this->entry.name = entry.name;
		this->entry.type = entry.type;
		catalog = entry.ParentCatalog().GetName();
	}
}

LogicalDependency::LogicalDependency(optional_ptr<Catalog> catalog_p, CatalogEntryInfo entry_p, Identifier catalog_str,
                                     LogicalDependencyType dependency_type_p)
    : entry(std::move(entry_p)), catalog(std::move(catalog_str)), dependency_type(dependency_type_p) {
	if (catalog_p) {
		catalog = catalog_p->GetName();
	}
}

bool LogicalDependency::operator==(const LogicalDependency &other) const {
	return other.entry.name == entry.name && other.entry.schema_path == entry.schema_path &&
	       other.entry.type == entry.type && other.dependency_type == dependency_type;
}

void LogicalDependencyList::AddDependency(CatalogEntry &entry) {
	AddDependencyInternal(LogicalDependency(entry));
}

void LogicalDependencyList::AddDependency(const LogicalDependency &entry) {
	AddDependencyInternal(entry);
}

void LogicalDependencyList::AddRecreationDependency(CatalogEntry &entry) {
	AddDependencyInternal(LogicalDependency(entry, LogicalDependencyType::RECREATION_ONLY));
}

void LogicalDependencyList::AddDependencyInternal(LogicalDependency dependency) {
	auto existing = set.find(dependency);
	if (existing == set.end()) {
		set.insert(std::move(dependency));
		return;
	}
	if (existing->dependency_type == LogicalDependencyType::RECREATION_ONLY &&
	    dependency.dependency_type == LogicalDependencyType::BLOCKING) {
		set.erase(existing);
		set.insert(std::move(dependency));
	}
}

void LogicalDependencyList::AddDependencies(const LogicalDependencyList &dependencies) {
	for (auto &dependency : dependencies.Set()) {
		AddDependency(dependency);
	}
}

bool LogicalDependencyList::ContainsBlockingDependency(CatalogEntry &entry_p) {
	LogicalDependency logical_entry(entry_p);
	auto dependency = set.find(logical_entry);
	return dependency != set.end() && dependency->dependency_type == LogicalDependencyType::BLOCKING;
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

const LogicalDependencyList::create_info_set_t &LogicalDependencyList::Set() const {
	return set;
}

LogicalDependencyList::create_info_set_t LogicalDependencyList::GetSetForSerialization(Serializer &serializer) const {
	if (serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
		return set;
	}
	create_info_set_t result;
	for (auto &dependency : set) {
		if (dependency.dependency_type == LogicalDependencyType::BLOCKING) {
			result.insert(dependency);
		}
	}
	return result;
}

bool LogicalDependencyList::operator==(const LogicalDependencyList &other) const {
	if (set.size() != other.set.size()) {
		return false;
	}

	for (auto &entry : set) {
		auto other_entry = other.set.find(entry);
		if (other_entry == other.set.end() || other_entry->dependency_type != entry.dependency_type) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
