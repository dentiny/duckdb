#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

void CreateInfo::CopyProperties(CreateInfo &other) const {
	other.type = type;
	other.SetQualifiedName(GetQualifiedName());
	other.on_conflict = on_conflict;
	other.temporary = temporary;
	other.internal = internal;
	other.extension_name = extension_name;
	other.sql = sql;
	other.dependencies = dependencies;
	other.comment = comment;
	other.tags = tags;
}

unique_ptr<AlterInfo> CreateInfo::GetAlterInfo() const {
	throw NotImplementedException("GetAlterInfo not implemented for this type");
}

string CreateInfo::QualifiedNameToString() const {
	// for temporary entries the catalog is implied, so it is omitted from the rendered name
	auto path = qualified_name.Path();
	if (temporary && path.size() >= 3) {
		path.erase(path.begin());
	}
	auto name = std::move(path.back());
	path.pop_back();
	return QualifiedName(std::move(path), std::move(name)).ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA);
}

string CreateInfo::GetCreatePrefix(const string &entry) const {
	string prefix = "CREATE";
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		prefix += " OR REPLACE";
	}
	if (temporary) {
		prefix += " TEMP";
	}
	prefix += " " + entry + " ";

	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		prefix += " IF NOT EXISTS ";
	}
	return prefix;
}

} // namespace duckdb
