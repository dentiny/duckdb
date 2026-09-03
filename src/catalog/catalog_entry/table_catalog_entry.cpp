#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

#include <sstream>

namespace duckdb {

constexpr const char *TableCatalogEntry::Name;

TableCatalogEntry::TableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : StandardEntry(CatalogType::TABLE_ENTRY, schema, catalog, info.GetTableName()), columns(std::move(info.columns)),
      constraints(std::move(info.constraints)) {
	this->temporary = info.temporary;
	this->dependencies = info.dependencies;
	this->comment = info.comment;
	this->tags = info.tags;
}

bool TableCatalogEntry::HasGeneratedColumns() const {
	return columns.LogicalColumnCount() != columns.PhysicalColumnCount();
}

StorageIndex TableCatalogEntry::GetStorageIndex(const ColumnIndex &column_id) const {
	if (column_id.IsRowIdColumn()) {
		return StorageIndex(COLUMN_IDENTIFIER_ROW_ID);
	}
	if (column_id.IsRowNumberColumn()) {
		return StorageIndex(COLUMN_IDENTIFIER_ROW_NUMBER);
	}

	// The index of the base ColumnIndex is equal to the physical column index in the table
	// for any child indices because the indices are already the physical indices.
	// Only the top-level can have generated columns.
	auto &col = GetColumn(column_id.ToLogical());
	auto result = StorageIndex::FromColumnIndex(column_id);
	result.SetIndex(col.StorageOid());
	return result;
}

LogicalIndex TableCatalogEntry::GetColumnIndex(Identifier &column_name, bool if_exists) const {
	auto entry = columns.GetColumnIndex(column_name);
	if (!entry.IsValid()) {
		if (if_exists) {
			return entry;
		}
		vector<string> column_names;
		for (auto &col : columns.Logical()) {
			column_names.emplace_back(col.Name());
		}
		auto candidates =
		    StringUtil::CandidatesErrorMessage(column_names, column_name.GetIdentifierName(), "Did you mean");
		throw BinderException("Table %s does not have a column with name %s\n%s", name, column_name, candidates);
	}
	return entry;
}

unique_ptr<BlockingSample> TableCatalogEntry::GetSample() {
	return nullptr;
}

bool TableCatalogEntry::ColumnExists(const Identifier &name) const {
	return columns.ColumnExists(name);
}

const ColumnDefinition &TableCatalogEntry::GetColumn(const Identifier &name) const {
	return columns.GetColumn(name);
}

vector<LogicalType> TableCatalogEntry::GetTypes() const {
	vector<LogicalType> types;
	for (auto &col : columns.Physical()) {
		types.push_back(col.Type());
	}
	return types;
}

unique_ptr<CreateInfo> TableCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateTableInfo>();
	// carry the full (possibly nested) schema path: [catalog, schema_path..., name]
	result->SetQualifiedName(schema.GetQualifiedName(name));
	result->columns = columns.Copy();
	result->constraints.reserve(constraints.size());
	result->dependencies = dependencies;
	std::for_each(constraints.begin(), constraints.end(),
	              [&result](const unique_ptr<Constraint> &c) { result->constraints.emplace_back(c->Copy()); });
	result->temporary = temporary;
	result->internal = internal;
	result->comment = comment;
	result->tags = tags;
	return std::move(result);
}

string TableCatalogEntry::ColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints) {
	duckdb::stringstream ss;

	ss << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key columns
	logical_index_set_t not_null_columns;
	logical_index_set_t unique_columns;
	logical_index_set_t pk_columns;
	identifier_set_t multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = constraint->Cast<UniqueConstraint>();
			if (pk.HasIndex()) {
				// no columns specified: single column constraint
				if (pk.IsPrimaryKey()) {
					pk_columns.insert(pk.GetIndex());
				} else {
					unique_columns.insert(pk.GetIndex());
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after all columns
				if (pk.IsPrimaryKey()) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.GetColumnNames()) {
						multi_key_pks.insert(col);
					}
				}
				extra_constraints.push_back(constraint->ToString());
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << column.ToSQLString();
		bool not_null = not_null_columns.find(column.Logical()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Logical()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(Identifier(column.Name().GetIdentifierName())) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Logical()) != unique_columns.end();
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ")";
	return ss.str();
}

string TableCatalogEntry::ColumnNamesToSQL(const ColumnList &columns) {
	if (columns.empty()) {
		return "";
	}

	duckdb::stringstream ss;
	ss << "(";

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << SQLIdentifier(column.Name()) << " ";
	}
	ss << ")";
	return ss.str();
}

string TableCatalogEntry::ToSQL() const {
	auto create_info = GetInfo();
	create_info->StripCatalogQualification();
	return create_info->ToString();
}

TableFunction TableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                 const EntryLookupInfo &lookup_info) {
	return GetScanFunction(context, bind_data);
}

const ColumnList &TableCatalogEntry::GetColumns() const {
	return columns;
}

const ColumnDefinition &TableCatalogEntry::GetColumn(LogicalIndex idx) const {
	return columns.GetColumn(idx);
}

const vector<unique_ptr<Constraint>> &TableCatalogEntry::GetConstraints() const {
	return constraints;
}

// LCOV_EXCL_START
DataTable &TableCatalogEntry::GetStorage() {
	throw InternalException("Calling GetStorage on a TableCatalogEntry that is not a DuckTableEntry");
}
// LCOV_EXCL_STOP

void LogicalUpdate::BindExtraColumns(TableCatalogEntry &table, LogicalGet &get, LogicalProjection &proj,
                                     LogicalUpdate &update, physical_index_set_t &bound_columns) {
	if (bound_columns.size() <= 1) {
		return;
	}
	idx_t found_column_count = 0;
	physical_index_set_t found_columns;
	for (idx_t i = 0; i < update.columns.size(); i++) {
		if (bound_columns.find(update.columns[i]) != bound_columns.end()) {
			// this column is referenced in the CHECK constraint
			found_column_count++;
			found_columns.insert(update.columns[i]);
		}
	}
	if (found_column_count != bound_columns.size()) {
		// columns that were required are not all part of the UPDATE
		// add them to the scan and update set
		for (auto &physical_id : bound_columns) {
			if (found_columns.find(physical_id) != found_columns.end()) {
				// column is already projected
				continue;
			}
			// column is not projected yet: project it by adding the clause "i=i" to the set of updated columns
			auto &column = table.GetColumns().GetColumn(physical_id);
			auto proj_ref = make_uniq<BoundColumnRefExpression>(
			    column.Type(), ColumnBinding(get.table_index, ProjectionIndex(get.GetColumnIds().size())));
			auto proj_index = ColumnBinding::PushExpression(proj.expressions, std::move(proj_ref));
			update.expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(column.Type(), ColumnBinding(proj.table_index, proj_index)));
			get.AddColumnId(column.Logical().index);
			update.columns.push_back(physical_id);
		}
	}
}

vector<ColumnSegmentInfo> TableCatalogEntry::GetColumnSegmentInfo(const QueryContext &context,
                                                                  const ColumnSegmentInfoScanOptions &options) {
	return {};
}

void TableCatalogEntry::InitializeColumnSegmentInfoScan(ColumnSegmentInfoScanState &state) {
}

bool TableCatalogEntry::ScanColumnSegmentInfo(const QueryContext &context, ColumnSegmentInfoScanState &state,
                                              vector<ColumnSegmentInfo> &result) {
	return false;
}

void TableCatalogEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                              LogicalUpdate &update, ClientContext &context) {
	// check the constraints and indexes of the table to see if we need to project any additional columns
	// we do this for indexes with multiple columns and CHECK constraints in the UPDATE clause
	// suppose we have a constraint CHECK(i + j < 10); now we need both i and j to check the constraint
	// if we are only updating one of the two columns we add the other one to the UPDATE set
	// with a "useless" update (i.e. i=i) so we can verify that the CHECK constraint is not violated
	auto bound_constraints = binder.BindConstraints(constraints, name, columns);
	for (auto &constraint : bound_constraints) {
		if (constraint->type == ConstraintType::CHECK) {
			auto &check = constraint->Cast<BoundCheckConstraint>();
			// check constraint! check if we need to add any extra columns to the UPDATE clause
			LogicalUpdate::BindExtraColumns(*this, get, proj, update, check.bound_columns);
		}
	}
	if (update.return_chunk) {
		physical_index_set_t all_columns;
		for (auto &column : GetColumns().Physical()) {
			all_columns.insert(column.Physical());
		}
		LogicalUpdate::BindExtraColumns(*this, get, proj, update, all_columns);
	}
	// for index updates we always turn any update into an insert and a delete
	// we thus need all the columns to be available, hence we check if the update touches any index columns
	// If the returning keyword is used, we need access to the whole row in case the user requests it.
	// Therefore switch the update to a delete and insert.
	update.update_is_del_and_insert = Settings::Get<ForceUpdateToDelAndInsertSetting>(context);
	TableStorageInfo table_storage_info = GetStorageInfo(context);
	for (auto index : table_storage_info.index_info) {
		for (auto &column : update.columns) {
			if (index.column_set.find(column.index) != index.column_set.end()) {
				update.update_is_del_and_insert = true;
				break;
			}
		}
	}

	// we also convert any updates on LIST columns into delete + insert
	for (auto &col_index : update.columns) {
		auto &column = GetColumns().GetColumn(col_index);
		if (!column.Type().SupportsRegularUpdate()) {
			update.update_is_del_and_insert = true;
			break;
		}
	}

	if (update.update_is_del_and_insert) {
		// the update updates a column required by an index or requires returning the updated rows,
		// push projections for all columns
		physical_index_set_t all_columns;
		for (auto &column : GetColumns().Physical()) {
			all_columns.insert(column.Physical());
		}
		LogicalUpdate::BindExtraColumns(*this, get, proj, update, all_columns);
	}
}

optional_ptr<Constraint> TableCatalogEntry::GetPrimaryKey() const {
	for (const auto &constraint : GetConstraints()) {
		if (constraint->type == ConstraintType::UNIQUE) {
			auto &unique = constraint->Cast<UniqueConstraint>();
			if (unique.IsPrimaryKey()) {
				return &unique;
			}
		}
	}
	return nullptr;
}

bool TableCatalogEntry::HasPrimaryKey() const {
	return GetPrimaryKey() != nullptr;
}

LogicalType TableCatalogEntry::GetExpectedTypeForInsert(const ColumnDefinition &column) const {
	if (column.HasNestedDefaults()) {
		return LogicalType::INVALID;
	}
	return column.Type();
}

static child_list_t<LogicalType> GetRemappableChildren(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		return StructType::GetChildTypes(type);
	case LogicalTypeId::LIST:
		return {{"list", ListType::GetChildType(type)}};
	case LogicalTypeId::MAP:
		return {{"key", MapType::KeyType(type)}, {"value", MapType::ValueType(type)}};
	default:
		throw InternalException("Cannot get nested defaults for type %s", type);
	}
}

static optional_ptr<const ParsedExpression> FindNestedDefault(const ParsedExpression &defaults,
                                                              const Identifier &name) {
	if (defaults.GetExpressionClass() != ExpressionClass::FUNCTION) {
		throw InternalException("Nested defaults must be stored as a struct_pack expression");
	}
	auto &function = defaults.Cast<FunctionExpression>();
	if (function.FunctionName() != "struct_pack") {
		throw InternalException("Nested defaults must be stored as a struct_pack expression");
	}
	for (auto &argument : function.GetArguments()) {
		if (argument.GetName() == name) {
			return argument.GetExpression();
		}
	}
	return nullptr;
}

static unique_ptr<ParsedExpression> BuildNestedDefaults(const LogicalType &source_type, const LogicalType &target_type,
                                                        optional_ptr<const ParsedExpression> stored_defaults) {
	auto source_children = GetRemappableChildren(source_type);
	auto target_children = GetRemappableChildren(target_type);
	identifier_map_t<LogicalType> source_map;
	for (auto &child : source_children) {
		source_map.emplace(child.first, child.second);
	}

	vector<FunctionArgument> defaults;
	for (auto &target_child : target_children) {
		auto source_entry = source_map.find(target_child.first);
		auto stored_default = stored_defaults ? FindNestedDefault(*stored_defaults, target_child.first)
		                                      : optional_ptr<const ParsedExpression>();
		if (source_entry == source_map.end()) {
			auto default_value =
			    stored_default ? stored_default->Copy() : make_uniq<ConstantExpression>(Value(target_child.second));
			defaults.emplace_back(target_child.first, std::move(default_value));
			continue;
		}
		if (source_entry->second == target_child.second || !source_entry->second.IsNested() ||
		    !target_child.second.IsNested() || source_entry->second.id() != target_child.second.id()) {
			continue;
		}
		auto child_defaults = BuildNestedDefaults(source_entry->second, target_child.second, stored_default);
		if (child_defaults) {
			defaults.emplace_back(target_child.first, std::move(child_defaults));
		}
	}
	if (defaults.empty()) {
		return nullptr;
	}
	return make_uniq<FunctionExpression>("struct_pack", std::move(defaults));
}

static Value ConstructRemapMapping(const LogicalType &source_type, const LogicalType &target_type) {
	auto source_children = GetRemappableChildren(source_type);
	auto target_children = GetRemappableChildren(target_type);
	identifier_map_t<LogicalType> target_map;
	for (auto &child : target_children) {
		target_map.emplace(child.first, child.second);
	}

	child_list_t<Value> mapping;
	for (auto &source_child : source_children) {
		auto target_entry = target_map.find(source_child.first);
		if (target_entry == target_map.end()) {
			continue;
		}
		Value mapping_value(source_child.first);
		if (source_child.second.IsNested() && target_entry->second.IsNested() &&
		    source_child.second.id() == target_entry->second.id()) {
			vector<Value> nested_mapping;
			nested_mapping.emplace_back(source_child.first);
			nested_mapping.push_back(ConstructRemapMapping(source_child.second, target_entry->second));
			mapping_value = Value::TUPLE(std::move(nested_mapping));
		}
		mapping.emplace_back(source_child.first, std::move(mapping_value));
	}
	return Value::STRUCT(std::move(mapping));
}

unique_ptr<Expression> TableCatalogEntry::GetDefaultExpressionForColumn(ClientContext &context,
                                                                        const LogicalType &input_type,
                                                                        const ColumnDefinition &column,
                                                                        ColumnBinding binding,
                                                                        const Expression &constant_value) const {
	(void)constant_value;
	return ApplyNestedDefaults(context, make_uniq<BoundColumnRefExpression>(input_type, binding), column);
}

unique_ptr<Expression> TableCatalogEntry::ApplyNestedDefaults(ClientContext &context, unique_ptr<Expression> input,
                                                              const ColumnDefinition &column) {
	auto &input_type = input->GetReturnType();
	if (!column.HasNestedDefaults() || input_type == column.Type() || !input_type.IsNested() ||
	    input_type.id() != column.Type().id()) {
		return BoundCastExpression::AddCastToType(context, std::move(input), column.Type());
	}

	auto defaults = BuildNestedDefaults(input_type, column.Type(), column.NestedDefaults());
	if (!defaults) {
		return BoundCastExpression::AddCastToType(context, std::move(input), column.Type());
	}
	auto binder = Binder::CreateBinder(context);
	ConstantBinder default_binder(*binder, context, "nested DEFAULT value");
	auto bound_defaults = default_binder.Bind(defaults);

	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(input));
	children.push_back(make_uniq<BoundConstantExpression>(Value(column.Type())));
	children.push_back(make_uniq<BoundConstantExpression>(ConstructRemapMapping(input_type, column.Type())));
	children.push_back(std::move(bound_defaults));
	return RemapStructFun::GetFunction().Bind(context, std::move(children));
}

virtual_column_map_t TableCatalogEntry::GetVirtualColumns() const {
	virtual_column_map_t virtual_columns;
	virtual_columns.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::ROW_TYPE)));
	return virtual_columns;
}

vector<column_t> TableCatalogEntry::GetRowIdColumns() const {
	vector<column_t> result;
	result.push_back(COLUMN_IDENTIFIER_ROW_ID);
	return result;
}

optional_ptr<CatalogEntry> TableCatalogEntry::CreateTrigger(CatalogTransaction transaction, CreateTriggerInfo &info) {
	throw NotImplementedException("Triggers are not supported for this table type");
}

void TableCatalogEntry::ScanTriggers(CatalogTransaction transaction,
                                     const std::function<void(CatalogEntry &)> &callback) const {
	// Default: no triggers (non-DuckDB tables do not support triggers)
}

optional_ptr<CatalogEntry> TableCatalogEntry::GetTrigger(CatalogTransaction transaction, const Identifier &name) const {
	// Default: no triggers (non-DuckDB tables do not support triggers)
	return nullptr;
}

vector<const_reference<TriggerCatalogEntry>> TableCatalogEntry::GetTriggersForEvent(CatalogTransaction transaction,
                                                                                    TriggerEventType event_type,
                                                                                    TriggerForEach for_each) const {
	vector<const_reference<TriggerCatalogEntry>> result;
	// CatalogSet is backed by case_insensitive_tree_t (a map with case-insensitive comparator),
	// so ScanTriggers yields entries in alphabetical order by name
	ScanTriggers(transaction, [&](CatalogEntry &entry) {
		auto &trigger = entry.Cast<TriggerCatalogEntry>();
		if (trigger.event_type == event_type && trigger.for_each == for_each) {
			result.emplace_back(trigger);
		}
	});
	return result;
}

vector<const_reference<TriggerCatalogEntry>> TableCatalogEntry::GetTriggersForEvent(CatalogTransaction transaction,
                                                                                    TriggerTiming timing,
                                                                                    TriggerEventType event_type,
                                                                                    TriggerForEach for_each) const {
	vector<const_reference<TriggerCatalogEntry>> result;
	ScanTriggers(transaction, [&](CatalogEntry &entry) {
		auto &trigger = entry.Cast<TriggerCatalogEntry>();
		if (trigger.event_type == event_type && trigger.for_each == for_each && trigger.timing == timing) {
			result.emplace_back(trigger);
		}
	});
	return result;
}

} // namespace duckdb
