#include <stdint.h>
#include <string>
#include <utility>

#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "writer/list_column_writer.hpp"
#include "writer/primitive_column_writer.hpp"
#include "column_writer.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "parquet_column_schema.hpp"
#include "parquet_statistics.hpp"
#include "parquet_types.h"

namespace duckdb {

using namespace duckdb_parquet; // NOLINT

using duckdb_parquet::ConvertedType;
using duckdb_parquet::FieldRepetitionType;

static bool CanCollectElementStats(const LogicalType &type) {
	auto stats_type = BaseStatistics::GetStatsType(type);
	return stats_type == StatisticsType::NUMERIC_STATS || stats_type == StatisticsType::STRING_STATS;
}

static bool HasElementMinMax(const BaseStatistics &stats) {
	switch (stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		return NumericStats::HasMinMax(stats);
	case StatisticsType::STRING_STATS:
		return StringStats::HasMinMax(stats);
	default:
		return false;
	}
}

static void UpdateElementStats(vector<BaseStatistics> &element_stats, const LogicalType &type, idx_t index,
                               const Value &value) {
	if (index >= ListStats::MAX_ELEMENT_STATS) {
		return;
	}
	while (element_stats.size() <= index) {
		element_stats.push_back(BaseStatistics::CreateUnknown(type));
	}
	if (value.IsNull()) {
		element_stats[index].Set(StatsInfo::CAN_HAVE_NULL_VALUES);
		return;
	}
	auto value_stats = BaseStatistics::FromConstant(value);
	if (element_stats[index].CanHaveNull()) {
		value_stats.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
	}
	if (!HasElementMinMax(element_stats[index])) {
		element_stats[index] = std::move(value_stats);
	} else {
		element_stats[index].Merge(value_stats);
	}
}

void ListColumnWriter::InitializeElementStats(ListColumnWriterState &state) const {
	auto &type = Schema().type;
	if (type.id() == LogicalTypeId::LIST) {
		state.element_type = ListType::GetChildType(type);
	} else if (type.id() == LogicalTypeId::ARRAY) {
		state.element_type = ArrayType::GetChildType(type);
	} else {
		return;
	}
	state.collect_element_stats = CanCollectElementStats(state.element_type);
}

void ListColumnWriter::CollectListElementStats(ListColumnWriterState &state, Vector &vector, idx_t count) const {
	if (!state.collect_element_stats) {
		return;
	}
	UnifiedVectorFormat format;
	vector.ToUnifiedFormat(format);
	auto entries = UnifiedVectorFormat::GetData<list_entry_t>(format);
	auto &child = ListVector::GetChild(vector);
	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			continue;
		}
		auto entry = entries[idx];
		auto length = MinValue<idx_t>(entry.length, ListStats::MAX_ELEMENT_STATS);
		for (idx_t k = 0; k < length; k++) {
			UpdateElementStats(state.element_stats, state.element_type, k, child.GetValue(entry.offset + k));
		}
	}
}

void ListColumnWriter::CollectArrayElementStats(ListColumnWriterState &state, Vector &vector, idx_t count) const {
	if (!state.collect_element_stats) {
		return;
	}
	auto array_size = ArrayType::GetSize(vector.GetType());
	auto length = MinValue<idx_t>(array_size, ListStats::MAX_ELEMENT_STATS);
	UnifiedVectorFormat format;
	vector.ToUnifiedFormat(format);
	auto &child = ArrayVector::GetChild(vector);
	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			continue;
		}
		auto offset = idx * array_size;
		for (idx_t k = 0; k < length; k++) {
			UpdateElementStats(state.element_stats, state.element_type, k, child.GetValue(offset + k));
		}
	}
}

void ListColumnWriter::FinalizeElementStats(ListColumnWriterState &state) const {
	if (state.element_stats.empty()) {
		return;
	}
	auto *primitive_state = dynamic_cast<PrimitiveColumnWriterState *>(state.child_state.get());
	if (!primitive_state) {
		return;
	}
	ParquetStatisticsUtils::WriteListElementStats(state.row_group.columns[primitive_state->col_idx],
	                                               state.element_stats);
}

unique_ptr<ColumnWriterState> ListColumnWriter::InitializeWriteState(duckdb_parquet::RowGroup &row_group) {
	auto result = make_uniq<ListColumnWriterState>(row_group, row_group.columns.size());
	result->child_state = GetChildWriter().InitializeWriteState(row_group);
	InitializeElementStats(*result);
	return std::move(result);
}

bool ListColumnWriter::HasAnalyze() {
	return GetChildWriter().HasAnalyze();
}
void ListColumnWriter::Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	auto &list_child = ListVector::GetChildMutable(vector);
	auto list_count = ListVector::GetListSize(vector);
	GetChildWriter().Analyze(*state.child_state, &state_p, list_child, list_count);
}

void ListColumnWriter::FinalizeAnalyze(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	GetChildWriter().FinalizeAnalyze(*state.child_state);
}

static idx_t GetConsecutiveChildList(Vector &list, Vector &result, idx_t offset, idx_t count) {
	// returns a consecutive child list that fully flattens and repeats all required elements
	auto &validity = FlatVector::ValidityMutable(list);
	auto list_entries = FlatVector::GetData<list_entry_t>(list);
	bool is_consecutive = true;
	idx_t total_length = 0;
	for (idx_t c = offset; c < offset + count; c++) {
		if (!validity.RowIsValid(c)) {
			continue;
		}
		if (list_entries[c].offset != total_length) {
			is_consecutive = false;
		}
		total_length += list_entries[c].length;
	}
	if (is_consecutive) {
		// already consecutive - leave it as-is
		return total_length;
	}
	SelectionVector sel(total_length);
	idx_t index = 0;
	for (idx_t c = offset; c < offset + count; c++) {
		if (!validity.RowIsValid(c)) {
			continue;
		}
		for (idx_t k = 0; k < list_entries[c].length; k++) {
			sel.set_index(index++, list_entries[c].offset + k);
		}
	}
	result.Slice(sel, total_length);
	result.Flatten();
	return total_length;
}

void ListColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count,
                               bool vector_can_span_multiple_pages) {
	auto &state = state_p.Cast<ListColumnWriterState>();

	auto list_data = FlatVector::GetData<list_entry_t>(vector);
	auto &validity = FlatVector::ValidityMutable(vector);

	// write definition levels and repeats
	idx_t start = 0;
	idx_t vcount = parent ? parent->definition_levels.size() - state.parent_index : count;
	idx_t vector_index = 0;
	for (idx_t i = start; i < vcount; i++) {
		idx_t parent_index = state.parent_index + i;
		if (parent && !parent->is_empty.empty() && parent->is_empty[parent_index]) {
			state.definition_levels.push_back(parent->definition_levels[parent_index]);
			state.repetition_levels.push_back(parent->repetition_levels[parent_index]);
			state.is_empty.push_back(true);
			continue;
		}
		auto first_repeat_level =
		    parent && !parent->repetition_levels.empty() ? parent->repetition_levels[parent_index] : MaxRepeat();
		if (parent && parent->definition_levels[parent_index] != PARQUET_DEFINE_VALID) {
			state.definition_levels.push_back(parent->definition_levels[parent_index]);
			state.repetition_levels.push_back(first_repeat_level);
			state.is_empty.push_back(true);
		} else if (validity.RowIsValid(vector_index)) {
			// push the repetition levels
			if (list_data[vector_index].length == 0) {
				state.definition_levels.push_back(MaxDefine());
				state.is_empty.push_back(true);
			} else {
				state.definition_levels.push_back(PARQUET_DEFINE_VALID);
				state.is_empty.push_back(false);
			}
			state.repetition_levels.push_back(first_repeat_level);
			for (idx_t k = 1; k < list_data[vector_index].length; k++) {
				state.repetition_levels.push_back(MaxRepeat() + 1);
				state.definition_levels.push_back(PARQUET_DEFINE_VALID);
				state.is_empty.push_back(false);
			}
		} else {
			if (!can_have_nulls) {
				throw IOException("Parquet writer: map key column is not allowed to contain NULL values");
			}
			state.definition_levels.push_back(MaxDefine() - 1);
			state.repetition_levels.push_back(first_repeat_level);
			state.is_empty.push_back(true);
		}
		vector_index++;
	}
	state.parent_index += vcount;

	auto &list_child = ListVector::GetChildMutable(vector);
	Vector child_list(Vector::Ref(list_child));
	auto child_length = GetConsecutiveChildList(vector, child_list, 0, count);
	// The elements of a single list should not span multiple Parquet pages
	// So, we force the entire vector to fit on a single page by setting "vector_can_span_multiple_pages=false"
	GetChildWriter().Prepare(*state.child_state, &state_p, child_list, child_length, false);
}

void ListColumnWriter::BeginWrite(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	GetChildWriter().BeginWrite(*state.child_state);
}

void ListColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	CollectListElementStats(state, vector, count);

	auto &list_child = ListVector::GetChildMutable(vector);
	Vector child_list(Vector::Ref(list_child));
	auto child_length = GetConsecutiveChildList(vector, child_list, 0, count);
	GetChildWriter().Write(*state.child_state, child_list, child_length);
}

void ListColumnWriter::PrepareWrite(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	GetChildWriter().PrepareWrite(*state.child_state);
}

void ListColumnWriter::FinalizeWrite(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	GetChildWriter().FinalizeWrite(*state.child_state);
	FinalizeElementStats(state);
}

ColumnWriter &ListColumnWriter::GetChildWriter() {
	D_ASSERT(child_writers.size() == 1);
	return *child_writers[0];
}

const ColumnWriter &ListColumnWriter::GetChildWriter() const {
	D_ASSERT(child_writers.size() == 1);
	return *child_writers[0];
}

idx_t ListColumnWriter::FinalizeSchema(vector<duckdb_parquet::SchemaElement> &schemas) {
	idx_t schema_idx = schemas.size();

	auto &schema = column_schema;
	schema.SetSchemaIndex(schema_idx);

	auto null_type = schema.repetition_type;
	auto &name = schema.name;
	auto &field_id = schema.field_id;
	auto &type = schema.type;

	// set up the two schema elements for the list
	// for some reason we only set the converted type in the OPTIONAL element
	// first an OPTIONAL element
	duckdb_parquet::SchemaElement optional_element;
	optional_element.repetition_type = null_type;
	optional_element.num_children = 1;
	optional_element.converted_type = (type.id() == LogicalTypeId::MAP) ? ConvertedType::MAP : ConvertedType::LIST;
	optional_element.__isset.num_children = true;
	optional_element.__isset.type = false;
	optional_element.__isset.repetition_type = true;
	optional_element.__isset.converted_type = true;
	optional_element.name = name;
	if (field_id.IsValid()) {
		optional_element.__isset.field_id = true;
		optional_element.field_id = NumericCast<int32_t>(field_id.GetIndex());
	}
	schemas.push_back(std::move(optional_element));

	if (type.id() != LogicalTypeId::MAP) {
		duckdb_parquet::SchemaElement repeated_element;
		repeated_element.repetition_type = FieldRepetitionType::REPEATED;
		repeated_element.__isset.num_children = true;
		repeated_element.__isset.type = false;
		repeated_element.__isset.repetition_type = true;
		repeated_element.num_children = 1;
		repeated_element.name = "list";
		schemas.push_back(std::move(repeated_element));
	} else {
		//! When we're describing a MAP, we skip the dummy "list" element
		//! Instead, the "key_value" struct will be marked as REPEATED
		D_ASSERT(GetChildWriter().Schema().repetition_type == FieldRepetitionType::REPEATED);
	}
	return GetChildWriter().FinalizeSchema(schemas);
}

} // namespace duckdb
