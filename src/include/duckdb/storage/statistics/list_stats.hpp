//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/list_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/statistics/stats_merge_type.hpp"

namespace duckdb {
class BaseStatistics;
struct SelectionVector;
class Vector;
class Value;

struct ListStats {
	//! Per-index min/max for the first N list/array elements (SQL index 1 = offset 0)
	static constexpr idx_t MAX_ELEMENT_STATS = 32;

	DUCKDB_API static void Construct(BaseStatistics &stats);
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	DUCKDB_API static const BaseStatistics &GetChildStats(const BaseStatistics &stats);
	DUCKDB_API static BaseStatistics &GetChildStats(BaseStatistics &stats);
	DUCKDB_API static void SetChildStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats);

	DUCKDB_API static optional_ptr<const BaseStatistics> TryGetElementStats(const BaseStatistics &stats, idx_t index);
	DUCKDB_API static void SetElementStats(BaseStatistics &stats, idx_t index, const BaseStatistics &element_stats);
	DUCKDB_API static void UpdateElementStats(BaseStatistics &stats, const Vector &vector, idx_t count);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	DUCKDB_API static child_list_t<Value> ToStruct(const BaseStatistics &stats);

	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other,
	                             StatsMergeType merge_type = StatsMergeType::MERGE_STATS);
	DUCKDB_API static void Copy(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, const Vector &vector, const SelectionVector &sel,
	                              idx_t count);
};

} // namespace duckdb
