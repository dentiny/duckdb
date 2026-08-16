#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

struct ListElementStatsExtraData : public ExtraStatsData {
	vector<BaseStatistics> element_stats;
};

static unique_ptr<ListElementStatsExtraData> CopyElementStats(const ExtraStatsData &src) {
	auto &extra = src.Cast<ListElementStatsExtraData>();
	auto result = make_uniq<ListElementStatsExtraData>();
	result->element_stats.reserve(extra.element_stats.size());
	for (auto &stats : extra.element_stats) {
		result->element_stats.push_back(stats.Copy());
	}
	return result;
}

void ListStats::Construct(BaseStatistics &stats) {
	stats.child_stats = unsafe_unique_array<BaseStatistics>(new BaseStatistics[1]);
	BaseStatistics::Construct(stats.child_stats[0], ListType::GetChildType(stats.GetType()));
}

BaseStatistics ListStats::CreateUnknown(LogicalType type) {
	auto &child_type = ListType::GetChildType(type);
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	result.child_stats[0].Copy(BaseStatistics::CreateUnknown(child_type));
	return result;
}

BaseStatistics ListStats::CreateEmpty(LogicalType type) {
	auto &child_type = ListType::GetChildType(type);
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	result.child_stats[0].Copy(BaseStatistics::CreateEmpty(child_type));
	return result;
}

void ListStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	D_ASSERT(stats.child_stats);
	D_ASSERT(other.child_stats);
	stats.child_stats[0].Copy(other.child_stats[0]);
	stats.extra_data = other.extra_data ? CopyElementStats(*other.extra_data) : nullptr;
}

const BaseStatistics &ListStats::GetChildStats(const BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::LIST_STATS) {
		throw InternalException("ListStats::GetChildStats called on stats that is not a list");
	}
	D_ASSERT(stats.child_stats);
	return stats.child_stats[0];
}
BaseStatistics &ListStats::GetChildStats(BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::LIST_STATS) {
		throw InternalException("ListStats::GetChildStats called on stats that is not a list");
	}
	D_ASSERT(stats.child_stats);
	return stats.child_stats[0];
}

void ListStats::SetChildStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats) {
	if (!new_stats) {
		stats.child_stats[0].Copy(BaseStatistics::CreateUnknown(ListType::GetChildType(stats.GetType())));
	} else {
		stats.child_stats[0].Copy(*new_stats);
	}
}

optional_ptr<const BaseStatistics> ListStats::TryGetElementStats(const BaseStatistics &stats, idx_t index) {
	if (!stats.extra_data) {
		return nullptr;
	}
	auto &extra = stats.extra_data->Cast<ListElementStatsExtraData>();
	return index < extra.element_stats.size() ? &extra.element_stats[index] : nullptr;
}

void ListStats::SetElementStats(BaseStatistics &stats, idx_t index, const BaseStatistics &element_stats) {
	if (index >= MAX_ELEMENT_STATS) {
		return;
	}
	if (!stats.extra_data) {
		stats.extra_data = make_uniq<ListElementStatsExtraData>();
	}
	auto &extra = stats.extra_data->Cast<ListElementStatsExtraData>();
	while (extra.element_stats.size() <= index) {
		extra.element_stats.push_back(BaseStatistics::CreateUnknown(ListType::GetChildType(stats.GetType())));
	}
	extra.element_stats[index] = element_stats.Copy();
}

void ListStats::UpdateElementStats(BaseStatistics &stats, const Vector &vector, idx_t count) {
	auto &child_type = ListType::GetChildType(stats.GetType());
	if (BaseStatistics::GetStatsType(child_type) != StatisticsType::NUMERIC_STATS) {
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
		auto &entry = entries[idx];
		auto length = MinValue<idx_t>(entry.length, MAX_ELEMENT_STATS);
		for (idx_t k = 0; k < length; k++) {
			auto value = child.GetValue(entry.offset + k);
			auto next = value.IsNull() ? BaseStatistics::CreateUnknown(child_type) : BaseStatistics::FromConstant(value);
			if (value.IsNull()) {
				next.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
			}
			if (auto prev = TryGetElementStats(stats, k)) {
				auto merged = prev->Copy();
				merged.Merge(next);
				SetElementStats(stats, k, merged);
			} else {
				SetElementStats(stats, k, next);
			}
		}
	}
}

void ListStats::Merge(BaseStatistics &stats, const BaseStatistics &other, StatsMergeType merge_type) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}

	auto &child_stats = ListStats::GetChildStats(stats);
	auto &other_child_stats = ListStats::GetChildStats(other);
	child_stats.Merge(other_child_stats, merge_type);
	if (!other.extra_data) {
		return;
	}
	if (!stats.extra_data) {
		stats.extra_data = CopyElementStats(*other.extra_data);
		return;
	}
	auto &dst = stats.extra_data->Cast<ListElementStatsExtraData>();
	auto &src = other.extra_data->Cast<ListElementStatsExtraData>();
	if (dst.element_stats.size() < src.element_stats.size()) {
		dst.element_stats.reserve(src.element_stats.size());
	}
	for (idx_t i = 0; i < src.element_stats.size(); i++) {
		if (i >= dst.element_stats.size()) {
			dst.element_stats.push_back(src.element_stats[i].Copy());
		} else {
			dst.element_stats[i].Merge(src.element_stats[i], merge_type);
		}
	}
}

void ListStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &child_stats = ListStats::GetChildStats(stats);
	serializer.WriteProperty(200, "child_stats", child_stats);
}

void ListStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &type = base.GetType();
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto &child_type = ListType::GetChildType(type);

	// Push the logical type of the child type to the deserialization context
	deserializer.Set<const LogicalType &>(child_type);
	base.child_stats[0].Copy(deserializer.ReadProperty<BaseStatistics>(200, "child_stats"));
	deserializer.Unset<LogicalType>();
}

child_list_t<Value> ListStats::ToStruct(const BaseStatistics &stats) {
	auto &child_stats = ListStats::GetChildStats(stats);
	child_list_t<Value> result;
	result.emplace_back("child_stats", child_stats.ToStruct());
	return result;
}

void ListStats::Verify(const BaseStatistics &stats, const Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &child_stats = ListStats::GetChildStats(stats);
	const auto &child_entry = ListVector::GetChild(vector);
	auto entries = vector.Values<list_entry_t>();

	idx_t total_list_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto entry = entries[idx];
		if (entry.IsValid()) {
			total_list_count += entry.GetValue().length;
		}
	}
	SelectionVector list_sel(total_list_count);
	idx_t list_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto entry = entries[idx];
		if (entry.IsValid()) {
			auto list = entry.GetValue();
			for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
				list_sel.set_index(list_count++, list.offset + list_idx);
			}
		}
	}

	child_stats.Verify(child_entry, list_sel, list_count);
}

} // namespace duckdb
