#include "duckdb/common/assert.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/arg_properties.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

namespace {

struct MonotonicExpressionTransform {
	unique_ptr<Expression> expression;
	ColumnBinding input_binding;
	LogicalType input_type;
};

struct ValueComparator {
	virtual ~ValueComparator() = default;
	virtual bool Compare(Value &lhs, Value &rhs) const = 0;
	virtual Value GetVal(BaseStatistics &stats) const = 0;
};

struct MonotonicEndpointInfo {
	ColumnBinding binding;
	LogicalType input_type;
	LogicalType result_type;
	StorageIndex storage_index;
	vector<MonotonicExpressionTransform> transforms;
	unique_ptr<ValueComparator> comparator;
};

enum class AggregateStatsPlanType : uint8_t { ROW_COUNT, MONOTONIC_ENDPOINT };

struct AggregateStatsPlan {
	explicit AggregateStatsPlan(AggregateStatsPlanType type_p) : type(type_p) {
	}

	AggregateStatsPlanType type;
	unique_ptr<MonotonicEndpointInfo> endpoint;
};

template <typename StatsType>
struct MinValueComp : public ValueComparator {
	bool Compare(Value &lhs, Value &rhs) const override {
		return lhs < rhs;
	}
	Value GetVal(BaseStatistics &stats) const override {
		return StatsType::Min(stats);
	}
};

template <typename StatsType>
struct MaxValueComp : public ValueComparator {
	bool Compare(Value &lhs, Value &rhs) const override {
		return lhs > rhs;
	}
	Value GetVal(BaseStatistics &stats) const override {
		return StatsType::Max(stats);
	}
};

template <typename StatsType>
unique_ptr<ValueComparator> GetComparator(const Identifier &fun_name) {
	if (fun_name == "min") {
		return make_uniq<MinValueComp<StatsType>>();
	}
	D_ASSERT(fun_name == "max");
	return make_uniq<MaxValueComp<StatsType>>();
}

unique_ptr<ValueComparator> GetComparator(const Identifier &fun_name, const LogicalType &type) {
	if (type == LogicalType::VARCHAR) {
		return GetComparator<StringStats>(fun_name);
	} else if (type.IsNumeric() || type.IsTemporal()) {
		return GetComparator<NumericStats>(fun_name);
	}
	return nullptr;
}

bool IsSafeMinMaxCast(const LogicalType &source, const LogicalType &target) {
	if (source == target) {
		return true;
	}
	if (!source.IsIntegral() || !target.IsIntegral()) {
		return false;
	}
	LogicalType max_type;
	return LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(source, target, max_type) && max_type == target;
}

struct MonotonicExpressionInfo {
	ColumnBinding binding;
	LogicalType input_type;
};

bool TryGetMonotonicExpressionInfo(const Expression &expr, MonotonicExpressionInfo &info) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		const auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		info.binding = col_ref.Binding();
		info.input_type = col_ref.GetReturnType();
		return true;
	}
	if (BoundCastExpression::IsCast(expr)) {
		const auto &cast = expr.Cast<BoundFunctionExpression>();
		const auto &cast_child = BoundCastExpression::Child(cast);
		if (!IsSafeMinMaxCast(cast_child.GetReturnType(), BoundCastExpression::TargetType(cast))) {
			return false;
		}
		return TryGetMonotonicExpressionInfo(cast_child, info);
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	const auto &func = expr.Cast<BoundFunctionExpression>();
	if (!func.Function().HasArgProperties() || func.Function().GetStability() != FunctionStability::CONSISTENT) {
		return false;
	}

	bool found_input = false;
	for (idx_t i = 0; i < func.GetChildren().size(); i++) {
		const auto &child = *func.GetChildren()[i];
		if (child.IsFoldable()) {
			continue;
		}
		if (found_input) {
			// Multiple varying arguments can have extrema originating from different rows.
			return false;
		}
		MonotonicExpressionInfo child_info;
		if (!TryGetMonotonicExpressionInfo(child, child_info)) {
			return false;
		}
		const auto monotonicity = func.Function().GetArgProperties(i).monotonicity;
		if (!IsMonotonicIncreasing(monotonicity) && !IsMonotonicDecreasing(monotonicity)) {
			return false;
		}
		info = std::move(child_info);
		found_input = true;
	}
	return found_input;
}

bool TryGetMonotonicEndpointInfo(const Expression &expr, MonotonicEndpointInfo &info) {
	MonotonicExpressionInfo expression_info;
	if (!TryGetMonotonicExpressionInfo(expr, expression_info)) {
		return false;
	}
	info.binding = expression_info.binding;
	info.input_type = expression_info.input_type;
	info.result_type = expr.GetReturnType();
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		MonotonicExpressionTransform transform;
		transform.expression = expr.Copy();
		transform.input_binding = expression_info.binding;
		transform.input_type = expression_info.input_type;
		info.transforms.push_back(std::move(transform));
	}
	return true;
}

bool ReplaceInputWithConstant(unique_ptr<Expression> &expr, const ColumnBinding &binding, const Value &value) {
	if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		const auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		if (col_ref.Binding() != binding) {
			return false;
		}
		expr = make_uniq<BoundConstantExpression>(value);
		return true;
	}
	bool success = true;
	bool found_input = false;
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		if (!success || child->IsFoldable()) {
			return;
		}
		if (!ReplaceInputWithConstant(child, binding, value)) {
			success = false;
			return;
		}
		found_input = true;
	});
	return success && found_input;
}

bool IsUnusableMonotonicValue(const Value &value) {
	if (value.IsNull()) {
		return true;
	}
	switch (value.type().id()) {
	case LogicalTypeId::DOUBLE:
		return Value::IsNan(value.GetValue<double>());
	case LogicalTypeId::FLOAT:
		return Value::IsNan(value.GetValue<float>());
	default:
		return false;
	}
}

bool EvaluateMonotonicExpression(ClientContext &context, const MonotonicEndpointInfo &info, Value input,
                                 Value &result) {
	for (auto transform = info.transforms.rbegin(); transform != info.transforms.rend(); ++transform) {
		if (input.type() != transform->input_type) {
			auto cast = input.DefaultTryCastAs(transform->input_type);
			if (!cast) {
				return false;
			}
			input = std::move(*cast);
		}
		auto expression = transform->expression->Copy();
		if (!ReplaceInputWithConstant(expression, transform->input_binding, input) ||
		    !ExpressionExecutor::TryEvaluateScalar(context, *expression, input) || IsUnusableMonotonicValue(input)) {
			return false;
		}
	}
	if (input.type() != info.result_type) {
		auto cast = input.DefaultTryCastAs(info.result_type);
		if (!cast) {
			return false;
		}
		input = std::move(*cast);
	}
	if (!info.transforms.empty() && IsUnusableMonotonicValue(input)) {
		return false;
	}
	result = std::move(input);
	return true;
}

bool TryGetValueFromStats(const PartitionStatistics &stats, const StorageIndex &storage_index,
                          const ValueComparator &comparator, const LogicalType &result_type, Value &result) {
	if (!stats.partition_row_group) {
		return false;
	}
	auto column_stats = stats.partition_row_group->GetColumnStatistics(storage_index);
	if (!column_stats) {
		return false;
	}
	if (!stats.partition_row_group->MinMaxIsExact(storage_index) || stats.partition_row_group->HasPendingWrites()) {
		return false;
	}
	if (column_stats->GetStatsType() == StatisticsType::NUMERIC_STATS) {
		if (!NumericStats::HasMinMax(*column_stats)) {
			// TODO: This also returns if an entire row group is null. In that case, we could skip/compare null
			return false;
		}
	} else {
		D_ASSERT(column_stats->GetStatsType() == StatisticsType::STRING_STATS);
		if (!StringStats::HasMinMax(*column_stats)) {
			return false;
		}
		if (StringStats::GetMinType(*column_stats) != StringStatsType::EXACT_STATS ||
		    StringStats::GetMaxType(*column_stats) != StringStatsType::EXACT_STATS) {
			// string stats are not exact - we cannot use the value from the stats
			return false;
		}
	}
	result = comparator.GetVal(*column_stats);
	if (result.type() == result_type) {
		return true;
	}
	auto cast = result.DefaultTryCastAs(result_type);
	if (!cast) {
		return false;
	}
	result = std::move(*cast);
	return true;
}

bool TryExecuteRowCount(const vector<PartitionStatistics> &partition_stats, Value &result) {
	idx_t count = 0;
	for (const auto &stats : partition_stats) {
		if (stats.count_type == CountType::COUNT_APPROXIMATE) {
			return false;
		}
		count += stats.count;
	}
	result = Value::BIGINT(NumericCast<int64_t>(count));
	return true;
}

bool TryExecuteMonotonicEndpoint(ClientContext &context, const vector<PartitionStatistics> &partition_stats,
                                 const MonotonicEndpointInfo &info, Value &result) {
	auto min_comparator = GetComparator(Identifier("min"), info.input_type);
	auto max_comparator = GetComparator(Identifier("max"), info.input_type);
	if (!min_comparator || !max_comparator) {
		return false;
	}

	auto get_partition_result = [&](const PartitionStatistics &stats, Value &partition_result) {
		Value input_min;
		Value input_max;
		if (!TryGetValueFromStats(stats, info.storage_index, *min_comparator, info.input_type, input_min) ||
		    !TryGetValueFromStats(stats, info.storage_index, *max_comparator, info.input_type, input_max)) {
			return false;
		}
		Value output_min;
		Value output_max;
		if (!EvaluateMonotonicExpression(context, info, std::move(input_min), output_min) ||
		    !EvaluateMonotonicExpression(context, info, std::move(input_max), output_max)) {
			return false;
		}
		partition_result = std::move(output_min);
		if (!info.comparator->Compare(partition_result, output_max)) {
			partition_result = std::move(output_max);
		}
		return true;
	};

	if (!get_partition_result(partition_stats[0], result)) {
		return false;
	}
	for (idx_t partition_idx = 1; partition_idx < partition_stats.size(); partition_idx++) {
		Value partition_result;
		if (!get_partition_result(partition_stats[partition_idx], partition_result)) {
			return false;
		}
		if (!info.comparator->Compare(result, partition_result)) {
			result = std::move(partition_result);
		}
	}
	return true;
}

bool TryExecuteAggregateStatsPlan(ClientContext &context, const vector<PartitionStatistics> &partition_stats,
                                  const AggregateStatsPlan &plan, Value &result) {
	switch (plan.type) {
	case AggregateStatsPlanType::ROW_COUNT:
		return TryExecuteRowCount(partition_stats, result);
	case AggregateStatsPlanType::MONOTONIC_ENDPOINT:
		D_ASSERT(plan.endpoint);
		return TryExecuteMonotonicEndpoint(context, partition_stats, *plan.endpoint, result);
	default:
		throw InternalException("Unsupported aggregate statistics plan");
	}
}

bool GroupingSetCanIntroduceNull(const LogicalAggregate &aggr, idx_t group_idx) {
	if (aggr.grouping_sets.empty()) {
		return false;
	}
	const auto projection_idx = ProjectionIndex(group_idx);
	for (const auto &grouping_set : aggr.grouping_sets) {
		if (grouping_set.find(projection_idx) == grouping_set.end()) {
			return true;
		}
	}
	return false;
}

} // namespace

void StatisticsPropagator::TryExecuteAggregates(LogicalAggregate &aggr, unique_ptr<LogicalOperator> &node_ptr) {
	if (!aggr.groups.empty()) {
		// not possible with groups
		return;
	}
	// Build a statistics execution strategy for every aggregate.
	vector<AggregateStatsPlan> aggregate_plans;
	aggregate_plans.reserve(aggr.expressions.size());

	for (auto &aggr_ref : aggr.expressions) {
		if (aggr_ref->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			// not an aggregate
			return;
		}
		auto &aggr_expr = aggr_ref->Cast<BoundAggregateExpression>();
		if (aggr_expr.GetFilter()) {
			// aggregate has a filter - bail
			return;
		}
		if (aggr_expr.StateExportMode() == AggregateStateExportMode::STATE_EXPORT) {
			// aggregate is in state export mode - cannot replace with a constant
			return;
		}
		auto &fun_name = aggr_expr.Function().GetName();
		if (fun_name == "min" || fun_name == "max") {
			if (aggr_expr.GetChildren().size() != 1) {
				return;
			}
			auto endpoint = make_uniq<MonotonicEndpointInfo>();
			if (!TryGetMonotonicEndpointInfo(*aggr_expr.GetChildren()[0], *endpoint)) {
				return;
			}
			endpoint->comparator = GetComparator(fun_name, endpoint->result_type);
			if (!endpoint->comparator) {
				// Type has no min max statistics
				return;
			}
			AggregateStatsPlan plan(AggregateStatsPlanType::MONOTONIC_ENDPOINT);
			plan.endpoint = std::move(endpoint);
			aggregate_plans.push_back(std::move(plan));
		} else if (fun_name == "count_star") {
			aggregate_plans.emplace_back(AggregateStatsPlanType::ROW_COUNT);
		} else {
			// No statistics execution strategy exists for this aggregate.
			return;
		}
	}

	// skip any projections
	reference<LogicalOperator> child_ref = *aggr.children[0];
	while (child_ref.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		for (auto &plan : aggregate_plans) {
			if (plan.type != AggregateStatsPlanType::MONOTONIC_ENDPOINT) {
				continue;
			}
			auto &endpoint = *plan.endpoint;
			auto &proj = child_ref.get().Cast<LogicalProjection>();
			auto &expr = proj.GetExpression(endpoint.binding);
			MonotonicEndpointInfo projection_info;
			if (!TryGetMonotonicEndpointInfo(expr, projection_info)) {
				return;
			}
			if (!IsSafeMinMaxCast(projection_info.result_type, endpoint.input_type)) {
				return;
			}
			endpoint.binding = projection_info.binding;
			endpoint.input_type = projection_info.input_type;
			for (auto &transform : projection_info.transforms) {
				endpoint.transforms.push_back(std::move(transform));
			}
		}
		child_ref = *child_ref.get().children[0];
	}

	if (child_ref.get().type != LogicalOperatorType::LOGICAL_GET) {
		// child must be a LOGICAL_GET
		return;
	}
	auto &get = child_ref.get().Cast<LogicalGet>();
	if (!get.function.get_partition_stats) {
		// GET does not support getting the partition stats
		return;
	}
	if (get.extra_info.sample_options) {
		// only use row group statistics if we query the whole table
		return;
	}

	// we can do the rewrite! get the stats
	GetPartitionStatsInput input(get.function, get.bind_data.get());
	auto partition_stats = get.function.get_partition_stats(context, input);
	if (partition_stats.empty()) {
		// no partition stats found
		return;
	}

	for (auto &plan : aggregate_plans) {
		if (plan.type != AggregateStatsPlanType::MONOTONIC_ENDPOINT) {
			continue;
		}
		auto &endpoint = *plan.endpoint;
		auto &column_index = get.GetColumnIndex(endpoint.binding);
		if (!get.TryGetStorageIndex(column_index, endpoint.storage_index)) {
			//! Can't get a storage index for this column, so it doesn't have stats we can use
			//! This happens when we're dealing with a generated column for example
			return;
		}
	}

	vector<LogicalType> types;
	vector<unique_ptr<Expression>> agg_results;
	bool need_to_scan = false;
	vector<idx_t> scan_partition_indices;
	// we can keep execute eager aggregate if all partitions could be either filtered entirely or remained entirely
	if (get.table_filters.HasFilters()) {
		map<StorageIndex, reference<TableFilter>> filter_storage_index_map;
		for (auto &entry : get.table_filters) {
			auto filter_idx = entry.GetIndex();
			auto &filter = entry.Filter();
			auto &column_index = get.GetColumnIndex(filter_idx);
			StorageIndex storage_index;
			if (!get.TryGetStorageIndex(column_index, storage_index)) {
				return;
			}
			filter_storage_index_map.emplace(storage_index, filter);
		}
		vector<PartitionStatistics> precomputed_partition_stats;
		for (idx_t partition_idx = 0; partition_idx < partition_stats.size(); partition_idx++) {
			auto &stats = partition_stats[partition_idx];
			if (!stats.partition_row_group) {
				return;
			}
			auto filter_result = FilterPropagateResult::FILTER_ALWAYS_TRUE;
			for (auto &entry : filter_storage_index_map) {
				auto &storage_index = entry.first;
				auto &filter = entry.second;
				auto prg = stats.partition_row_group;
				if (!prg) {
					return;
				}
				auto column_stats = prg->GetColumnStatistics(storage_index);
				if (!column_stats) {
					return;
				}
				if (!prg->MinMaxIsExact(storage_index) || prg->HasPendingWrites()) {
					filter_result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
					break;
				}
				auto &expr_filter =
				    ExpressionFilter::GetExpressionFilter(filter.get(), "AggregateStats::CheckPartitionFilters");
				auto col_filter_result = expr_filter.CheckStatistics(context, *column_stats);
				if (col_filter_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
					// all data in this partition is filtered out, remove this partition entirely
					filter_result = FilterPropagateResult::FILTER_ALWAYS_FALSE;
					break;
				}
				if (col_filter_result != FilterPropagateResult::FILTER_ALWAYS_TRUE) {
					filter_result = col_filter_result;
				}
			}
			switch (filter_result) {
			case FilterPropagateResult::FILTER_ALWAYS_TRUE:
				precomputed_partition_stats.push_back(std::move(stats));
				break;
			case FilterPropagateResult::FILTER_ALWAYS_FALSE:
				break;
			default:
				need_to_scan = true;
				scan_partition_indices.push_back(partition_idx);
				break;
			}
		}
		if (precomputed_partition_stats.empty()) {
			// no partitions can be pre-computed
			return;
		}
		partition_stats = std::move(precomputed_partition_stats);
	}

	if (partition_stats.empty()) {
		// no partitions can be pre-computed
		return;
	}

	for (const auto &plan : aggregate_plans) {
		Value result;
		if (!TryExecuteAggregateStatsPlan(context, partition_stats, plan, result)) {
			return;
		}
		types.push_back(result.type());
		agg_results.push_back(make_uniq<BoundConstantExpression>(std::move(result)));
	}

	if (need_to_scan) {
		// Partial precomputation combines plan-time partition statistics with an execution-time scan that
		// skips partitions by their index in the row-group list. That list can change in between
		// (concurrent appends, checkpoints), in which case a skipped partition is scanned again and its
		// rows are counted twice. Only the full precomputation (no scan) is safe.
		return;
	}
	if (need_to_scan) {
		// Partial precomputation: some partitions need scanning
		// Insert a LogicalProjection above the aggregate that combines pre-computed constants with scan results
		if (!get.function.set_partitions_to_scan) {
			// scan does not support partition filtering - bail out
			return;
		}

		// Build projection expressions that merge pre-computed values with aggregate results
		auto proj_index = optimizer.binder.GenerateTableIndex();
		vector<unique_ptr<Expression>> proj_expressions;
		for (idx_t i = 0; i < aggr.expressions.size(); i++) {
			auto &aggr_expr = aggr.expressions[i]->Cast<BoundAggregateExpression>();
			auto &fun_name = aggr_expr.Function().GetName();

			// Reference to the aggregate output column
			auto agg_col_ref = make_uniq<BoundColumnRefExpression>(
			    aggr_expr.GetReturnType(), ColumnBinding(aggr.aggregate_index, ProjectionIndex(i)));

			if (fun_name == "count_star") {
				// pre_count + count_star_from_scan
				auto &pre_count_expr = agg_results[i];
				auto add_expr = optimizer.BindScalarFunction("+", pre_count_expr->Copy(), std::move(agg_col_ref));
				add_expr->SetAlias(aggr.expressions[i]->GetAlias());
				proj_expressions.push_back(std::move(add_expr));
			} else if (fun_name == "min" || fun_name == "max") {
				// For min: COALESCE(least(pre_min, agg_min), pre_min)
				// For max: COALESCE(greatest(pre_max, agg_max), pre_max)
				auto &pre_val_expr = agg_results[i];
				Identifier merge_func((fun_name == "min") ? "least" : "greatest");
				auto merged = optimizer.BindScalarFunction(merge_func, pre_val_expr->Copy(), std::move(agg_col_ref));
				auto coalesce =
				    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, aggr_expr.GetReturnType());
				coalesce->GetChildrenMutable().push_back(std::move(merged));
				coalesce->GetChildrenMutable().push_back(pre_val_expr->Copy());
				coalesce->SetAlias(aggr.expressions[i]->GetAlias());
				proj_expressions.push_back(std::move(coalesce));
			}
		}

		// Tell the scan to only scan partitions whose aggregates were NOT pre-computed
		get.SetPartitionsToScan(std::move(scan_partition_indices));

		// Create LogicalProjection above the aggregate
		auto projection = make_uniq<LogicalProjection>(proj_index, std::move(proj_expressions));
		projection->children.push_back(std::move(node_ptr));

		ColumnBindingReplacer replacer;
		for (idx_t i = 0; i < aggr.expressions.size(); i++) {
			auto old_binding = ColumnBinding(aggr.aggregate_index, ProjectionIndex(i));
			auto new_binding = ColumnBinding(proj_index, ProjectionIndex(i));
			replacer.replacement_bindings.emplace_back(old_binding, new_binding);
		}

		replacer.stop_operator = projection.get();
		node_ptr = std::move(projection);
		replacer.VisitOperator(*root);
		return;
	}

	// Set column names
	for (idx_t expr_idx = 0; expr_idx < agg_results.size(); expr_idx++) {
		agg_results[expr_idx]->SetAlias(aggr.expressions[expr_idx]->GetAlias());
	}

	vector<vector<unique_ptr<Expression>>> expressions;
	expressions.push_back(std::move(agg_results));
	auto expression_get =
	    make_uniq<LogicalExpressionGet>(aggr.aggregate_index, std::move(types), std::move(expressions));
	expression_get->children.push_back(make_uniq<LogicalDummyScan>(aggr.group_index));
	node_ptr = std::move(expression_get);
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalAggregate &aggr,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate statistics in the child node
	node_stats = PropagateStatistics(aggr.children[0]);

	// handle the groups: simply propagate statistics and assign the stats to the group binding
	aggr.group_stats.resize(aggr.groups.size());
	for (idx_t group_idx = 0; group_idx < aggr.groups.size(); group_idx++) {
		auto stats = PropagateExpression(aggr.groups[group_idx]);
		if (stats && GroupingSetCanIntroduceNull(aggr, group_idx)) {
			stats->Set(StatsInfo::CAN_HAVE_NULL_VALUES);
		}
		aggr.group_stats[group_idx] = stats ? stats->ToUnique() : nullptr;
		if (!stats) {
			continue;
		}
		ColumnBinding group_binding(aggr.group_index, ProjectionIndex(group_idx));
		statistics_map[group_binding] = std::move(stats);
	}

	// propagate statistics in the aggregates
	for (idx_t aggregate_idx = 0; aggregate_idx < aggr.expressions.size(); aggregate_idx++) {
		auto &expr = aggr.expressions[aggregate_idx];

		auto stats = PropagateExpression(expr);
		if (!stats) {
			continue;
		}
		ColumnBinding aggregate_binding(aggr.aggregate_index, ProjectionIndex(aggregate_idx));
		statistics_map[aggregate_binding] = std::move(stats);
	}

	// check whether all inputs to the aggregate functions are valid
	TupleDataValidityType distinct_validity = TupleDataValidityType::CANNOT_HAVE_NULL_VALUES;
	for (const auto &aggr_ref : aggr.expressions) {
		if (distinct_validity == TupleDataValidityType::CAN_HAVE_NULL_VALUES) {
			break;
		}
		if (aggr_ref->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			// Bail if it's not a bound aggregate
			distinct_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES;
			break;
		}
		auto &aggr_expr = aggr_ref->Cast<BoundAggregateExpression>();
		for (const auto &child : aggr_expr.GetChildren()) {
			if (child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				// Bail if bound aggregate child is not a colref
				distinct_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES;
				break;
			}
			const auto &col_ref = child->Cast<BoundColumnRefExpression>();
			auto it = statistics_map.find(col_ref.Binding());
			if (it == statistics_map.end() || !it->second || it->second->CanHaveNull()) {
				// Bail if no stats or if there can be a NULL
				distinct_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES;
				break;
			}
		}
	}
	aggr.distinct_validity = distinct_validity;

	// after we propagate statistics - try to directly execute aggregates using statistics
	TryExecuteAggregates(aggr, node_ptr);

	// the max cardinality of an aggregate is the max cardinality of the input (i.e. when every row is a unique
	// group)
	return std::move(node_stats);
}

} // namespace duckdb
