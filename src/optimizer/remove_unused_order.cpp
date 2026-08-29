#include "duckdb/optimizer/remove_unused_order.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

namespace {

bool CanIgnoreChildOrder(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		for (auto &e : op.expressions) {
			if (e->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				return false;
			}
			auto &aggr = e->Cast<BoundAggregateExpression>();
			if (aggr.Function().GetOrderDependent() != AggregateOrderDependent::NOT_ORDER_DEPENDENT ||
			    aggr.GetOrderBys()) {
				return false;
			}
		}
		return true;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return op.Cast<LogicalDistinct>().distinct_type == DistinctType::DISTINCT;
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return true;
	default:
		return false;
	}
}

} // namespace

unique_ptr<LogicalOperator> RemoveUnusedOrder::Optimize(unique_ptr<LogicalOperator> op) {
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	if (!CanIgnoreChildOrder(*op)) {
		return op;
	}
	for (auto &child : op->children) {
		// walk past order-preserving passthrough operators
		auto *cur = &child;
		while (!(*cur)->children.empty() &&
		       ((*cur)->type == LogicalOperatorType::LOGICAL_PROJECTION ||
		        (*cur)->type == LogicalOperatorType::LOGICAL_FILTER)) {
			cur = &(*cur)->children[0];
		}
		if ((*cur)->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
			*cur = std::move((*cur)->children[0]);
		}
	}
	return op;
}

} // namespace duckdb
