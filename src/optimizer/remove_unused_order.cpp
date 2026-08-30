#include "duckdb/optimizer/remove_unused_order.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

bool AnyExpressionIsVolatile(const vector<unique_ptr<Expression>> &exprs) {
	for (auto &e : exprs) {
		if (e->IsVolatile()) {
			return true;
		}
	}
	return false;
}

bool CanIgnoreChildOrder(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &agg = op.Cast<LogicalAggregate>();
		// volatile group expressions can change group assignment based on input order
		if (AnyExpressionIsVolatile(agg.groups)) {
			return false;
		}
		for (auto &e : op.expressions) {
			if (e->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				return false;
			}
			auto &aggr = e->Cast<BoundAggregateExpression>();
			if (aggr.Function().GetOrderDependent() != AggregateOrderDependent::NOT_ORDER_DEPENDENT ||
			    aggr.GetOrderBys() || aggr.IsVolatile()) {
				return false;
			}
		}
		return true;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return op.Cast<LogicalDistinct>().distinct_type == DistinctType::DISTINCT;
	default:
		return false;
	}
}

// True for operators whose row-by-row output preserves input order AND whose
// per-row work does not depend on input order (i.e. no volatile expressions).
bool IsOrderTransparent(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
		return !AnyExpressionIsVolatile(op.expressions);
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
		while (!(*cur)->children.empty() && IsOrderTransparent(**cur)) {
			cur = &(*cur)->children[0];
		}
		if ((*cur)->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
			*cur = std::move((*cur)->children[0]);
		}
	}
	return op;
}

} // namespace duckdb
