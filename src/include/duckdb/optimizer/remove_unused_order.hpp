//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/remove_unused_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Drops LogicalOrder nodes that sit directly below an order-insensitive parent
//! (aggregate with all NOT_ORDER_DEPENDENT aggregates, DISTINCT, or set ops).
class RemoveUnusedOrder {
public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb
