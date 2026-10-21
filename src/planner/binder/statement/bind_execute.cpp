#include "duckdb/planner/binder.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

static bool IsLiteralType(const LogicalType &type) {
	return type.id() == LogicalTypeId::INTEGER_LITERAL || type.id() == LogicalTypeId::STRING_LITERAL;
}

//! Statements that are always rebound, or whose plan depends on the parameter values, can never be cached
static bool CanCacheRebind(const PreparedStatementData &prepared) {
	if (!prepared.can_cache_rebind || prepared.properties.always_require_rebind) {
		return false;
	}
	for (auto &entry : prepared.value_map) {
		if (entry.second->return_type.id() == LogicalTypeId::INVALID) {
			return false;
		}
	}
	return true;
}

//! NULL values would let the binder infer the parameter type and skip bind-time checks on the value
static bool HasNullValue(const identifier_map_t<BoundParameterData> &bind_values) {
	for (auto &entry : bind_values) {
		if (entry.second.GetValue().type().id() == LogicalTypeId::SQLNULL) {
			return true;
		}
	}
	return false;
}

//! The plan may be reused if every parameter has exactly the type of the supplied value (see RequireRebind)
static bool ParameterTypesMatch(const PreparedStatementData &prepared,
                                const identifier_map_t<BoundParameterData> &bind_values) {
	for (auto &entry : prepared.value_map) {
		auto value = bind_values.find(entry.first);
		if (value == bind_values.end() || entry.second->return_type != value->second.GetValue().type()) {
			return false;
		}
	}
	return true;
}

shared_ptr<PreparedStatementData>
Binder::PrepareForValueTypes(const Identifier &name, const PreparedStatementData &original,
                             const identifier_map_t<BoundParameterData> &bind_values) {
	Planner planner(context);
	for (auto &value : bind_values) {
		// literals have no fixed type: infer the parameter type from its context instead
		if (!IsLiteralType(value.second.return_type)) {
			planner.parameter_data.emplace(value.first, value.second);
		}
	}
	planner.keep_parameters = true;
	auto typed = planner.PrepareSQLStatement(original.unbound_statement->Copy());
	planner.plan = make_uniq<LogicalPrepare>(name, typed, std::move(planner.plan));
	planner.Optimize();
	PhysicalPlanGenerator physical_planner(context);
	physical_planner.Plan(std::move(planner.plan));
	// inferred or invalidated parameter types mean that the plan depends on the parameter values
	if (!typed->physical_plan || !ParameterTypesMatch(*typed, bind_values)) {
		return nullptr;
	}
	global_binder_state->bound_tables = planner.binder->global_binder_state->bound_tables;
	return typed;
}

BoundStatement Binder::Bind(ExecuteStatement &stmt) {
	auto parameter_count = stmt.named_param_map.size();

	// bind the prepared statement
	auto &client_data = ClientData::Get(context);

	auto entry = client_data.prepared_statements.find(stmt.name);
	if (entry == client_data.prepared_statements.end()) {
		throw BinderException("Prepared statement %s does not exist", stmt.name);
	}

	// check if we need to rebind the prepared statement
	// this happens if the catalog changes, since in this case e.g. tables we relied on may have been deleted
	auto prepared = entry->second;
	auto &named_param_map = prepared->unbound_statement->named_param_map;

	// values that were supplied pre-typed do not go through the literal binding below
	identifier_map_t<BoundParameterData> bind_values = stmt.bound_values;
	if (stmt.bound_values.empty()) {
		PreparedStatement::VerifyParameters(stmt.named_values, named_param_map, &context);
	} else {
		PreparedStatement::VerifyParameters(stmt.bound_values, named_param_map, &context);
	}

	auto &mapped_named_values = stmt.named_values;
	// bind any supplied parameters
	auto constant_binder = Binder::CreateBinder(context);
	constant_binder->SetCanContainNulls(true);
	for (auto &pair : mapped_named_values) {
		bool is_literal = pair.second->GetExpressionType() == ExpressionType::VALUE_CONSTANT;

		ConstantBinder cbinder(*constant_binder, context, "EXECUTE statement");
		auto bound_expr = cbinder.Bind(pair.second);
		BoundParameterData parameter_data;
		if (is_literal) {
			auto &constant = bound_expr->Cast<BoundConstantExpression>();
			LogicalType return_type;
			if (constant.GetReturnType() == LogicalTypeId::VARCHAR &&
			    StringType::GetCollation(constant.GetReturnType()).empty()) {
				return_type = LogicalTypeId::STRING_LITERAL;
			} else if (constant.GetReturnType().IsIntegral()) {
				return_type = LogicalType::INTEGER_LITERAL(constant.GetValue());
			} else {
				return_type = constant.GetValue().type();
			}
			parameter_data = BoundParameterData(constant.TakeValue(), std::move(return_type));
		} else {
			auto value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
			auto value_type = value.type();
			parameter_data = BoundParameterData(std::move(value), std::move(value_type));
		}
		bind_values[pair.first] = std::move(parameter_data);
	}
	prepared->PopulateMissingParameterValues(context, bind_values);
	unique_ptr<LogicalOperator> rebound_plan;

	RebindQueryInfo rebind = RebindQueryInfo::DO_NOT_REBIND;
	if (prepared->RequireRebind(context, bind_values)) {
		rebind = RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
	for (auto &state : context.registered_state->States()) {
		BindPreparedStatementCallbackInfo info {*prepared, bind_values};
		auto new_rebind = state->OnRebindPreparedStatement(context, info, rebind);
		if (new_rebind == RebindQueryInfo::ATTEMPT_TO_REBIND) {
			rebind = RebindQueryInfo::ATTEMPT_TO_REBIND;
		}
	}
	if (rebind == RebindQueryInfo::ATTEMPT_TO_REBIND) {
		// catalog was modified or statement does not have clear types: rebind the statement before running the execute
		auto original = entry->second;
		bool cached = false;
		bool type_bind_failed = false;
		if (CanCacheRebind(*original) && !HasNullValue(bind_values)) {
			try {
				auto typed = PrepareForValueTypes(stmt.name, *original, bind_values);
				if (typed) {
					cached = true;
					prepared = typed;
					entry->second = typed;
				} else {
					// keep the original plan for EXECUTEs that match its types, but don't try again
					original->can_cache_rebind = false;
				}
			} catch (std::exception &ex) {
				ErrorData error(ex);
				if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::INTERNAL ||
				    Exception::InvalidatesDatabase(error.Type())) {
					throw;
				}
				type_bind_failed = true;
			}
		}
		if (!cached) {
			Planner prepared_planner(context);
			prepared_planner.parameter_data = bind_values;
			prepared = prepared_planner.PrepareSQLStatement(original->unbound_statement->Copy());
			rebound_plan = std::move(prepared_planner.plan);
			D_ASSERT(prepared->properties.bound_all_parameters);
			global_binder_state->bound_tables = prepared_planner.binder->global_binder_state->bound_tables;
			if (type_bind_failed) {
				// binding succeeds with the values but not with their types, e.g. a function that requires a constant
				original->can_cache_rebind = false;
			}
		}
	}
	// copy the properties of the prepared statement into the planner
	auto &properties = GetStatementProperties();
	properties = prepared->properties;
	properties.parameter_count = parameter_count;

	BoundStatement result;
	result.names = prepared->names;
	result.types = prepared->types;

	prepared->Bind(context, bind_values);
	if (rebound_plan) {
		auto execute_plan = make_uniq<LogicalExecute>(std::move(prepared));
		execute_plan->children.push_back(std::move(rebound_plan));
		result.plan = std::move(execute_plan);
	} else {
		result.plan = make_uniq<LogicalExecute>(std::move(prepared));
	}
	return result;
}

} // namespace duckdb
