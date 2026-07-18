#include "catch.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/planner/binder.hpp"

using namespace duckdb;

TEST_CASE("Bound named parameter values provide checked access", "[named_parameter]") {
	NamedParameterType declared_type;
	declared_type = LogicalType::UBIGINT;
	REQUIRE(declared_type.GetType() == LogicalType::UBIGINT);
	REQUIRE(declared_type.GetCastPolicy() == NamedParameterCastPolicy::EXACT_TYPE);
	REQUIRE(NamedParameterType::Castable(LogicalType::UBIGINT).GetCastPolicy() ==
	        NamedParameterCastPolicy::IMPLICIT_CAST);

	BoundNamedParameterValue count(Value::UBIGINT(42), declared_type.GetType(), "repeat_row", "num_rows");
	REQUIRE(count.GetType() == LogicalType::UBIGINT);
	REQUIRE(count.GetValue<uint64_t>() == 42);
	REQUIRE_THROWS_AS(count.GetValue<double>(), InternalException);
	REQUIRE_THROWS_AS(BoundNamedParameterValue(Value("not a number"), LogicalType::UBIGINT, "repeat_row", "num_rows"),
	                  InternalException);
}

TEST_CASE("Named parameter binding enforces exact and castable declarations", "[named_parameter]") {
	QueryErrorContext error_context;
	named_parameter_type_map_t types;
	types["value"] = LogicalType::DOUBLE;

	named_parameter_map_t integer_value;
	integer_value["value"] = Value::INTEGER(1);
	try {
		Binder::BindNamedParameters(types, integer_value, error_context, "exact_double");
		FAIL("Expected exact type mismatch");
	} catch (const InvalidInputException &ex) {
		const string message = ex.what();
		REQUIRE(message.find("value") != string::npos);
		REQUIRE(message.find("exact_double") != string::npos);
		REQUIRE(message.find("DOUBLE") != string::npos);
		REQUIRE(message.find("INTEGER") != string::npos);
	}

	named_parameter_map_t decimal_value;
	decimal_value["value"] = Value::DECIMAL(int16_t(10), 2, 1);
	REQUIRE_THROWS_AS(Binder::BindNamedParameters(types, decimal_value, error_context, "exact_double"),
	                  InvalidInputException);

	named_parameter_map_t double_value;
	double_value["value"] = Value::DOUBLE(1);
	auto exact_result = Binder::BindNamedParameters(types, double_value, error_context, "exact_double");
	REQUIRE(exact_result.at("value").GetValue<double>() == 1);

	types["value"] = NamedParameterType::Castable(LogicalType::DOUBLE);
	named_parameter_map_t castable_value;
	castable_value["value"] = Value::INTEGER(1);
	auto castable_result = Binder::BindNamedParameters(types, castable_value, error_context, "castable_double");
	REQUIRE(castable_result.at("value").GetType() == LogicalType::DOUBLE);
	REQUIRE(castable_result.at("value").GetValue<double>() == 1);

	named_parameter_map_t invalid_cast;
	invalid_cast["value"] = Value("not a double");
	REQUIRE_THROWS_AS(Binder::BindNamedParameters(types, invalid_cast, error_context, "castable_double"),
	                  InvalidInputException);

	types["value"] = NamedParameterType::Castable(LogicalType::BOOLEAN);
	named_parameter_map_t castable_boolean;
	castable_boolean["value"] = Value::INTEGER(1);
	auto boolean_result = Binder::BindNamedParameters(types, castable_boolean, error_context, "castable_boolean");
	REQUIRE(boolean_result.at("value").GetValue<bool>());
}

TEST_CASE("Named parameter binding normalizes NULL and preserves ANY", "[named_parameter]") {
	QueryErrorContext error_context;
	named_parameter_type_map_t types;
	types["value"] = LogicalType::DOUBLE;

	named_parameter_map_t null_value;
	null_value["value"] = Value();
	auto null_result = Binder::BindNamedParameters(types, null_value, error_context, "concrete_null");
	REQUIRE(null_result.at("value").GetType() == LogicalType::DOUBLE);
	REQUIRE_THROWS_AS(null_result.at("value").GetValue<int64_t>(), InvalidInputException);
	REQUIRE(!null_result.at("value").GetOptionalValue<int64_t>().has_value());

	types["value"] = LogicalType::ANY;
	named_parameter_map_t any_null;
	any_null["value"] = Value();
	auto any_null_result = Binder::BindNamedParameters(types, any_null, error_context, "any_null");
	REQUIRE(any_null_result.at("value").GetType() == LogicalType::SQLNULL);
	REQUIRE_THROWS_AS(any_null_result.at("value").GetValue<int64_t>(), InvalidInputException);
	REQUIRE(!any_null_result.at("value").GetOptionalValue<int64_t>().has_value());

	named_parameter_map_t any_value;
	any_value["value"] = Value("value");
	auto any_result = Binder::BindNamedParameters(types, any_value, error_context, "any_value");
	REQUIRE(any_result.at("value").GetType() == LogicalType::VARCHAR);
	REQUIRE(any_result.at("value").GetValue<string>() == "value");
}

TEST_CASE("Bound named parameter values distinguish required and optional NULL", "[named_parameter]") {
	BoundNamedParameterValue value(Value(LogicalType::BIGINT), LogicalType::BIGINT, "test_function", "option");
	REQUIRE(value.IsNull());
	REQUIRE_THROWS_AS(value.GetValue<int64_t>(), InvalidInputException);
	REQUIRE(!value.GetOptionalValue<int64_t>().has_value());
}

TEST_CASE("Bound named parameter values preserve ANY and nested values", "[named_parameter]") {
	BoundNamedParameterValue any(Value("value"), LogicalType::ANY, "test_function", "any_option");
	REQUIRE(any.GetType() == LogicalType::VARCHAR);
	REQUIRE(any.GetValue<string>() == "value");

	auto list = Value::LIST(LogicalType::INTEGER, {Value::INTEGER(1), Value::INTEGER(2)});
	BoundNamedParameterValue nested(list, list.type(), "test_function", "nested_option");
	auto result = nested.GetValue<Value>();
	REQUIRE(result.type() == list.type());
	REQUIRE(result.ToString() == list.ToString());
}
