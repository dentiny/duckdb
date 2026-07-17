#include "catch.hpp"
#include "duckdb/common/named_parameter_map.hpp"

using namespace duckdb;

TEST_CASE("Bound named parameter values provide checked access", "[named_parameter]") {
	NamedParameterType declared_type;
	declared_type = LogicalType::UBIGINT;
	REQUIRE(declared_type.GetType() == LogicalType::UBIGINT);

	BoundNamedParameterValue count(Value::UBIGINT(42), declared_type.GetType(), "repeat_row", "num_rows");
	REQUIRE(count.GetType() == LogicalType::UBIGINT);
	REQUIRE(count.GetValue<uint64_t>() == 42);
	REQUIRE_THROWS_AS(count.GetValue<double>(), InternalException);
	REQUIRE_THROWS_AS(BoundNamedParameterValue(Value("not a number"), LogicalType::UBIGINT, "repeat_row", "num_rows"),
	                  InternalException);
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
