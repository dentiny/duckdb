//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/named_parameter_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/types/value.hpp"
namespace duckdb {

class BoundNamedParameterValue {
public:
	BoundNamedParameterValue(Value value_p, Identifier function_name_p, Identifier option_name_p)
	    : value(std::move(value_p)), function_name(std::move(function_name_p)), option_name(std::move(option_name_p)) {
	}

	bool IsNull() const {
		return value.IsNull();
	}
	const LogicalType &type() const {
		return value.type();
	}
	string ToString() const {
		ValidateNotNull();
		return value.ToString();
	}

	template <class T>
	T GetValue() const {
		ValidateNotNull();
		return value.GetValue<T>();
	}

	template <class T>
	optional<T> GetOptionalValue() const {
		if (IsNull()) {
			return nullopt;
		}
		return value.GetValue<T>();
	}

	operator const Value &() const {
		ValidateNotNull();
		return value;
	}

	const Value &GetValueOrNull() const {
		return value;
	}

private:
	void ValidateNotNull() const {
		if (IsNull()) {
			throw InvalidInputException("Named parameter \"%s\" for table function \"%s\" cannot be NULL", option_name,
			                            function_name);
		}
	}

	Value value;
	Identifier function_name;
	Identifier option_name;
};

using named_parameter_type_map_t = identifier_map_t<LogicalType>;
using named_parameter_map_t = identifier_map_t<Value>;
using bound_named_parameter_map_t = identifier_map_t<BoundNamedParameterValue>;

} // namespace duckdb
