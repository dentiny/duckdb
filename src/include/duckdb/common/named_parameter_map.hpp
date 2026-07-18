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
#include "duckdb/common/type_util.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

enum class NamedParameterCastPolicy : uint8_t { EXACT_TYPE, IMPLICIT_CAST };

class NamedParameterType {
public:
	NamedParameterType() = default;
	NamedParameterType(LogicalType type_p) : type(std::move(type_p)) { // NOLINT: preserve LogicalType assignment
	}
	NamedParameterType &operator=(LogicalType type_p) {
		type = std::move(type_p);
		cast_policy = NamedParameterCastPolicy::EXACT_TYPE;
		return *this;
	}

	static NamedParameterType Castable(LogicalType type) {
		return NamedParameterType(std::move(type), NamedParameterCastPolicy::IMPLICIT_CAST);
	}

	const LogicalType &GetType() const {
		return type;
	}
	NamedParameterCastPolicy GetCastPolicy() const {
		return cast_policy;
	}

private:
	NamedParameterType(LogicalType type_p, NamedParameterCastPolicy cast_policy_p)
	    : type(std::move(type_p)), cast_policy(cast_policy_p) {
	}

	LogicalType type;
	NamedParameterCastPolicy cast_policy = NamedParameterCastPolicy::EXACT_TYPE;
};

struct CTableInternalBindInfo;

class BoundNamedParameterValue {
public:
	BoundNamedParameterValue(Value value_p, LogicalType declared_type, Identifier function_name_p,
	                         Identifier option_name_p)
	    : value(std::move(value_p)),
	      bound_type(declared_type.id() == LogicalTypeId::ANY ? value.type() : std::move(declared_type)),
	      function_name(std::move(function_name_p)), option_name(std::move(option_name_p)) {
		if (value.type() != bound_type) {
			throw InternalException("Named parameter \"%s\" for table function \"%s\" was bound as %s, but has type %s",
			                        option_name, function_name, bound_type, value.type());
		}
	}

	bool IsNull() const {
		return value.IsNull();
	}
	const LogicalType &GetType() const {
		return bound_type;
	}

	template <class T>
	T GetValue() const {
		if (IsNull()) {
			throw InvalidInputException("Named parameter \"%s\" for table function \"%s\" cannot be NULL",
			                            option_name, function_name);
		}
		VerifyType<T>();
		return GetValueInternal<T>();
	}

	template <class T>
	optional<T> GetOptionalValue() const {
		if (IsNull()) {
			return nullopt;
		}
		VerifyType<T>();
		return GetValueInternal<T>();
	}

private:
	template <class T>
	void VerifyType() const {
		using TYPE = typename std::remove_cv<T>::type;
		if constexpr (std::is_same<TYPE, Value>()) {
			return;
		} else if constexpr (std::is_same<TYPE, string>()) {
			if (bound_type.InternalType() == PhysicalType::VARCHAR) {
				return;
			}
		} else if (StorageTypeCompatible<TYPE>(bound_type.InternalType())) {
			return;
		}
		throw InternalException("Named parameter \"%s\" for table function \"%s\" has type %s, but the callback "
		                        "requested an incompatible C++ type",
		                        option_name, function_name, bound_type);
	}

	template <class T>
	T GetValueInternal() const {
		using TYPE = typename std::remove_cv<T>::type;
		if constexpr (std::is_same<TYPE, Value>()) {
			return value;
		} else {
			return value.GetValueUnsafe<T>();
		}
	}

	const Value &GetRawValueForBridge() const {
		return value;
	}

	friend struct CTableInternalBindInfo;
	Value value;
	LogicalType bound_type;
	Identifier function_name;
	Identifier option_name;
};

using named_parameter_type_map_t = identifier_map_t<NamedParameterType>;
using named_parameter_map_t = identifier_map_t<Value>;
using bound_named_parameter_map_t = identifier_map_t<BoundNamedParameterValue>;

} // namespace duckdb
