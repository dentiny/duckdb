//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/checked_integer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include <type_traits>

namespace duckdb {

//! CheckedInteger is a templated wrapper around integer types (signed or unsigned)
//! that throws an InternalException on overflow or underflow for arithmetic operations
//! (++, --, +=, -=, *=, /=, +, -, *, /) similar to Rust's integer overflow behavior in debug mode.
template <class T>
class CheckedInteger {
	static_assert(std::is_integral<T>::value, "CheckedInteger only supports integral types");

private:
	T value;

public:
	using value_type = T;

	CheckedInteger() : value(0) {}
	CheckedInteger(T v) : value(v) {}  // implicit for seamless use as normal integer

	// explicit conversion to underlying type
	explicit operator T() const {
		return value;
	}

	T GetValue() const {
		return value;
	}

	// increment / decrement
	CheckedInteger &operator++() {
		T result;
		if (!TryAddOperator::Operation(value, T(1), result)) {
			throw InternalException("Overflow in increment of CheckedInteger");
		}
		value = result;
		return *this;
	}

	CheckedInteger operator++(int) {
		CheckedInteger tmp(*this);
		++(*this);
		return tmp;
	}

	CheckedInteger &operator--() {
		T result;
		if (!TrySubtractOperator::Operation(value, T(1), result)) {
			throw InternalException("Underflow in decrement of CheckedInteger");
		}
		value = result;
		return *this;
	}

	CheckedInteger operator--(int) {
		CheckedInteger tmp(*this);
		--(*this);
		return tmp;
	}

	// compound assignment
	CheckedInteger &operator+=(CheckedInteger rhs) {
		return operator+=(rhs.value);
	}
	CheckedInteger &operator+=(T rhs) {
		T result;
		if (!TryAddOperator::Operation(value, rhs, result)) {
			throw InternalException("Overflow in addition for CheckedInteger");
		}
		value = result;
		return *this;
	}

	CheckedInteger &operator-=(CheckedInteger rhs) {
		return operator-=(rhs.value);
	}
	CheckedInteger &operator-=(T rhs) {
		T result;
		if (!TrySubtractOperator::Operation(value, rhs, result)) {
			throw InternalException("Underflow in subtraction for CheckedInteger");
		}
		value = result;
		return *this;
	}

	CheckedInteger &operator*=(CheckedInteger rhs) {
		return operator*=(rhs.value);
	}
	CheckedInteger &operator*=(T rhs) {
		T result;
		if (!TryMultiplyOperator::Operation(value, rhs, result)) {
			throw InternalException("Overflow in multiplication for CheckedInteger");
		}
		value = result;
		return *this;
	}

	CheckedInteger &operator/=(CheckedInteger rhs) {
		return operator/=(rhs.value);
	}
	CheckedInteger &operator/=(T rhs) {
		if (rhs == 0) {
			throw InternalException("Division by zero in CheckedInteger");
		}
		if (NumericLimits<T>::IsSigned() && value == NumericLimits<T>::Minimum() && rhs == T(-1)) {
			throw InternalException("Overflow in division for CheckedInteger");
		}
		value /= rhs;
		return *this;
	}

	// binary arithmetic (return new CheckedInteger)
	CheckedInteger operator+(CheckedInteger rhs) const {
		return operator+(rhs.value);
	}
	CheckedInteger operator+(T rhs) const {
		T result;
		if (!TryAddOperator::Operation(value, rhs, result)) {
			throw InternalException("Overflow in addition for CheckedInteger");
		}
		return CheckedInteger(result);
	}

	CheckedInteger operator-(CheckedInteger rhs) const {
		return operator-(rhs.value);
	}
	CheckedInteger operator-(T rhs) const {
		T result;
		if (!TrySubtractOperator::Operation(value, rhs, result)) {
			throw InternalException("Underflow in subtraction for CheckedInteger");
		}
		return CheckedInteger(result);
	}

	CheckedInteger operator*(CheckedInteger rhs) const {
		return operator*(rhs.value);
	}
	CheckedInteger operator*(T rhs) const {
		T result;
		if (!TryMultiplyOperator::Operation(value, rhs, result)) {
			throw InternalException("Overflow in multiplication for CheckedInteger");
		}
		return CheckedInteger(result);
	}

	CheckedInteger operator/(CheckedInteger rhs) const {
		return operator/(rhs.value);
	}
	CheckedInteger operator/(T rhs) const {
		if (rhs == 0) {
			throw InternalException("Division by zero in CheckedInteger");
		}
		if (NumericLimits<T>::IsSigned() && value == NumericLimits<T>::Minimum() && rhs == T(-1)) {
			throw InternalException("Overflow in division for CheckedInteger");
		}
		return CheckedInteger(value / rhs);
	}

	// comparisons to support usage in conditions/loops
	bool operator==(const CheckedInteger &other) const {
		return value == other.value;
	}
	bool operator!=(const CheckedInteger &other) const {
		return value != other.value;
	}
	bool operator<(const CheckedInteger &other) const {
		return value < other.value;
	}
	bool operator>(const CheckedInteger &other) const {
		return value > other.value;
	}
	bool operator<=(const CheckedInteger &other) const {
		return value <= other.value;
	}
	bool operator>=(const CheckedInteger &other) const {
		return value >= other.value;
	}

	// for symmetry, also allow T op checked via these? but for now member covers checked op T
};

// non-member operators for symmetry (T op checked)
template <class TL, class TR>
CheckedInteger<TR> operator+(TL lhs, const CheckedInteger<TR> &rhs) {
	return CheckedInteger<TR>(lhs) + rhs.GetValue();
}

template <class TL, class TR>
CheckedInteger<TR> operator-(TL lhs, const CheckedInteger<TR> &rhs) {
	return CheckedInteger<TR>(lhs) - rhs.GetValue();
}

template <class TL, class TR>
CheckedInteger<TR> operator*(TL lhs, const CheckedInteger<TR> &rhs) {
	return CheckedInteger<TR>(lhs) * rhs.GetValue();
}

template <class TL, class TR>
CheckedInteger<TR> operator/(TL lhs, const CheckedInteger<TR> &rhs) {
	return CheckedInteger<TR>(lhs) / rhs.GetValue();
}

// type aliases for templated versions (Rust-inspired short names)
using i8_t = CheckedInteger<int8_t>;
using i16_t = CheckedInteger<int16_t>;
using i32_t = CheckedInteger<int32_t>;
using i64_t = CheckedInteger<int64_t>;
using u8_t = CheckedInteger<uint8_t>;
using u16_t = CheckedInteger<uint16_t>;
using u32_t = CheckedInteger<uint32_t>;
using u64_t = CheckedInteger<uint64_t>;

} // namespace duckdb
