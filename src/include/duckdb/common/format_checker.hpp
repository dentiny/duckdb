//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/format_checker.hpp
//
// Compile-time format string checking (following abseil's str_format pattern)
// Only enabled on Clang with the enable_if attribute
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

// Check if we can enable the format checker (Clang with enable_if attribute)
#ifndef DUCKDB_FORMAT_CHECKER
#if defined(__clang__) && __has_attribute(enable_if)
#define DUCKDB_FORMAT_CHECKER 1
#else
#define DUCKDB_FORMAT_CHECKER 0
#endif
#endif

namespace duckdb {

//===----------------------------------------------------------------------===//
// FormatConversionCharSet - Bitset of allowed format conversion characters
//===----------------------------------------------------------------------===//
enum class FormatConversionCharSet : uint64_t {
	kNone = 0,
	// Text
	s = 1ULL << 0,  // string
	// Integer
	d = 1ULL << 1,
	i = 1ULL << 2,
	o = 1ULL << 3,
	u = 1ULL << 4,
	x = 1ULL << 5,
	X = 1ULL << 6,
	// Float
	f = 1ULL << 7,
	F = 1ULL << 8,
	e = 1ULL << 9,
	E = 1ULL << 10,
	g = 1ULL << 11,
	G = 1ULL << 12,
	a = 1ULL << 13,
	A = 1ULL << 14,
	// Misc
	c = 1ULL << 15,  // char
	p = 1ULL << 16,  // pointer
	l = 1ULL << 17,  // long modifier
	// Compound sets
	kIntegral = d | i | o | u | x | X,
	kFloating = f | F | e | E | g | G | a | A,
	kNumeric = kIntegral | kFloating,
	kString = s,
	kPointer = p,
};

constexpr FormatConversionCharSet operator|(FormatConversionCharSet a, FormatConversionCharSet b) {
	return static_cast<FormatConversionCharSet>(static_cast<uint64_t>(a) | static_cast<uint64_t>(b));
}

// Map character to its FormatConversionCharSet bit
constexpr FormatConversionCharSet CharToConv(char c) {
	return (c == 's') ? FormatConversionCharSet::s :
	       (c == 'd') ? FormatConversionCharSet::d :
	       (c == 'i') ? FormatConversionCharSet::i :
	       (c == 'o') ? FormatConversionCharSet::o :
	       (c == 'u') ? FormatConversionCharSet::u :
	       (c == 'x') ? FormatConversionCharSet::x :
	       (c == 'X') ? FormatConversionCharSet::X :
	       (c == 'f') ? FormatConversionCharSet::f :
	       (c == 'F') ? FormatConversionCharSet::F :
	       (c == 'e') ? FormatConversionCharSet::e :
	       (c == 'E') ? FormatConversionCharSet::E :
	       (c == 'g') ? FormatConversionCharSet::g :
	       (c == 'G') ? FormatConversionCharSet::G :
	       (c == 'a') ? FormatConversionCharSet::a :
	       (c == 'A') ? FormatConversionCharSet::A :
	       (c == 'c') ? FormatConversionCharSet::c :
	       (c == 'p') ? FormatConversionCharSet::p :
	       FormatConversionCharSet::kNone;
}

constexpr bool Contains(FormatConversionCharSet set, char c) {
	return (static_cast<uint64_t>(set) & static_cast<uint64_t>(CharToConv(c))) != 0;
}

//===----------------------------------------------------------------------===//
// FormatConvertResult - Return type that encodes allowed format specifiers
// (Following abseil's ArgConvertResult pattern)
//===----------------------------------------------------------------------===//
template <FormatConversionCharSet C>
struct FormatConvertResult {
	static constexpr FormatConversionCharSet kConv = C;
	bool value;
};

// Helper to extract CharSet from FormatConvertResult
template <typename T>
constexpr FormatConversionCharSet ExtractCharSet(T) {
	return T::kConv;
}

//===----------------------------------------------------------------------===//
// DuckDBFormatConvert overloads - Define allowed specifiers per type
// (Following abseil's FormatConvertImpl pattern via ADL)
//===----------------------------------------------------------------------===//

// Result types for common categories
using IntegralConvertResult = FormatConvertResult<
    FormatConversionCharSet::kNumeric | FormatConversionCharSet::c>;
using FloatingConvertResult = FormatConvertResult<FormatConversionCharSet::kFloating>;
using StringConvertResult = FormatConvertResult<FormatConversionCharSet::kString>;
using PointerConvertResult = FormatConvertResult<FormatConversionCharSet::kPointer>;

// Integral types
inline IntegralConvertResult DuckDBFormatConvert(int8_t) { return {true}; }
inline IntegralConvertResult DuckDBFormatConvert(uint8_t) { return {true}; }
inline IntegralConvertResult DuckDBFormatConvert(int16_t) { return {true}; }
inline IntegralConvertResult DuckDBFormatConvert(uint16_t) { return {true}; }
inline IntegralConvertResult DuckDBFormatConvert(int32_t) { return {true}; }
inline IntegralConvertResult DuckDBFormatConvert(uint32_t) { return {true}; }
inline IntegralConvertResult DuckDBFormatConvert(int64_t) { return {true}; }
inline IntegralConvertResult DuckDBFormatConvert(uint64_t) { return {true}; }

// Floating point types
inline FloatingConvertResult DuckDBFormatConvert(float) { return {true}; }
inline FloatingConvertResult DuckDBFormatConvert(double) { return {true}; }

// String types
inline StringConvertResult DuckDBFormatConvert(const char*) { return {true}; }
inline StringConvertResult DuckDBFormatConvert(char*) { return {true}; }
inline StringConvertResult DuckDBFormatConvert(const string&) { return {true}; }

// Pointer types (for %p)
template <typename T>
inline PointerConvertResult DuckDBFormatConvert(T*) { return {true}; }

// Fallback: any type not explicitly defined gets string conversion
// This allows custom types like SQLIdentifier, LogicalType, etc. to work with %s
template <typename T>
inline StringConvertResult DuckDBFormatConvert(const T&) { return {true}; }

//===----------------------------------------------------------------------===//
// ArgumentToConv - Uses decltype to detect allowed specifiers via ADL
// (Following abseil's ArgumentToConv pattern)
//===----------------------------------------------------------------------===//
template <typename T>
constexpr FormatConversionCharSet ArgumentToConv() {
	using ConvResult = decltype(DuckDBFormatConvert(std::declval<const T&>()));
	return ExtractCharSet(ConvResult{});
}

//===----------------------------------------------------------------------===//
// Constexpr format string validation
//===----------------------------------------------------------------------===//

constexpr bool IsConversionChar(char c) {
	return c == 's' || c == 'd' || c == 'i' || c == 'o' || c == 'u' ||
	       c == 'x' || c == 'X' || c == 'f' || c == 'F' || c == 'e' ||
	       c == 'E' || c == 'g' || c == 'G' || c == 'a' || c == 'A' ||
	       c == 'c' || c == 'p' || c == 'n';
}

// Forward declarations for mutual recursion
template <FormatConversionCharSet... C>
constexpr bool ValidateFormat(const char* p, size_t arg_index, const FormatConversionCharSet* allowed, size_t num_args);

// Find the conversion character in a format specifier
constexpr const char* FindConversionChar(const char* p) {
	return (*p == '\0') ? p :
	       IsConversionChar(*p) ? p :
	       FindConversionChar(p + 1);
}

// Check argument and continue validation
template <FormatConversionCharSet... C>
constexpr bool CheckArgAndContinue(const char* p, char conv, size_t arg_index, 
                                    const FormatConversionCharSet* allowed, size_t num_args) {
	return (arg_index >= num_args) ? false :
	       (!Contains(allowed[arg_index], conv)) ? false :
	       ValidateFormat<C...>(p, arg_index + 1, allowed, num_args);
}

// Handle character after '%'
template <FormatConversionCharSet... C>
constexpr bool ValidateAfterPercent(const char* p, size_t arg_index, 
                                     const FormatConversionCharSet* allowed, size_t num_args) {
	return (*p == '\0') ? false :
	       (*p == '%') ? ValidateFormat<C...>(p + 1, arg_index, allowed, num_args) :
	       (*FindConversionChar(p) == '\0') ? false :
	       CheckArgAndContinue<C...>(FindConversionChar(p) + 1, *FindConversionChar(p), arg_index, allowed, num_args);
}

// Main validation loop
template <FormatConversionCharSet... C>
constexpr bool ValidateFormat(const char* p, size_t arg_index, 
                               const FormatConversionCharSet* allowed, size_t num_args) {
	return (*p == '\0') ? (arg_index == num_args) :
	       (*p == '%') ? ValidateAfterPercent<C...>(p + 1, arg_index, allowed, num_args) :
	       ValidateFormat<C...>(p + 1, arg_index, allowed, num_args);
}

// Entry point for format validation
template <FormatConversionCharSet... C>
constexpr bool ValidFormatImpl(const char* format) {
	return ValidateFormat<C...>(format, 0, 
	                            (const FormatConversionCharSet[]){C..., FormatConversionCharSet::kNone}, 
	                            sizeof...(C));
}

// Specialization for zero arguments
template <>
constexpr bool ValidFormatImpl<>(const char* format) {
	return ValidateFormat<>(format, 0, nullptr, 0);
}

// Helper to ensure format string is constexpr
constexpr bool EnsureConstexpr(const char*) {
	return true;
}

//===----------------------------------------------------------------------===//
// FormatSpecTemplate - The wrapper that does compile-time checking
//===----------------------------------------------------------------------===//

#if DUCKDB_FORMAT_CHECKER

template <FormatConversionCharSet... Args>
class FormatSpecTemplate {
public:
	// Honeypot: format string is not constexpr
	FormatSpecTemplate(...) // NOLINT
	    __attribute__((unavailable("Format string is not constexpr.")));

	// Honeypot: format is constexpr but invalid
	template <typename = void>
	FormatSpecTemplate(const char* s) // NOLINT
	    __attribute__((enable_if(EnsureConstexpr(s), "constexpr trap"),
	                   unavailable("Format specifier does not match the argument type.")));

	// Good: format is constexpr and valid
	FormatSpecTemplate(const char* s) // NOLINT
	    __attribute__((enable_if(ValidFormatImpl<Args...>(s), "valid format")))
	    : format_(s) {}

	const char* format() const { return format_; }

private:
	const char* format_;
};

#else  // !DUCKDB_FORMAT_CHECKER

// Fallback for non-Clang compilers: no compile-time checking
template <FormatConversionCharSet... Args>
class FormatSpecTemplate {
public:
	FormatSpecTemplate(const char* s) : format_(s) {} // NOLINT
	const char* format() const { return format_; }

private:
	const char* format_;
};

#endif  // DUCKDB_FORMAT_CHECKER

//===----------------------------------------------------------------------===//
// FormatSpec type alias - maps argument types to FormatSpecTemplate
//===----------------------------------------------------------------------===//
template <typename... Args>
using FormatSpec = FormatSpecTemplate<ArgumentToConv<Args>()...>;

} // namespace duckdb
