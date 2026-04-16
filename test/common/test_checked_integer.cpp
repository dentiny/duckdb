#include "catch.hpp"
#include "duckdb/common/checked_integer.hpp"

using namespace duckdb;

TEST_CASE("Checked integer increment/decrement overflow", "[checked_integer]") {
	// signed i64_t overflow on ++
	i64_t max_val(NumericLimits<int64_t>::Maximum());
	REQUIRE_THROWS_AS(++max_val, InternalException);
	REQUIRE_THROWS_AS(max_val++, InternalException);

	// signed i64_t underflow on --
	i64_t min_val(NumericLimits<int64_t>::Minimum());
	REQUIRE_THROWS_AS(--min_val, InternalException);
	REQUIRE_THROWS_AS(min_val--, InternalException);

	// unsigned u8_t overflow on ++
	u8_t u8_max(NumericLimits<uint8_t>::Maximum());
	REQUIRE_THROWS_AS(++u8_max, InternalException);
	REQUIRE_THROWS_AS(u8_max++, InternalException);

	// unsigned u8_t underflow on --
	u8_t u8_min(NumericLimits<uint8_t>::Minimum());
	REQUIRE_THROWS_AS(--u8_min, InternalException);
	REQUIRE_THROWS_AS(u8_min--, InternalException);

	// valid increment/decrement should work
	i32_t val(100);
	++val;
	REQUIRE(val == 101);
	val--;
	REQUIRE(val == 100);
}

TEST_CASE("Checked integer compound assignment overflow", "[checked_integer]") {
	// += overflow
	i64_t a(NumericLimits<int64_t>::Maximum() - 10);
	REQUIRE_THROWS_AS(a += 20, InternalException);

	// -= underflow
	i64_t b(NumericLimits<int64_t>::Minimum() + 10);
	REQUIRE_THROWS_AS(b -= 20, InternalException);

	// *= overflow
	i32_t c(100000);
	REQUIRE_THROWS_AS(c *= 100000, InternalException);

	// /= division by zero
	i64_t d(100);
	REQUIRE_THROWS_AS(d /= 0, InternalException);

	// /= INT_MIN / -1 overflow
	i64_t e(NumericLimits<int64_t>::Minimum());
	REQUIRE_THROWS_AS(e /= -1, InternalException);

	// valid operations
	u32_t u(100);
	u += 50;
	REQUIRE(u == 150u);
	u -= 25;
	REQUIRE(u == 125u);
	u *= 2;
	REQUIRE(u == 250u);
	u /= 5;
	REQUIRE(u == 50u);
}

TEST_CASE("Checked integer binary arithmetic overflow", "[checked_integer]") {
	// + overflow
	i16_t x(NumericLimits<int16_t>::Maximum() - 5);
	REQUIRE_THROWS_AS(x + 10, InternalException);

	// - underflow
	i16_t y(NumericLimits<int16_t>::Minimum() + 5);
	REQUIRE_THROWS_AS(y - 10, InternalException);

	// * overflow
	i8_t z(100);
	REQUIRE_THROWS_AS(z * 2, InternalException);

	// / division by zero
	i32_t w(42);
	REQUIRE_THROWS_AS(w / 0, InternalException);

	// valid operations return correct results
	u64_t u1(1000);
	auto u2 = u1 + 500;
	REQUIRE(u2 == 1500u);

	i64_t s1(-100);
	auto s2 = s1 - 50;
	REQUIRE(s2 == -150);
}

TEST_CASE("Checked integer comparisons", "[checked_integer]") {
	i64_t a(100);
	i64_t b(200);

	REQUIRE(a < b);
	REQUIRE(a <= b);
	REQUIRE(b > a);
	REQUIRE(b >= a);
	REQUIRE(a != b);
	REQUIRE_FALSE(a == b);

	// comparison with raw T (use GetValue() for conversion to raw type)
	REQUIRE(a.GetValue() < 150);
	REQUIRE(a.GetValue() == 100);
}
