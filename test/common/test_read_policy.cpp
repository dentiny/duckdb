#include "catch.hpp"
#include "duckdb/storage/read_policy.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// Block size used by AlignedReadPolicy
constexpr idx_t BLOCK_SIZE = 2 * 1024 * 1024; // 2MiB

TEST_CASE("Test AlignedReadPolicy basic alignment", "[read_policy]") {
	AlignedReadPolicy policy;

	SECTION("Align down start location") {
		// Request at non-aligned location
		auto result = policy.CalculateRangesToRead(/*nr_bytes=*/1000, /*location=*/100,
		                                           /*file_size=*/10 * BLOCK_SIZE);

		// Should align down to 0 and return one block
		REQUIRE(result.total_location == 0);
		REQUIRE(result.total_bytes == BLOCK_SIZE);
		REQUIRE(result.ranges.size() == 1);
		REQUIRE(result.ranges[0].read_location == 0);
		REQUIRE(result.ranges[0].read_bytes == BLOCK_SIZE);
	}

	SECTION("Already aligned start location") {
		// Request at aligned location
		auto result = policy.CalculateRangesToRead(/*nr_bytes=*/1000, /*location=*/BLOCK_SIZE,
		                                           /*file_size=*/10 * BLOCK_SIZE);

		// Should stay at block boundary
		REQUIRE(result.total_location == BLOCK_SIZE);
		REQUIRE(result.total_bytes == BLOCK_SIZE);
		REQUIRE(result.ranges.size() == 1);
		REQUIRE(result.ranges[0].read_location == BLOCK_SIZE);
		REQUIRE(result.ranges[0].read_bytes == BLOCK_SIZE);
	}

	SECTION("Large request spanning multiple blocks") {
		// Request 3.5 blocks worth of data
		idx_t request_size = 3 * BLOCK_SIZE + BLOCK_SIZE / 2;
		auto result = policy.CalculateRangesToRead(/*nr_bytes=*/request_size, /*location=*/BLOCK_SIZE,
		                                           /*file_size=*/10 * BLOCK_SIZE);

		// Should start at aligned location and span 4 blocks
		REQUIRE(result.total_location == BLOCK_SIZE);
		REQUIRE(result.total_bytes == 4 * BLOCK_SIZE);
		REQUIRE(result.ranges.size() == 4);
		for (idx_t i = 0; i < 4; ++i) {
			REQUIRE(result.ranges[i].read_location == (1 + i) * BLOCK_SIZE);
			REQUIRE(result.ranges[i].read_bytes == BLOCK_SIZE);
		}
	}

	SECTION("Small request within one block") {
		// Request small amount at aligned location
		auto result = policy.CalculateRangesToRead(/*nr_bytes=*/100, /*location=*/2 * BLOCK_SIZE,
		                                           /*file_size=*/10 * BLOCK_SIZE);

		REQUIRE(result.total_location == 2 * BLOCK_SIZE);
		REQUIRE(result.total_bytes == BLOCK_SIZE);
		REQUIRE(result.ranges.size() == 1);
	}
}

TEST_CASE("Test AlignedReadPolicy file size clamping", "[read_policy]") {
	AlignedReadPolicy policy;

	SECTION("Request extends past file size") {
		idx_t file_size = 3 * BLOCK_SIZE + BLOCK_SIZE / 2;
		auto result = policy.CalculateRangesToRead(/*nr_bytes=*/BLOCK_SIZE, /*location=*/3 * BLOCK_SIZE,
		                                           /*file_size=*/file_size);

		// Should align down to 3 * BLOCK_SIZE
		REQUIRE(result.total_location == 3 * BLOCK_SIZE);
		// Should clamp to file size, not align up to 4 * BLOCK_SIZE
		REQUIRE(result.total_bytes == BLOCK_SIZE / 2);
		REQUIRE(result.ranges.size() == 1);
		REQUIRE(result.ranges[0].read_bytes == BLOCK_SIZE / 2);
	}

	SECTION("Aligned end exactly at file size") {
		idx_t file_size = 4 * BLOCK_SIZE;
		auto result = policy.CalculateRangesToRead(/*nr_bytes=*/BLOCK_SIZE, /*location=*/3 * BLOCK_SIZE,
		                                           /*file_size=*/file_size);

		REQUIRE(result.total_location == 3 * BLOCK_SIZE);
		REQUIRE(result.total_bytes == BLOCK_SIZE);
		REQUIRE(result.ranges.size() == 1);
	}

	SECTION("Request at end of file") {
		idx_t file_size = 5 * BLOCK_SIZE + 100;
		auto result = policy.CalculateRangesToRead(/*nr_bytes=*/50, /*location=*/5 * BLOCK_SIZE,
		                                           /*file_size=*/file_size);

		REQUIRE(result.total_location == 5 * BLOCK_SIZE);
		// Would align to 6 * BLOCK_SIZE, but clamped to file size
		REQUIRE(result.total_bytes == 100);
		REQUIRE(result.ranges.size() == 1);
	}
}

TEST_CASE("Test DefaultReadPolicy basic functionality", "[read_policy]") {
	DefaultReadPolicy policy;

	SECTION("Simple request") {
		auto result = policy.CalculateRangesToRead(/*nr_bytes=*/1000, /*location=*/500,
		                                           /*file_size=*/10000);

		REQUIRE(result.total_location == 500);
		REQUIRE(result.total_bytes == 1000);
		REQUIRE(result.ranges.size() == 1);
		REQUIRE(result.ranges[0].read_location == 500);
		REQUIRE(result.ranges[0].read_bytes == 1000);
	}

	SECTION("Request extends past file size") {
		auto result = policy.CalculateRangesToRead(/*nr_bytes=*/1000, /*location=*/9500,
		                                           /*file_size=*/10000);

		REQUIRE(result.total_location == 9500);
		// Should clamp to file size
		REQUIRE(result.total_bytes == 500);
		REQUIRE(result.ranges.size() == 1);
	}
}
