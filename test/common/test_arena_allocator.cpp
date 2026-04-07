#include "catch.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/allocator.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("ArenaAllocator Reset preserves head chunk allocation size", "[arena_allocator]") {
	Allocator &alloc = Allocator::DefaultAllocator();

	ArenaAllocator arena(alloc, 64);

	arena.Allocate(32);
	auto size_after_first = arena.AllocationSize();
	for (int i = 0; i < 10; i++) {
		arena.Allocate(size_after_first);
	}
	REQUIRE(arena.AllocationSize() > size_after_first);

	arena.Reset();
	REQUIRE(arena.SizeInBytes() == 0);

	auto head = arena.GetHead();
	REQUIRE(head != nullptr);
	REQUIRE(arena.AllocationSize() == head->maximum_size);
	REQUIRE(head->next == nullptr);
	REQUIRE(arena.GetHead() == arena.GetTail());
}
