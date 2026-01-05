#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

using namespace duckdb;
using namespace std;

struct TestObject : public ObjectCacheEntry {
	int value;
	TestObject(int value) : value(value) {
	}
	~TestObject() override = default;
	string GetObjectType() override {
		return ObjectType();
	}
	static string ObjectType() {
		return "TestObject";
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}
};

struct AnotherTestObject : public ObjectCacheEntry {
	int value;
	AnotherTestObject(int value) : value(value) {
	}
	~AnotherTestObject() override = default;
	string GetObjectType() override {
		return ObjectType();
	}
	static string ObjectType() {
		return "AnotherTestObject";
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}
};

struct EvictableTestObject : public ObjectCacheEntry {
	int value;
	idx_t size;
	EvictableTestObject(int value, idx_t size) : value(value), size(size) {
	}
	~EvictableTestObject() override = default;
	string GetObjectType() override {
		return ObjectType();
	}
	static string ObjectType() {
		return "EvictableTestObject";
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(size);
	}
};

TEST_CASE("Test ObjectCache", "[api]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;

	auto &cache = ObjectCache::GetObjectCache(context);

	REQUIRE(cache.GetObject("test") == nullptr);
	cache.Put("test", make_shared_ptr<TestObject>(42));

	REQUIRE(cache.GetObject("test") != nullptr);

	cache.Delete("test");
	REQUIRE(cache.GetObject("test") == nullptr);

	REQUIRE(cache.GetOrCreate<TestObject>("test", 42) != nullptr);
	REQUIRE(cache.Get<TestObject>("test") != nullptr);
	REQUIRE(cache.GetOrCreate<TestObject>("test", 1337)->value == 42);
	REQUIRE(cache.Get<TestObject>("test")->value == 42);

	REQUIRE(cache.GetOrCreate<AnotherTestObject>("test", 13) == nullptr);
}

TEST_CASE("Test ObjectCache Memory Accounting", "[api]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &cache = ObjectCache::GetObjectCache(context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	
	idx_t initial_memory = buffer_pool.GetUsedMemory();
	
	idx_t obj_size = 1024 * 1024;
	cache.Put("evictable1", make_shared_ptr<EvictableTestObject>(1, obj_size));
	
	idx_t after_put_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_put_memory == initial_memory + obj_size);
	
	cache.Delete("evictable1");
	idx_t after_delete_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_delete_memory == initial_memory);
}

TEST_CASE("Test ObjectCache Manual Eviction", "[api]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &cache = ObjectCache::GetObjectCache(context);
	
	idx_t obj_size = 1024 * 1024;
	for (idx_t i = 0; i < 10; i++) {
		cache.Put("evictable" + to_string(i), make_shared_ptr<EvictableTestObject>(i, obj_size));
	}
	
	REQUIRE(cache.GetEntryCount() == 10);
	
	idx_t target_to_free = 5 * 1024 * 1024;
	idx_t freed = cache.EvictToReduceMemory(target_to_free);
	REQUIRE(freed >= target_to_free);
	REQUIRE(cache.GetEntryCount() <= 5);
}
