#include "catch.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/scoped_directory.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include "duckdb/storage/read_policy_registry.hpp"
#include "test_helpers.hpp"

namespace duckdb {

// Util function to create a test file with known content
string CreateTestFile(FileSystem &fs, const string &path, idx_t size_mb) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	vector<uint8_t> data(size_mb * 1024 * 1024);
	for (idx_t idx = 0; idx < data.size(); ++idx) {
		data[idx] = static_cast<uint8_t>((idx / (1024 * 1024)) % 256);
	}

	handle->Write(QueryContext(), data.data(), data.size(), /*location=*/0);
	handle->Sync();
	handle->Close();

	return path;
}

// Util function to verify buffer content using BufferHandle
void VerifyData(const BufferHandle &buffer_handle, idx_t size, idx_t file_offset) {
	REQUIRE(buffer_handle.IsValid());
	data_ptr_t buffer = buffer_handle.Ptr();

	for (idx_t idx = 0; idx < size; ++idx) {
		uint8_t expected = static_cast<uint8_t>(((file_offset + idx) / (1024 * 1024)) % 256);
		if (buffer[idx] != expected) {
			idx_t actual_file_offset = file_offset + idx;
			FAIL("Data mismatch at buffer index " << idx << " (file offset " << actual_file_offset << "): got "
			                                      << static_cast<int>(buffer[idx]) << ", expected "
			                                      << static_cast<int>(expected));
		}
		REQUIRE(buffer[idx] == expected);
	}
}

// Util function to clear cache between tests
void ClearCache(ExternalFileCache &cache) {
	cache.SetEnabled(false);
	cache.SetEnabled(true);
}

// Test template for different scenarios
void TestReadScenario(FileSystem &fs, ExternalFileCache &cache, DatabaseInstance &db, const string &test_file,
                      const string &policy_name, idx_t read_location, idx_t read_bytes) {
	auto &config = DBConfig::GetConfig(db);
	config.options.external_file_cache_read_policy_name = policy_name;

	ClearCache(cache);

	CachingFileSystem caching_fs(fs, db);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

	// First read, which should miss cache
	data_ptr_t buffer1;
	auto buffer_handle1 = handle->Read(buffer1, read_bytes, read_location);
	VerifyData(/*buffer_handle=*/buffer_handle1, /*size=*/read_bytes, /*file_offset=*/read_location);
	
	// Second read, which should hit cache
	data_ptr_t buffer2;
	auto buffer_handle2 = handle->Read(buffer2, read_bytes, read_location);
	VerifyData(/*buffer_handle=*/buffer_handle2, /*size=*/read_bytes, /*file_offset=*/read_location);
}

// ============================================================================
// DEFAULT READ POLICY TESTS
// ============================================================================

TEST_CASE("DefaultReadPolicy - Single block, perfectly aligned", "[caching_file_system][default_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_default_single_aligned"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_default_single_aligned.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	// Read exactly 1MiB at 1MiB boundary (aligned to 1MiB)
	TestReadScenario(fs, cache, *db.instance, test_file, /*policy_name=*/"default", /*read_location=*/1024 * 1024,
	                 /*read_bytes=*/1024 * 1024);
}

TEST_CASE("DefaultReadPolicy - Single block, unaligned", "[caching_file_system][default_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_default_single_unaligned"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_default_single_unaligned.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	// Read 512KB starting at 1.5MiB (unaligned)
	TestReadScenario(fs, cache, *db.instance, test_file, /*policy_name=*/"default",
	                 /*read_location=*/static_cast<idx_t>(1.5 * 1024 * 1024), /*read_bytes=*/512 * 1024);
}

TEST_CASE("DefaultReadPolicy - Multiple blocks, perfectly aligned", "[caching_file_system][default_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_default_multi_aligned"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_default_multi_aligned.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	// Read 3MiB starting at 1MiB (aligned to 1MiB boundaries, spans multiple 1MiB segments)
	TestReadScenario(fs, cache, *db.instance, test_file, /*policy_name=*/"default", /*read_location=*/1024 * 1024,
	                 /*read_bytes=*/3 * 1024 * 1024);
}

TEST_CASE("DefaultReadPolicy - Multiple blocks, unaligned", "[caching_file_system][default_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_default_multi_unaligned"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_default_multi_unaligned.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	// Read 2.5MiB starting at 1.5MiB (unaligned, spans multiple segments)
	TestReadScenario(fs, cache, *db.instance, test_file, /*policy_name=*/"default",
	                 /*read_location=*/static_cast<idx_t>(1.5 * 1024 * 1024),
	                 /*read_bytes=*/static_cast<idx_t>(2.5 * 1024 * 1024));
}

// ============================================================================
// ALIGNED READ POLICY TESTS
// ============================================================================

TEST_CASE("AlignedReadPolicy - Single block, perfectly aligned", "[caching_file_system][aligned_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_aligned_single_aligned"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_aligned_single_aligned.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	// Read 1MiB at 2MiB (aligned to 2MiB boundary, fits in single 2MiB block)
	TestReadScenario(fs, cache, *db.instance, test_file, /*policy_name=*/"aligned", /*read_location=*/2 * 1024 * 1024,
	                 /*read_bytes=*/1024 * 1024);
}

TEST_CASE("AlignedReadPolicy - Single block, unaligned", "[caching_file_system][aligned_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_aligned_single_unaligned"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_aligned_single_unaligned.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	// Read 1MiB at 3MiB (unaligned, but fits in single 2MiB block: 2-4MiB)
	TestReadScenario(fs, cache, *db.instance, test_file, /*policy_name=*/"aligned", /*read_location=*/3 * 1024 * 1024,
	                 /*read_bytes=*/1024 * 1024);
}

TEST_CASE("AlignedReadPolicy - Multiple blocks, perfectly aligned", "[caching_file_system][aligned_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_aligned_multi_aligned"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_aligned_multi_aligned.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	// Read 4MiB at 2MiB (aligned to 2MiB boundary, spans two 2MiB blocks: 2-4MiB and 4-6MiB)
	TestReadScenario(fs, cache, *db.instance, test_file, /*policy_name=*/"aligned", /*read_location=*/2 * 1024 * 1024,
	                 /*read_bytes=*/4 * 1024 * 1024);
}

TEST_CASE("AlignedReadPolicy - Multiple blocks, unaligned", "[caching_file_system][aligned_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_aligned_multi_unaligned"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_aligned_multi_unaligned.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	// Read 3MiB at 3MiB (unaligned start, spans two 2MiB blocks: 2-4MiB and 4-6MiB)
	TestReadScenario(fs, cache, *db.instance, test_file, /*policy_name=*/"aligned", /*read_location=*/3 * 1024 * 1024,
	                 /*read_bytes=*/3 * 1024 * 1024);
}

// ============================================================================
// OVERLAPPING READS TESTS
// ============================================================================

TEST_CASE("DefaultReadPolicy - Overlapping reads", "[caching_file_system][default_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "default";

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_default_overlapping"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_default_overlapping.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

	// First read: 1MiB starting at 1MiB
	data_ptr_t buffer1;
	auto buffer_handle1 = handle->Read(buffer1, 1024 * 1024, 1024 * 1024);
	VerifyData(/*buffer_handle=*/buffer_handle1, /*size=*/1024 * 1024, /*file_offset=*/1024 * 1024);

	// Second read: 1MiB starting at 1.5MiB (overlaps with first read: 0.5MiB overlap)
	// This should use the cached portion from the first read and only read the new 0.5MiB
	data_ptr_t buffer2;
	auto buffer_handle2 = handle->Read(buffer2, 1024 * 1024, static_cast<idx_t>(1.5 * 1024 * 1024));
	VerifyData(/*buffer_handle=*/buffer_handle2, /*size=*/1024 * 1024, /*file_offset=*/static_cast<idx_t>(1.5 * 1024 * 1024));

	// Third read: 2MiB starting at 0.5MiB (overlaps with both previous reads)
	// Should use cached portions from both previous reads
	data_ptr_t buffer3;
	auto buffer_handle3 = handle->Read(buffer3, 2 * 1024 * 1024, 512 * 1024);
	VerifyData(/*buffer_handle=*/buffer_handle3, /*size=*/2 * 1024 * 1024, /*file_offset=*/512 * 1024);
}

TEST_CASE("AlignedReadPolicy - Overlapping reads", "[caching_file_system][aligned_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_aligned_overlapping"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_aligned_overlapping.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

	// First read: 1MiB starting at 1.5MiB
	// With aligned policy, this should cache the 0-2MiB block
	data_ptr_t buffer1;
	auto buffer_handle1 = handle->Read(buffer1, 1024 * 1024, static_cast<idx_t>(1.5 * 1024 * 1024));
	VerifyData(/*buffer_handle=*/buffer_handle1, /*size=*/1024 * 1024, /*file_offset=*/static_cast<idx_t>(1.5 * 1024 * 1024));

	// Second read: 1MiB starting at 0.5MiB (overlaps with first read's cached block: 0-2MiB)
	// Should hit the cache from the first read
	data_ptr_t buffer2;
	auto buffer_handle2 = handle->Read(buffer2, 1024 * 1024, 512 * 1024);
	VerifyData(/*buffer_handle=*/buffer_handle2, /*size=*/1024 * 1024, /*file_offset=*/512 * 1024);

	// Third read: 1MiB starting at 3.5MiB
	// With aligned policy, this should cache the 2-4MiB block
	data_ptr_t buffer3;
	auto buffer_handle3 = handle->Read(buffer3, 1024 * 1024, static_cast<idx_t>(3.5 * 1024 * 1024));
	VerifyData(/*buffer_handle=*/buffer_handle3, /*size=*/1024 * 1024, /*file_offset=*/static_cast<idx_t>(3.5 * 1024 * 1024));

	// Fourth read: 2MiB starting at 2.5MiB (overlaps with third read's cached block: 2-4MiB)
	// Should use the cached portion from the third read
	data_ptr_t buffer4;
	auto buffer_handle4 = handle->Read(buffer4, 2 * 1024 * 1024, static_cast<idx_t>(2.5 * 1024 * 1024));
	VerifyData(/*buffer_handle=*/buffer_handle4, /*size=*/2 * 1024 * 1024, /*file_offset=*/static_cast<idx_t>(2.5 * 1024 * 1024));
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

TEST_CASE("CachingFileSystem - Concurrent reads duplicate prevention", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_concurrent"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_concurrent.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	CachingFileSystem caching_fs(fs, *db.instance);

	// Simulate concurrent reads to same aligned block
	// Both reads align to 2-4MiB block
	vector<thread> threads;

	for (idx_t idx = 0; idx < 2; ++idx) {
		threads.emplace_back([&]() {
			OpenFileInfo info;
			info.path = test_file;
			auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

			data_ptr_t buffer;
			auto buffer_handle = handle->Read(buffer, 1024 * 1024, static_cast<idx_t>(2.5 * 1024 * 1024));
			VerifyData(/*buffer_handle=*/buffer_handle, /*size=*/1024 * 1024, /*file_offset=*/static_cast<idx_t>(2.5 * 1024 * 1024));
		});
	}

	for (auto &thread : threads) {
		thread.join();
	}
}

TEST_CASE("CachingFileSystem - Mixed cached and uncached blocks", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_mixed"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_mixed.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

	// Cache blocks 0-2MiB and 4-6MiB
	data_ptr_t buffer1;
	auto buffer_handle1 = handle->Read(buffer1, 1024 * 1024, 512 * 1024); // Caches 0-2MiB
	(void)buffer_handle1; // Keep buffer1 valid

	data_ptr_t buffer2;
	auto buffer_handle2 = handle->Read(buffer2, 1024 * 1024, static_cast<idx_t>(4.5 * 1024 * 1024)); // Caches 4-6MiB
	(void)buffer_handle2; // Keep buffer2 valid

	// Now read 5MiB starting at 0
	// Blocks: 0-2MiB (cached), 2-4MiB (miss), 4-6MiB (cached)
	data_ptr_t buffer3;
	auto buffer_handle3 = handle->Read(buffer3, 5 * 1024 * 1024, 0);
	VerifyData(/*buffer_handle=*/buffer_handle3, /*size=*/5 * 1024 * 1024, /*file_offset=*/0);
}

TEST_CASE("CachingFileSystem - Cache invalidation", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_invalidation"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_invalidation.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/5);

	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;

	// Read and cache
	{
		auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
		data_ptr_t buffer;
		handle->Read(buffer, 1024 * 1024, 0);
	}

	// Clear cache
	ClearCache(cache);

	// Read again - should read from file
	{
		auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
		data_ptr_t buffer;
		auto buffer_handle = handle->Read(buffer, 1024 * 1024, 0);
		VerifyData(/*buffer_handle=*/buffer_handle, /*size=*/1024 * 1024, /*file_offset=*/0);
	}
}

TEST_CASE("CachingFileSystem - Multi-threaded reads", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_multithreaded"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_multithreaded.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/20);

	CachingFileSystem caching_fs(fs, *db.instance);

	// Test with multiple threads reading different ranges concurrently
	constexpr idx_t num_threads = 8;
	vector<thread> threads;

	// Each thread reads a different 1MiB range starting at different offsets
	for (idx_t i = 0; i < num_threads; i++) {
		threads.emplace_back([&, i]() {
			OpenFileInfo info;
			info.path = test_file;
			auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

			// Each thread reads 1MiB at different locations: 0MiB, 2MiB, 4MiB, 6MiB, etc.
			idx_t read_location = i * 2 * 1024 * 1024;
			idx_t read_bytes = 1024 * 1024;

			data_ptr_t buffer;
			auto buffer_handle = handle->Read(buffer, read_bytes, read_location);
			D_ASSERT(buffer_handle.IsValid());
			VerifyData(/*buffer_handle=*/buffer_handle, /*size=*/read_bytes, /*file_offset=*/read_location);
		});
	}

	// Wait for all threads to complete
	for (auto &thread : threads) {
		thread.join();
	}

	// Now test concurrent reads to the same cached block (cache hits)
	ClearCache(cache);

	// First, populate cache with one read
	{
		OpenFileInfo info;
		info.path = test_file;
		auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
		data_ptr_t buffer;
		handle->Read(buffer, 1024 * 1024, 2 * 1024 * 1024);
	}

	// Now multiple threads read from the same cached block
	vector<thread> cache_hit_threads;

	for (idx_t i = 0; i < 4; i++) {
		cache_hit_threads.emplace_back([&, i]() {
			OpenFileInfo info;
			info.path = test_file;
			auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

			// All threads read from the same cached block (2-4MiB aligned block)
			// But at slightly different offsets within that block
			idx_t read_location = (2 * 1024 * 1024) + (i * 256 * 1024);
			idx_t read_bytes = 256 * 1024;

			data_ptr_t buffer;
			auto buffer_handle = handle->Read(buffer, read_bytes, read_location);
			D_ASSERT(buffer_handle.IsValid());
			VerifyData(/*buffer_handle=*/buffer_handle, /*size=*/read_bytes, /*file_offset=*/read_location);
		});
	}

	for (auto &thread : cache_hit_threads) {
		thread.join();
	}
}

TEST_CASE("CachingFileSystem - Concurrent same block requests", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);

	ScopedDirectory test_dir(TestJoinPath(TestDirectoryPath(), "test_concurrent_same_block"));
	auto test_file = TestJoinPath(test_dir.GetPath(), "cache_test_concurrent_same_block.bin");
	CreateTestFile(fs, test_file, /*size_mb=*/10);

	// Test with both read policies
	for (const auto &policy_name : {"default", "aligned"}) {
		auto &config = DBConfig::GetConfig(*db.instance);
		config.options.external_file_cache_read_policy_name = policy_name;

		// Clear cache for each policy test
		ClearCache(cache);

		CachingFileSystem caching_fs(fs, *db.instance);

		// Test 1: Multiple threads requesting the same block (cache miss)
		// All threads try to read the same 1MiB range starting at 2MiB
		constexpr idx_t num_threads = 16;
		constexpr idx_t read_location = 2 * 1024 * 1024;
		constexpr idx_t read_bytes = 1024 * 1024;

		vector<thread> cache_miss_threads;
		vector<bool> success_flags(num_threads, false);
		atomic<idx_t> threads_ready(0);

		// Use a barrier-like mechanism to maximize contention
		for (idx_t i = 0; i < num_threads; i++) {
			cache_miss_threads.emplace_back([&, i]() {
				threads_ready.fetch_add(1);
				// Wait for all threads to be ready
				while (threads_ready.load() < num_threads) {
					std::this_thread::yield();
				}

				OpenFileInfo info;
				info.path = test_file;
				auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

				data_ptr_t buffer;
				auto buffer_handle = handle->Read(buffer, read_bytes, read_location);
				if (buffer_handle.IsValid()) {
					VerifyData(/*buffer_handle=*/buffer_handle, /*size=*/read_bytes, /*file_offset=*/read_location);
					success_flags[i] = true;
				}
			});
		}

		// Wait for all threads to complete
		for (auto &thread : cache_miss_threads) {
			thread.join();
		}

		// Verify all threads succeeded
		for (idx_t i = 0; i < num_threads; i++) {
			REQUIRE(success_flags[i] == true);
		}

		// Test 2: Multiple threads requesting the same already-cached block (cache hit)
		// The block should already be in cache from the previous test
		vector<thread> cache_hit_threads;
		vector<bool> cache_hit_success_flags(num_threads, false);
		atomic<idx_t> cache_hit_threads_ready(0);

		for (idx_t i = 0; i < num_threads; i++) {
			cache_hit_threads.emplace_back([&, i]() {
				cache_hit_threads_ready.fetch_add(1);
				// Wait for all threads to be ready
				while (cache_hit_threads_ready.load() < num_threads) {
					std::this_thread::yield();
				}

				OpenFileInfo info;
				info.path = test_file;
				auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

				// Read from the same cached block, but potentially at different offsets within it
				idx_t offset_within_block = (i % 4) * 256 * 1024; // Different offsets within the same block
				idx_t thread_read_location = read_location + offset_within_block;
				idx_t thread_read_bytes = 256 * 1024;

				data_ptr_t buffer;
				auto buffer_handle = handle->Read(buffer, thread_read_bytes, thread_read_location);
				if (buffer_handle.IsValid()) {
					VerifyData(/*buffer_handle=*/buffer_handle, /*size=*/thread_read_bytes, /*file_offset=*/thread_read_location);
					cache_hit_success_flags[i] = true;
				}
			});
		}

		// Wait for all threads to complete
		for (auto &thread : cache_hit_threads) {
			thread.join();
		}

		// Verify all threads succeeded
		for (idx_t i = 0; i < num_threads; i++) {
			REQUIRE(cache_hit_success_flags[i] == true);
		}

		// Test 3: Multiple threads requesting overlapping ranges that map to the same block
		// This tests the case where reads are not perfectly aligned but still hit the same cached block
		ClearCache(cache);

		constexpr idx_t overlap_num_threads = 8;
		vector<thread> overlap_threads;
		vector<bool> overlap_success_flags(overlap_num_threads, false);
		atomic<idx_t> overlap_threads_ready(0);

		// All threads read overlapping ranges that all fall within the same 2MiB aligned block
		// For aligned policy, this should still use the same underlying cached block
		for (idx_t i = 0; i < overlap_num_threads; i++) {
			overlap_threads.emplace_back([&, i]() {
				overlap_threads_ready.fetch_add(1);
				// Wait for all threads to be ready
				while (overlap_threads_ready.load() < overlap_num_threads) {
					std::this_thread::yield();
				}

				OpenFileInfo info;
				info.path = test_file;
				auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);

				// Each thread reads a slightly different offset, but all within the same block
				idx_t thread_read_location = read_location + (i * 128 * 1024);
				idx_t thread_read_bytes = 512 * 1024;

				data_ptr_t buffer;
				auto buffer_handle = handle->Read(buffer, thread_read_bytes, thread_read_location);
				if (buffer_handle.IsValid()) {
					VerifyData(/*buffer_handle=*/buffer_handle, /*size=*/thread_read_bytes, /*file_offset=*/thread_read_location);
					overlap_success_flags[i] = true;
				}
			});
		}

		// Wait for all threads to complete
		for (auto &thread : overlap_threads) {
			thread.join();
		}

		// Verify all threads succeeded
		for (idx_t i = 0; i < overlap_num_threads; i++) {
			REQUIRE(overlap_success_flags[i] == true);
		}
	}
}

TEST_CASE("CachingFileSystem - Position-based Read overload", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &base_fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	CachingFileSystem fs(base_fs, *db.instance);

	ScopedDirectory test_dir("test_position_read");
	string test_file = test_dir.GetPath() + "/test_data.bin";

	// Create test file with 10MB of sequential data
	CreateTestFile(base_fs, test_file, /*size_mb=*/10);

	SECTION("Sequential reads using position-based overload") {
		// Test with DefaultReadPolicy
		DBConfig::GetConfig(*db.instance).options.external_file_cache_read_policy_name = "default";
		ClearCache(cache);

		auto handle = fs.OpenFile(test_file, FileFlags::FILE_FLAGS_READ);

		// First read: 1MB at position 0
		data_ptr_t buffer1 = nullptr;
		idx_t bytes1 = 1024 * 1024;
		auto buffer_handle1 = handle->Read(buffer1, bytes1);
		REQUIRE(buffer1 != nullptr);
		REQUIRE(bytes1 == 1024 * 1024);
		VerifyData(/*buffer_handle=*/buffer_handle1, /*size=*/bytes1, /*file_offset=*/0);

		// Second read: should start at position 1MB (position was updated)
		data_ptr_t buffer2 = nullptr;
		idx_t bytes2 = 512 * 1024;
		auto buffer_handle2 = handle->Read(buffer2, bytes2);
		REQUIRE(buffer2 != nullptr);
		REQUIRE(bytes2 == 512 * 1024);
		VerifyData(/*buffer_handle=*/buffer_handle2, /*size=*/bytes2, /*file_offset=*/1024 * 1024);

		// Third read: should start at position 1.5MB
		data_ptr_t buffer3 = nullptr;
		idx_t bytes3 = 256 * 1024;
		auto buffer_handle3 = handle->Read(buffer3, bytes3);
		REQUIRE(buffer3 != nullptr);
		REQUIRE(bytes3 == 256 * 1024);
		VerifyData(/*buffer_handle=*/buffer_handle3, /*size=*/bytes3, /*file_offset=*/static_cast<idx_t>(1.5 * 1024 * 1024));
	}

	SECTION("Sequential reads with AlignedReadPolicy") {
		DBConfig::GetConfig(*db.instance).options.external_file_cache_read_policy_name = "aligned";
		ClearCache(cache);

		auto handle = fs.OpenFile(test_file, FileFlags::FILE_FLAGS_READ);

		// Read 1MB starting from position 0
		data_ptr_t buffer1 = nullptr;
		idx_t bytes1 = 1024 * 1024;
		auto buffer_handle1 = handle->Read(buffer1, bytes1);
		VerifyData(/*buffer_handle=*/buffer_handle1, /*size=*/bytes1, /*file_offset=*/0);

		// Next read should continue from 1MB
		data_ptr_t buffer2 = nullptr;
		idx_t bytes2 = 1024 * 1024;
		auto buffer_handle2 = handle->Read(buffer2, bytes2);
		VerifyData(/*buffer_handle=*/buffer_handle2, /*size=*/bytes2, /*file_offset=*/1024 * 1024);
	}

	SECTION("Seek and position-based reads") {
		DBConfig::GetConfig(*db.instance).options.external_file_cache_read_policy_name = "default";
		ClearCache(cache);

		auto handle = fs.OpenFile(test_file, FileFlags::FILE_FLAGS_READ);

		// Read at position 0
		data_ptr_t buffer1 = nullptr;
		idx_t bytes1 = 512 * 1024;
		auto buffer_handle1 = handle->Read(buffer1, bytes1);
		VerifyData(/*buffer_handle=*/buffer_handle1, /*size=*/bytes1, /*file_offset=*/0);

		// Seek to 2MB
		handle->Seek(2 * 1024 * 1024);

		// Read should now be at 2MB
		data_ptr_t buffer2 = nullptr;
		idx_t bytes2 = 512 * 1024;
		auto buffer_handle2 = handle->Read(buffer2, bytes2);
		VerifyData(/*buffer_handle=*/buffer_handle2, /*size=*/bytes2, /*file_offset=*/2 * 1024 * 1024);

		// Next read should continue from 2.5MB
		data_ptr_t buffer3 = nullptr;
		idx_t bytes3 = 512 * 1024;
		auto buffer_handle3 = handle->Read(buffer3, bytes3);
		VerifyData(/*buffer_handle=*/buffer_handle3, /*size=*/bytes3, /*file_offset=*/static_cast<idx_t>(2.5 * 1024 * 1024));
	}

	SECTION("Position-based reads should hit cache") {
		DBConfig::GetConfig(*db.instance).options.external_file_cache_read_policy_name = "default";
		ClearCache(cache);

		auto handle1 = fs.OpenFile(test_file, FileFlags::FILE_FLAGS_READ);

		// First read - cache miss
		data_ptr_t buffer1 = nullptr;
		idx_t bytes1 = 1024 * 1024;
		auto buffer_handle1 = handle1->Read(buffer1, bytes1);
		VerifyData(/*buffer_handle=*/buffer_handle1, /*size=*/bytes1, /*file_offset=*/0);

		// Open another handle
		auto handle2 = fs.OpenFile(test_file, FileFlags::FILE_FLAGS_READ);

		// Read the same data - should be cache hit
		data_ptr_t buffer2 = nullptr;
		idx_t bytes2 = 1024 * 1024;
		auto buffer_handle2 = handle2->Read(buffer2, bytes2);
		VerifyData(/*buffer_handle=*/buffer_handle2, /*size=*/bytes2, /*file_offset=*/0);

		// Verify data is cached
		auto cache_info = cache.GetCachedFileInformation();
		bool found = false;
		for (auto &file : cache_info) {
			if (file.path == test_file) {
				found = true;
				REQUIRE(file.loaded);
				REQUIRE(file.nr_bytes > 0);
				break;
			}
		}
		REQUIRE(found);
	}

	SECTION("Multiple sequential position-based reads across blocks") {
		DBConfig::GetConfig(*db.instance).options.external_file_cache_read_policy_name = "aligned";
		ClearCache(cache);

		auto handle = fs.OpenFile(test_file, FileFlags::FILE_FLAGS_READ);

		// Read in 512KB chunks, crossing block boundaries (2MiB blocks)
		const idx_t chunk_size = 512 * 1024;
		const idx_t num_chunks = 8; // Total 4MB

		for (idx_t i = 0; i < num_chunks; ++i) {
			data_ptr_t buffer = nullptr;
			idx_t bytes = chunk_size;
			auto buffer_handle = handle->Read(buffer, bytes);
			REQUIRE(buffer != nullptr);
			REQUIRE(bytes == chunk_size);
			VerifyData(/*buffer_handle=*/buffer_handle, /*size=*/bytes, /*file_offset=*/i * chunk_size);
		}
	}
}

} // namespace duckdb
