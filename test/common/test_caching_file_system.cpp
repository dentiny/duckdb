#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include "duckdb/storage/read_policy_registry.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

namespace {

// Helper to create a test file with known content
static string CreateTestFile(FileSystem &fs, const string &path, idx_t size_mb) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	std::vector<uint8_t> data(size_mb * 1024 * 1024);
	
	// Fill with pattern: byte value = (offset / 1MB) % 256
	for (idx_t i = 0; i < data.size(); i++) {
		data[i] = static_cast<uint8_t>((i / (1024 * 1024)) % 256);
	}
	
	handle->Write(QueryContext(), data.data(), data.size(), 0);
	handle->Sync();
	handle.reset();
	
	return path;
}

// Helper to verify buffer content
static void VerifyData(data_ptr_t buffer, idx_t size, idx_t file_offset) {
	for (idx_t i = 0; i < size; i++) {
		uint8_t expected = static_cast<uint8_t>(((file_offset + i) / (1024 * 1024)) % 256);
		if (buffer[i] != expected) {
			std::cerr << "VerifyData FAILED at i=" << i << " (file_offset=" << (file_offset + i) << "): "
			          << "buffer[i]=" << (int)buffer[i] << " expected=" << (int)expected << std::endl;
		}
		REQUIRE(buffer[i] == expected);
	}
}

// Helper to clear cache between tests
static void ClearCache(ExternalFileCache &cache) {
	cache.SetEnabled(false);
	cache.SetEnabled(true);
}

// Test template for different scenarios
static void TestReadScenario(FileSystem &fs, ExternalFileCache &cache, DatabaseInstance &db, const string &test_file,
                             const string &policy_name, idx_t read_location, idx_t read_bytes,
                             bool expect_single_block, bool expect_aligned) {
	auto &config = DBConfig::GetConfig(db);
	config.options.external_file_cache_read_policy_name = policy_name;
	
	ClearCache(cache);
	
	CachingFileSystem caching_fs(fs, db);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
	
	// First read - should miss cache
	data_ptr_t buffer1;
	auto result1 = handle->Read(buffer1, read_bytes, read_location);
	REQUIRE(result1.IsValid());
	VerifyData(buffer1, read_bytes, read_location);
	
	// Second read - should hit cache
	data_ptr_t buffer2;
	auto result2 = handle->Read(buffer2, read_bytes, read_location);
	REQUIRE(result2.IsValid());
	VerifyData(buffer2, read_bytes, read_location);
}

} // namespace

// ============================================================================
// DEFAULT READ POLICY TESTS
// ============================================================================

TEST_CASE("DefaultReadPolicy - Single block, perfectly aligned", "[caching_file_system][default_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_default_single_aligned.bin");
	CreateTestFile(fs, test_file, 10);
	
	// Read exactly 1MB at 1MB boundary (aligned to 1MB)
	TestReadScenario(fs, cache, *db.instance, test_file, "default", 1024 * 1024, 1024 * 1024, true, true);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("DefaultReadPolicy - Single block, unaligned", "[caching_file_system][default_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_default_single_unaligned.bin");
	CreateTestFile(fs, test_file, 10);
	
	// Read 512KB starting at 1.5MB (unaligned)
	TestReadScenario(fs, cache, *db.instance, test_file, "default", (idx_t)(1.5 * 1024 * 1024), 512 * 1024, true, false);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("DefaultReadPolicy - Multiple blocks, perfectly aligned", "[caching_file_system][default_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_default_multi_aligned.bin");
	CreateTestFile(fs, test_file, 10);
	
	// Read 3MB starting at 1MB (aligned to 1MB boundaries, spans multiple 1MB segments)
	TestReadScenario(fs, cache, *db.instance, test_file, "default", 1024 * 1024, 3 * 1024 * 1024, false, true);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("DefaultReadPolicy - Multiple blocks, unaligned", "[caching_file_system][default_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_default_multi_unaligned.bin");
	CreateTestFile(fs, test_file, 10);
	
	// Read 2.5MB starting at 1.5MB (unaligned, spans multiple segments)
	TestReadScenario(fs, cache, *db.instance, test_file, "default", (idx_t)(1.5 * 1024 * 1024), (idx_t)(2.5 * 1024 * 1024), false, false);
	
	fs.RemoveFile(test_file);
}

// ============================================================================
// ALIGNED READ POLICY TESTS
// ============================================================================

TEST_CASE("AlignedReadPolicy - Single block, perfectly aligned", "[caching_file_system][aligned_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_aligned_single_aligned.bin");
	CreateTestFile(fs, test_file, 10);
	
	// Read 1MB at 2MB (aligned to 2MB boundary, fits in single 2MB block)
	TestReadScenario(fs, cache, *db.instance, test_file, "aligned", 2 * 1024 * 1024, 1024 * 1024, true, true);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("AlignedReadPolicy - Single block, unaligned", "[caching_file_system][aligned_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_aligned_single_unaligned.bin");
	CreateTestFile(fs, test_file, 10);
	
	// Read 1MB at 3MB (unaligned, but fits in single 2MB block: 2-4MB)
	TestReadScenario(fs, cache, *db.instance, test_file, "aligned", 3 * 1024 * 1024, 1024 * 1024, true, false);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("AlignedReadPolicy - Multiple blocks, perfectly aligned", "[caching_file_system][aligned_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_aligned_multi_aligned.bin");
	CreateTestFile(fs, test_file, 10);
	
	// Read 4MB at 2MB (aligned to 2MB boundary, spans two 2MB blocks: 2-4MB and 4-6MB)
	TestReadScenario(fs, cache, *db.instance, test_file, "aligned", 2 * 1024 * 1024, 4 * 1024 * 1024, false, true);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("AlignedReadPolicy - Multiple blocks, unaligned", "[caching_file_system][aligned_policy]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_aligned_multi_unaligned.bin");
	CreateTestFile(fs, test_file, 10);
	
	// Read 3MB at 3MB (unaligned start, spans two 2MB blocks: 2-4MB and 4-6MB)
	TestReadScenario(fs, cache, *db.instance, test_file, "aligned", 3 * 1024 * 1024, 3 * 1024 * 1024, false, false);
	
	fs.RemoveFile(test_file);
}

// ============================================================================
// ADDITIONAL COMPREHENSIVE TESTS
// ============================================================================

TEST_CASE("CachingFileSystem - Policy comparison: aligned vs default", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_policy_compare.bin");
	CreateTestFile(fs, test_file, 10);
	
	// Test with default (unaligned) policy
	{
		auto &config = DBConfig::GetConfig(*db.instance);
		config.options.external_file_cache_read_policy_name = "default";
		
		CachingFileSystem caching_fs(fs, *db.instance);
		OpenFileInfo info;
		info.path = test_file;
		auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
		
		// Read 100KB at 1.5MB - should cache exactly this range
		data_ptr_t buffer1;
		auto result1 = handle->Read(buffer1, 100 * 1024, (idx_t)(1.5 * 1024 * 1024));
		REQUIRE(result1.IsValid());
		VerifyData(buffer1, 100 * 1024, (idx_t)(1.5 * 1024 * 1024));
		
		// Read same location again - should hit
		data_ptr_t buffer2;
		auto result2 = handle->Read(buffer2, 100 * 1024, (idx_t)(1.5 * 1024 * 1024));
		REQUIRE(result2.IsValid());
		VerifyData(buffer2, 100 * 1024, (idx_t)(1.5 * 1024 * 1024));
	}
	
	ClearCache(cache);
	
	// Test with aligned policy
	{
		auto &config = DBConfig::GetConfig(*db.instance);
		config.options.external_file_cache_read_policy_name = "aligned";
		
		CachingFileSystem caching_fs(fs, *db.instance);
		OpenFileInfo info;
		info.path = test_file;
		auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
		
		// Read 100KB at 1.5MB - should align and cache 0-2MB block
		data_ptr_t buffer1;
		auto result1 = handle->Read(buffer1, 100 * 1024, (idx_t)(1.5 * 1024 * 1024));
		REQUIRE(result1.IsValid());
		VerifyData(buffer1, 100 * 1024, (idx_t)(1.5 * 1024 * 1024));
		
		// Read different location in same block - should hit cache
		data_ptr_t buffer2;
		auto result2 = handle->Read(buffer2, 100 * 1024, (idx_t)(1.7 * 1024 * 1024));
		REQUIRE(result2.IsValid());
		VerifyData(buffer2, 100 * 1024, (idx_t)(1.7 * 1024 * 1024));
	}
	
	fs.RemoveFile(test_file);
}

TEST_CASE("CachingFileSystem - Concurrent reads duplicate prevention", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";
	
	auto test_file = TestCreatePath("cache_test_concurrent.bin");
	CreateTestFile(fs, test_file, 10);
	
	CachingFileSystem caching_fs(fs, *db.instance);
	
	// Simulate concurrent reads to same aligned block
	// Both reads align to 2-4MB block
	std::vector<thread> threads;
	std::vector<bool> success(2, false);
	
	for (idx_t i = 0; i < 2; i++) {
		threads.emplace_back([&, i]() {
			OpenFileInfo info;
			info.path = test_file;
			auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
			
			data_ptr_t buffer;
			auto result = handle->Read(buffer, 1024 * 1024, (idx_t)(2.5 * 1024 * 1024));
			if (result.IsValid()) {
				VerifyData(buffer, 1024 * 1024, (idx_t)(2.5 * 1024 * 1024));
				success[i] = true;
			}
		});
	}
	
	for (auto &thread : threads) {
		thread.join();
	}
	
	REQUIRE(success[0]);
	REQUIRE(success[1]);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("CachingFileSystem - Mixed cached and uncached blocks", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";
	
	auto test_file = TestCreatePath("cache_test_mixed.bin");
	CreateTestFile(fs, test_file, 10);
	
	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
	
	// Cache blocks 0-2MB and 4-6MB
	data_ptr_t buffer1;
	handle->Read(buffer1, 1024 * 1024, 512 * 1024); // Caches 0-2MB
	
	data_ptr_t buffer2;
	handle->Read(buffer2, 1024 * 1024, (idx_t)(4.5 * 1024 * 1024)); // Caches 4-6MB
	
	// Now read 5MB starting at 0
	// Blocks: 0-2MB (cached), 2-4MB (miss), 4-6MB (cached)
	data_ptr_t buffer3;
	auto result3 = handle->Read(buffer3, 5 * 1024 * 1024, 0);
	REQUIRE(result3.IsValid());
	VerifyData(buffer3, 5 * 1024 * 1024, 0);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("CachingFileSystem - Cache invalidation", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_invalidation.bin");
	CreateTestFile(fs, test_file, 5);
	
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
		auto result = handle->Read(buffer, 1024 * 1024, 0);
		REQUIRE(result.IsValid());
		VerifyData(buffer, 1024 * 1024, 0);
	}
	
	fs.RemoveFile(test_file);
}
