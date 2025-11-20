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

} // namespace

TEST_CASE("CachingFileSystem - Basic unaligned read", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto test_file = TestCreatePath("cache_test_basic.bin");
	CreateTestFile(fs, test_file, 5); // 5MB file
	
	// Open file through caching filesystem
	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
	
	// Read 1MB at offset 1MB
	data_ptr_t buffer;
	auto result = handle->Read(buffer, 1024 * 1024, 1024 * 1024);
	REQUIRE(result.IsValid());
	VerifyData(buffer, 1024 * 1024, 1024 * 1024);
	
	// Second read of same location should hit cache
	data_ptr_t buffer2;
	auto result2 = handle->Read(buffer2, 1024 * 1024, 1024 * 1024);
	REQUIRE(result2.IsValid());
	VerifyData(buffer2, 1024 * 1024, 1024 * 1024);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("CachingFileSystem - Aligned read full miss", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	// Set aligned read policy
	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";
	
	auto test_file = TestCreatePath("cache_test_aligned.bin");
	CreateTestFile(fs, test_file, 10); // 10MB file
	
	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
	
	// Read 1MB at offset 3MB - should align to 2MB boundaries
	// Will read blocks: 2-4MB
	data_ptr_t buffer;
	auto result = handle->Read(buffer, 1024 * 1024, 3 * 1024 * 1024);
	REQUIRE(result.IsValid());
	VerifyData(buffer, 1024 * 1024, 3 * 1024 * 1024);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("CachingFileSystem - Aligned read full cache hit", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";
	
	auto test_file = TestCreatePath("cache_test_aligned_hit.bin");
	CreateTestFile(fs, test_file, 10);
	
	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
	
	// First read: 1MB at 3MB (aligns to 2-4MB block)
	data_ptr_t buffer1;
	auto result1 = handle->Read(buffer1, 1024 * 1024, 3 * 1024 * 1024);
	REQUIRE(result1.IsValid());
	VerifyData(buffer1, 1024 * 1024, 3 * 1024 * 1024);
	
	// Second read: same location - should be full cache hit
	data_ptr_t buffer2;
	auto result2 = handle->Read(buffer2, 1024 * 1024, 3 * 1024 * 1024);
	REQUIRE(result2.IsValid());
	VerifyData(buffer2, 1024 * 1024, 3 * 1024 * 1024);
	
	// Third read: overlapping but within same block - should hit cache
	data_ptr_t buffer3;
	auto result3 = handle->Read(buffer3, 512 * 1024, 3 * 1024 * 1024 + 256 * 1024);
	REQUIRE(result3.IsValid());
	VerifyData(buffer3, 512 * 1024, 3 * 1024 * 1024 + 256 * 1024);
	
	fs.RemoveFile(test_file);
}

TEST_CASE("CachingFileSystem - Aligned read partial cache hit", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";
	
	auto test_file = TestCreatePath("cache_test_partial_hit.bin");
	CreateTestFile(fs, test_file, 10);
	
	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
	
	// First read: 1MB at 1MB (aligns to 0-2MB block)
	data_ptr_t buffer1;
	auto result1 = handle->Read(buffer1, 1024 * 1024, 1024 * 1024);
	REQUIRE(result1.IsValid());
	VerifyData(buffer1, 1024 * 1024, 1024 * 1024);
	
	// Second read: 2MB at 1MB - spans two blocks (0-2MB and 2-4MB)
	// Block 0-2MB should hit cache, block 2-4MB should miss
	data_ptr_t buffer2;
	auto result2 = handle->Read(buffer2, 2 * 1024 * 1024, 1024 * 1024);
	REQUIRE(result2.IsValid());
	VerifyData(buffer2, 2 * 1024 * 1024, 1024 * 1024);
	
	// Third read: 1MB at 2.5MB (within 2-4MB block which is now cached)
	data_ptr_t buffer3;
	auto result3 = handle->Read(buffer3, 1024 * 1024, (idx_t)(2.5 * 1024 * 1024));
	REQUIRE(result3.IsValid());
	VerifyData(buffer3, 1024 * 1024, (idx_t)(2.5 * 1024 * 1024));
	
	fs.RemoveFile(test_file);
}

TEST_CASE("CachingFileSystem - Multiple aligned blocks spanning", "[caching_file_system]") {
	DuckDB db(nullptr);
	auto &fs = FileSystem::GetFileSystem(*db.instance);
	auto &cache = ExternalFileCache::Get(*db.instance);
	cache.SetEnabled(true);
	
	auto &config = DBConfig::GetConfig(*db.instance);
	config.options.external_file_cache_read_policy_name = "aligned";
	
	auto test_file = TestCreatePath("cache_test_multiblock.bin");
	CreateTestFile(fs, test_file, 10);
	
	CachingFileSystem caching_fs(fs, *db.instance);
	OpenFileInfo info;
	info.path = test_file;
	auto handle = caching_fs.OpenFile(info, FileFlags::FILE_FLAGS_READ);
	
	// Read 3MB at offset 1MB
	// This spans blocks: 0-2MB, 2-4MB (partial)
	// Aligned read will cover: 0-2MB and 2-4MB
	data_ptr_t buffer1;
	auto result1 = handle->Read(buffer1, 3 * 1024 * 1024, 1024 * 1024);
	REQUIRE(result1.IsValid());
	VerifyData(buffer1, 3 * 1024 * 1024, 1024 * 1024);
	
	// Now both blocks 0-2MB and 2-4MB should be cached
	// Read from first block
	data_ptr_t buffer2;
	auto result2 = handle->Read(buffer2, 512 * 1024, 256 * 1024);
	REQUIRE(result2.IsValid());
	VerifyData(buffer2, 512 * 1024, 256 * 1024);
	
	// Read from second block
	data_ptr_t buffer3;
	auto result3 = handle->Read(buffer3, 512 * 1024, (idx_t)(2.5 * 1024 * 1024));
	REQUIRE(result3.IsValid());
	VerifyData(buffer3, 512 * 1024, (idx_t)(2.5 * 1024 * 1024));
	
	fs.RemoveFile(test_file);
}

TEST_CASE("CachingFileSystem - Concurrent aligned reads duplicate prevention", "[caching_file_system]") {
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

TEST_CASE("CachingFileSystem - Mixed aligned and cached blocks", "[caching_file_system]") {
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

TEST_CASE("CachingFileSystem - Unaligned vs aligned policy comparison", "[caching_file_system]") {
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
	
	// Clear cache
	cache.SetEnabled(false);
	cache.SetEnabled(true);
	
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
	
	// Disable and re-enable cache to clear
	cache.SetEnabled(false);
	cache.SetEnabled(true);
	
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

