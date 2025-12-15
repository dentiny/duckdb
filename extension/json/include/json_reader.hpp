//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "json_reader_options.hpp"
#include "duckdb/common/mutex.hpp"
#include "json_common.hpp"
#include "json_enums.hpp"
#include "duckdb/storage/caching_file_system_wrapper.hpp"

namespace duckdb {
struct JSONScanGlobalState;
class JSONReader;

struct JSONString {
public:
	JSONString() {
	}
	JSONString(const char *pointer_p, idx_t size_p) : pointer(pointer_p), size(size_p) {
	}

	const char *pointer;
	idx_t size;

public:
	string ToString() {
		return string(pointer, size);
	}

	const char &operator[](size_t i) const {
		return pointer[i];
	}
};

enum class JSONFileReadType { SCAN_ENTIRE_FILE, SCAN_PARTIAL };

struct JSONReaderScanState {
	explicit JSONReaderScanState(ClientContext &context, Allocator &global_allocator,
	                             idx_t reconstruct_buffer_capacity);

	FileSystem &fs;
	Allocator &global_allocator;
	//! Thread-local allocator
	JSONAllocator allocator;
	idx_t buffer_capacity;
	bool initialized = false;
	// if we have a buffer already - this is our buffer index
	optional_idx buffer_index;
	//! Whether or not we are scanning the entire file
	//! If we are scanning the entire file we don't share reads between threads and just read the file until we are done
	JSONFileReadType file_read_type = JSONFileReadType::SCAN_PARTIAL;
	// Data for reading (if we have postponed reading)
	//! Buffer (if we have one)
	AllocatedData read_buffer;
	bool needs_to_read = false;
	idx_t request_size;
	idx_t read_position;
	idx_t read_size;
	//! Current scan data
	idx_t scan_count = 0;
	JSONString units[STANDARD_VECTOR_SIZE];
	yyjson_val *values[STANDARD_VECTOR_SIZE];
	//! Current buffer read info
	optional_ptr<JSONReader> current_reader;
	char *buffer_ptr = nullptr;
	idx_t buffer_size = 0;
	idx_t buffer_offset = 0;
	idx_t prev_buffer_remainder = 0;
	idx_t prev_buffer_offset = 0;
	idx_t lines_or_objects_in_buffer = 0;
	//! Whether this is the first time scanning this buffer
	bool is_first_scan = false;
	//! Whether this is the last batch of the file
	bool is_last = false;
	//! Buffer to reconstruct split values
	optional_idx batch_index;

	//! For some filesystems (e.g. S3), using a filehandle per thread increases performance
	unique_ptr<FileHandle> thread_local_filehandle;

public:
	//! Reset for parsing the next batch of JSON from the current buffer
	void ResetForNextParse();
	//! Reset state for reading the next buffer
	void ResetForNextBuffer();
};

struct JSONError {
	idx_t buf_index;
	idx_t line_or_object_in_buf;
	string error_msg;
};

class JSONReader : public BaseFileReader {
public:
	JSONReader(ClientContext &context, JSONReaderOptions options, OpenFileInfo file);

	void OpenJSONFile();
	void CloseHandle();
	void Reset();

	bool HasFileHandle() const;
	bool IsOpen() const;
	bool IsInitialized() const {
		return initialized;
	}

	JSONReaderOptions &GetOptions();

	JSONFormat GetFormat() const;
	void SetFormat(JSONFormat format);

	JSONRecordType GetRecordType() const;
	void SetRecordType(JSONRecordType type);

	const string &GetFileName() const;

public:
	string GetReaderType() const override {
		return "JSON";
	}

	void PrepareReader(ClientContext &context, GlobalTableFunctionState &) override;
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;
	void FinishFile(ClientContext &context, GlobalTableFunctionState &gstate_p) override;
	double GetProgressInFile(ClientContext &context) override;

public:
	//! Get a new buffer index (must hold the lock)
	idx_t GetBufferIndex();
	//! Records a parse error in the specified buffer
	void AddParseError(JSONReaderScanState &scan_state, idx_t line_or_object_in_buf, yyjson_read_err &err,
	                   const string &extra = "");
	//! Records a transform error in the specified buffer
	void AddTransformError(JSONReaderScanState &scan_state, idx_t object_index, const string &error_message);
	//! Whether this reader has thrown if an error has occurred
	bool HasThrown();

	void Initialize(Allocator &allocator, idx_t buffer_size);
	bool InitializeScan(JSONReaderScanState &state, JSONFileReadType file_read_type);
	void ParseJSON(JSONReaderScanState &scan_state, char *const json_start, const idx_t json_size,
	               const idx_t remaining);
	void ParseNextChunk(JSONReaderScanState &scan_state);
	idx_t Scan(JSONReaderScanState &scan_state);
	bool ReadNextBuffer(JSONReaderScanState &scan_state);
	bool PrepareBufferForRead(JSONReaderScanState &scan_state);

	//! Scan progress
	double GetProgress() const;

private:
	void SkipOverArrayStart(JSONReaderScanState &scan_state);
	void AutoDetect(Allocator &allocator, idx_t buffer_size);
	bool CopyRemainderFromPreviousBuffer(JSONReaderScanState &scan_state);
	void FinalizeBufferInternal(JSONReaderScanState &scan_state);
	void PrepareForReadInternal(JSONReaderScanState &scan_state);
	void PrepareForScan(JSONReaderScanState &scan_state);
	bool PrepareBufferSeek(JSONReaderScanState &scan_state);
	void ReadNextBufferSeek(JSONReaderScanState &scan_state);
	bool ReadNextBufferNoSeek(JSONReaderScanState &scan_state);
	void FinalizeBuffer(JSONReaderScanState &scan_state);

	void ThrowObjectSizeError(const idx_t object_size);

private:
	//! Add an error to the buffer - requires the lock to be held
	void AddError(idx_t buf_index, idx_t line_or_object_in_buf, const string &error_msg);
	//! Throw errors if possible - requires the lock to be held
	void ThrowErrorsIfPossible();
	//! Try to get the line number - requires the lock to be held
	optional_idx TryGetLineNumber(idx_t buf_index, idx_t line_or_object_in_buf);

private:
	ClientContext &context;
	JSONReaderOptions options;

	//! Caching file system wrapper
	CachingFileSystemWrapper caching_fs;
	//! File handle (wrapped with caching)
	unique_ptr<CachingFileHandleWrapper> file_handle;

	//! Whether or not the reader has been initialized
	bool initialized;
	//! Next buffer index within the file
	idx_t next_buffer_index;

	//! Line count per buffer
	vector<int64_t> buffer_line_or_object_counts;
	//! Whether any of the reading threads has thrown an error
	bool thrown;

	//! If we have auto-detected, this is the buffer read by the auto-detection
	AllocatedData auto_detect_data;
	idx_t auto_detect_data_size = 0;

	//! The first error we found in the file (if any)
	unique_ptr<JSONError> error;

	//! File properties (cached from file handle)
	bool can_seek;
	idx_t file_size;
	atomic<idx_t> read_position;

public:
	mutable mutex lock;
};

} // namespace duckdb
