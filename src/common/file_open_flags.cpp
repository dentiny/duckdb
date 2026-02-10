#include "duckdb/common/file_open_flags.hpp"

namespace duckdb {

const FileOpenFlags FileFlags::FILE_FLAGS_READ = FileOpenFlags(FileOpenFlags::FILE_FLAGS_READ);
const FileOpenFlags FileFlags::FILE_FLAGS_WRITE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_WRITE);
const FileOpenFlags FileFlags::FILE_FLAGS_DIRECT_IO = FileOpenFlags(FileOpenFlags::FILE_FLAGS_DIRECT_IO);
const FileOpenFlags FileFlags::FILE_FLAGS_FILE_CREATE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_FILE_CREATE);
const FileOpenFlags FileFlags::FILE_FLAGS_FILE_CREATE_NEW = FileOpenFlags(FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
const FileOpenFlags FileFlags::FILE_FLAGS_APPEND = FileOpenFlags(FileOpenFlags::FILE_FLAGS_APPEND);
const FileOpenFlags FileFlags::FILE_FLAGS_PRIVATE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_PRIVATE);
const FileOpenFlags FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS = FileOpenFlags(FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
const FileOpenFlags FileFlags::FILE_FLAGS_PARALLEL_ACCESS = FileOpenFlags(FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
const FileOpenFlags FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_EXCLUSIVE_CREATE);
const FileOpenFlags FileFlags::FILE_FLAGS_NULL_IF_EXISTS = FileOpenFlags(FileOpenFlags::FILE_FLAGS_NULL_IF_EXISTS);
const FileOpenFlags FileFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS = FileOpenFlags(FileOpenFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS);
const FileOpenFlags FileFlags::FILE_FLAGS_DISABLE_LOGGING = FileOpenFlags(FileOpenFlags::FILE_FLAGS_DISABLE_LOGGING);
const FileOpenFlags FileFlags::FILE_FLAGS_ENABLE_EXTENSION_INSTALL = FileOpenFlags(FileOpenFlags::FILE_FLAGS_ENABLE_EXTENSION_INSTALL);

} // namespace duckdb

