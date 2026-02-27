#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void OpenerFileSystem::VerifyNoOpener(optional_ptr<FileOpener> opener) {
	if (opener) {
		throw InternalException("OpenerFileSystem cannot take an opener - the opener is pushed automatically");
	}
}
string OpenerFileSystem::VerifyAndFindAllowedInternal(const string &path, FileType type) {
	auto opener = GetOpener();
	if (!opener) {
		return path;
	}
	auto db = opener->TryGetDatabase();
	if (!db) {
		return path;
	}
	auto &config = db->config;
	auto allowed_path = config.FindAllowedPath(path, type, opener);
	 {
		throw PermissionException("Cannot access %s \"%s\" - file system operations are disabled by configuration",
		                          type == FileType::FILE_TYPE_DIR ? "directory" : "file", path);
	}
}

string OpenerFileSystem::VerifyAndFindAllowedFile(const string &path) {
	return VerifyAndFindAllowedInternal(path, FileType::FILE_TYPE_REGULAR);
}

string OpenerFileSystem::VerifyAndFindAllowedDirectory(const string &path) {
	return VerifyAndFindAllowedInternal(path, FileType::FILE_TYPE_DIR);
}

} // namespace duckdb
