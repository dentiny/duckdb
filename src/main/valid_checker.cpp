#include "duckdb/main/valid_checker.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

ValidChecker::ValidChecker(DatabaseInstance &db) : is_invalidated(false), db(db) {
}

void ValidChecker::Invalidate(string error) {
	annotated_lock_guard<annotated_mutex> l(invalidate_lock);
	is_invalidated = true;
	invalidated_msg = std::move(error);
}

bool ValidChecker::IsInvalidated() {
	if (Settings::Get<DisableDatabaseInvalidationSetting>(db)) {
		return false;
	}
	return is_invalidated;
}

string ValidChecker::InvalidatedMessage() {
	annotated_lock_guard<annotated_mutex> l(invalidate_lock);
	return invalidated_msg;
}

} // namespace duckdb
