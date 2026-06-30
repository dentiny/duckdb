//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/data_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/table_index_list.hpp"

namespace duckdb {
class AttachedDatabase;
class DatabaseInstance;
class TableIOManager;
class RowGroupCollection;

struct DataTableInfo {
	friend class DataTable;

public:
	DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, Identifier schema,
	              Identifier table);

	//! Bind unknown indexes throwing an exception if binding fails.
	//! Only binds the specified index type, or all, if nullptr.
	void BindIndexes(ClientContext &context, const char *index_type = nullptr);

	//! Whether or not the table is temporary
	bool IsTemporary() const;

	AttachedDatabase &GetDB() const {
		return db;
	}

	TableIOManager &GetIOManager() {
		return *table_io_manager;
	}

	TableIndexList &GetIndexes() {
		return indexes;
	}
	//! Find and move out an IndexStorageInfo by name from the stored collection.
	IndexStorageInfo ExtractIndexStorageInfo(const Identifier &name);
	unique_ptr<StorageLockKey> GetSharedLock() {
		return checkpoint_lock.GetSharedLock();
	}
	bool AppendRequiresNewRowGroup(RowGroupCollection &collection, transaction_t checkpoint_id);
	optional_idx CheckpointRowGroupCount(const CheckpointOptions &options) const;
	void VerifyIndexBuffers();

	Identifier GetSchemaName();
	Identifier GetTableName();
	void SetTableName(Identifier name);

	//! Row-id boundary of an in-progress concurrent index build (base-relative, i.e. in GetNextRowId() space).
	//! The create-index scan only reads rows < boundary; rows >= boundary are buffered into the placeholder
	//! index and replayed at finalize. Returns INVALID_INDEX when no build is in progress.
	idx_t GetIndexBuildScanBoundary() const {
		return index_build_scan_boundary.load();
	}

private:
	//! The database instance of the table
	AttachedDatabase &db;
	//! The table IO manager
	shared_ptr<TableIOManager> table_io_manager;
	//! Lock for modifying the name
	mutex name_lock;
	//! The schema of the table
	Identifier schema;
	//! The name of the table
	Identifier table;
	//! The physical list of indexes of this table
	TableIndexList indexes;
	//! Index storage information of the indexes created by this table
	vector<IndexStorageInfo> index_storage_infos;
	//! Lock held while checkpointing
	StorageLock checkpoint_lock;
	//! The last seen checkpoint while doing a concurrent operation, if any
	optional_idx last_seen_checkpoint;
	//! The amount of row groups the checkpoint is processing
	optional_idx checkpoint_row_group_count;
	//! Row-id boundary of an in-progress concurrent index build (see GetIndexBuildScanBoundary).
	atomic<idx_t> index_build_scan_boundary {DConstants::INVALID_INDEX};
};

} // namespace duckdb
