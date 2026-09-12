#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

struct LegacyCardinalityFunction {
	struct GlobalState : public GlobalTableFunctionState {
		bool done = false;

		idx_t MaxThreads() const override {
			return 1;
		}
	};

	struct LocalState : public LocalTableFunctionState {
		vector<column_t> column_ids;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
	                                     vector<Identifier> &names) {
		names = {"col_a", "col_b"};
		return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
		return make_uniq<TableFunctionData>();
	}

	static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<GlobalState>();
	}

	static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &, TableFunctionInitInput &input,
	                                                     GlobalTableFunctionState *) {
		auto result = make_uniq<LocalState>();
		result->column_ids = input.column_ids;
		return std::move(result);
	}

	static void Scan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
		auto &global_state = input.global_state->Cast<GlobalState>();
		auto &local_state = input.local_state->Cast<LocalState>();
		if (global_state.done) {
			return;
		}
		global_state.done = true;

		for (idx_t col_idx = 0; col_idx < local_state.column_ids.size(); col_idx++) {
			auto &vector = output.data[col_idx];
			if (local_state.column_ids[col_idx] == 0) {
				auto data = FlatVector::GetDataMutable<string_t>(vector);
				for (idx_t row_idx = 0; row_idx < 9; row_idx++) {
					data[row_idx] = StringVector::AddString(vector, "row" + std::to_string(row_idx));
				}
			} else {
				for (idx_t row_idx = 0; row_idx < 9; row_idx++) {
					FlatVector::SetNull(vector, row_idx, true);
				}
			}
		}
		// Legacy table functions only set the DataChunk count, leaving the vector sizes at zero.
		output.SetCardinalityUnsafe(9);
	}

	static void Register(Connection &con) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function("legacy_cardinality_rows", {}, Scan, Bind, InitGlobal, InitLocal);
		function.projection_pushdown = true;
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

static void CheckCount(Connection &con, const string &filter, int64_t expected) {
	auto result = con.Query("SELECT count(*) FROM legacy_cardinality_rows() WHERE " + filter);
	REQUIRE(!result->HasError());
	REQUIRE(CHECK_COLUMN(result, 0, {expected}));
}

} // namespace

TEST_CASE("NULL filters support table functions using legacy cardinality", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	LegacyCardinalityFunction::Register(con);

	CheckCount(con, "col_b IS NULL", 9);
	CheckCount(con, "col_b IS NOT NULL", 0);
	CheckCount(con, "col_a IS NOT NULL", 9);
	CheckCount(con, "col_a IS NULL", 0);
	CheckCount(con, "col_a = 'row3'", 1);
}
