#include "duckdb/planner/binder.hpp"

namespace duckdb {

template <typename T>
map<string, T> order_case_insensitive_map(const case_insensitive_map_t<T> &input_map) {
	return map<string, T>(input_map.begin(), input_map.end());
}

template <typename T>
map<string, T> order_case_insensitive_map(const identifier_map_t<T> &input_map) {
	map<string, T> result;
	for (auto &entry : input_map) {
		result[entry.first.GetIdentifierName()] = entry.second;
	}
	return result;
}

bound_named_parameter_map_t Binder::BindNamedParameters(const named_parameter_type_map_t &types,
                                                        named_parameter_map_t &values,
                                                        QueryErrorContext &error_context,
                                                        const Identifier &func_name) {
	bound_named_parameter_map_t result;
	for (auto &kv : values) {
		auto entry = types.find(kv.first);
		if (entry == types.end()) {
			auto ordered_params = order_case_insensitive_map(types);
			// create a list of named parameters for the error
			string named_params;
			for (auto &kv_ordered_params : ordered_params) {
				named_params += "    ";
				named_params += kv_ordered_params.first;
				named_params += " ";
				named_params += kv_ordered_params.second.GetType().ToString();
				named_params += "\n";
			}
			string error_msg;
			if (named_params.empty()) {
				error_msg = "Function does not accept any named parameters.";
			} else {
				error_msg = "Candidates:\n" + named_params;
			}
			throw BinderException(error_context, "Invalid named parameter \"%s\" for function %s\n%s", kv.first,
			                      func_name.GetIdentifierName(), error_msg);
		}
		const auto &parameter_type = entry->second;
		const auto &type = parameter_type.GetType();
		if (kv.second.IsNull()) {
			if (type.id() != LogicalTypeId::ANY) {
				kv.second = Value(type);
			}
		} else if (type.id() != LogicalTypeId::ANY) {
			if (parameter_type.GetCastPolicy() == NamedParameterCastPolicy::IMPLICIT_CAST) {
				kv.second = kv.second.DefaultCastAs(type);
			} else if (kv.second.type() != type) {
				throw InvalidInputException("Named parameter \"%s\" for function \"%s\" requires type %s, but "
				                            "the argument has type %s",
				                            kv.first, func_name, type, kv.second.type());
			}
		}
		result.emplace(kv.first, BoundNamedParameterValue(kv.second, type, func_name, kv.first));
	}
	return result;
}

} // namespace duckdb
