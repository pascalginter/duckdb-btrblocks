#define DUCKDB_EXTENSION_MAIN

#include "quack_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
// OpenSSL linked through vcpkg
#include <btrblocks/arrow/DirectoryReader.hpp>

namespace duckdb {

struct QuackBindData : public FunctionData {
public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<QuackBindData>();
	};

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuackBindData>();
		return false;
	}
};

static unique_ptr<FunctionData> QuackBind(ClientContext &context, TableFunctionBindInput &input,
												  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<QuackBindData>();

	//auto &fs = FileSystem::GetFileSystem(context);
	//auto handle = fs.OpenFile("http://localhost:8000/data", FileFlags::FILE_FLAGS_READ);
	//auto size = handle->GetFileSize();
	//Allocator allocator;
	//auto data = allocator.Allocate(size);

	//handle->Read(data.get(), size);
	//std::cout << "data " << (char*) data.get() << std::endl;

	btrblocks::arrow::DirectoryReader reader("/home/pascal-ginter/code/ActiveDataLake/data/lineitem_btr_sf10");
	std::shared_ptr<::arrow::Schema> schema;
	reader.GetSchema(&schema);

	for (const auto& field : schema->fields()) {
        names.emplace_back(field->name());
		if (field->type() == ::arrow::int32()){
			return_types.emplace_back(LogicalType::INTEGER);
		}else if (field->type() == ::arrow::utf8()){
			return_types.emplace_back(LogicalType::VARCHAR);
		}else{
			std::cout << "unsupported type" << std::endl;
		}
    }
	return std::move(result);
}

struct QuackData : public GlobalTableFunctionState {
public:
	std::shared_ptr<::arrow::Table> table = nullptr;
	btrblocks::arrow::DirectoryReader reader;
	QuackData(std::string path) : reader(path){}
};

unique_ptr<GlobalTableFunctionState> QuackInit(ClientContext& context, TableFunctionInitInput &input) {
	auto result = make_uniq<QuackData>("/home/pascal-ginter/code/ActiveDataLake/data/lineitem_btr_sf10");
	return std::move(result);
}

void QuackTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto& data = data_p.global_state->Cast<QuackData>();
	auto& bind_data = data_p.bind_data->Cast<QuackBindData>();

	if (data.table == nullptr){
		data.reader.ReadTable(&data.table);
	}
	std::cout << output.GetCapacity() << std::endl;
}

static void LoadInternal(DatabaseInstance &instance) {
	TableFunction quack("quack", {}, QuackTableFunction, QuackBind, QuackInit);
	ExtensionUtil::RegisterFunction(instance, quack);
}

void QuackExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string QuackExtension::Name() {
	return "quack";
}

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void quack_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::QuackExtension>();
}

DUCKDB_EXTENSION_API const char *quack_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
