#include "DirectoryReader.hpp"
#include "RecordBatchStreamReader.hpp"
#include "common/Utils.hpp"
#include "compression/BtrReader.hpp"
#include "compression/Datablock.hpp"

#include <numeric>

DirectoryReader::DirectoryReader(duckdb::ClientContext& context, std::string dir) :
	context(context), btr_dir(dir){
  // Read the metadata
  std::cout << "instantiating reader" << std::endl;
  {
    auto metadata_path = btr_dir / "metadata";
	duckdb::FileSystem& fs = duckdb::FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(metadata_path, duckdb::FileFlags::FILE_FLAGS_READ);
  raw_file_metadata = duckdb::Allocator::DefaultAllocator().Allocate(handle->GetFileSize());
	handle->Read(raw_file_metadata.get(), handle->GetFileSize());
    file_metadata = btrblocks::FileMetadata::fromMemory((char*)raw_file_metadata.get());
  }

  ::arrow::FieldVector fields;
  for (btrblocks::units::u32 i = 0; i != file_metadata->num_columns; i++){
    std::shared_ptr<::arrow::DataType> type;
    switch (file_metadata->columns[i].type){
      case btrblocks::ColumnType::INTEGER:
        type = ::arrow::int32();
        break;
      case btrblocks::ColumnType::DOUBLE:
        type = ::arrow::float64();
        break;
      case btrblocks::ColumnType::STRING:
        type = ::arrow::utf8();
        break;
      default:
        std::cout << i << " " << static_cast<int>(file_metadata->columns[i].type) << std::endl;
        exit(1);
    }
    fields.push_back(::arrow::field("c_" + std::to_string(i), type));
  }
  schema = ::arrow::schema(fields);
  std::cout << "instantiating reader done" << std::endl;

}

::arrow::Status DirectoryReader::GetSchema(std::shared_ptr<::arrow::Schema>* out) {
  *out = schema;
  return ::arrow::Status::OK();
}

std::vector<int> DirectoryReader::get_all_row_group_indices() const {
  std::vector<int> row_group_indices(num_row_groups());
  std::iota(row_group_indices.begin(), row_group_indices.end(), 0);
  return row_group_indices;
}

std::vector<int> DirectoryReader::get_all_column_indices() const {
  std::vector<int> column_indices;
  for (btrblocks::units::u32 i=0; i!=file_metadata->num_columns; i++){
    if (file_metadata->columns[i].type == btrblocks::ColumnType::INTEGER
        || file_metadata->columns[i].type == btrblocks::ColumnType::DOUBLE
        || file_metadata->columns[i].type == btrblocks::ColumnType::STRING){
      column_indices.push_back(i);
    }
  }
  return column_indices;
}

::arrow::Status DirectoryReader::GetRecordBatchReader(std::shared_ptr<::arrow::RecordBatchReader>* out) {
  return GetRecordBatchReader(get_all_row_group_indices(), out);
}

::arrow::Status DirectoryReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                     std::shared_ptr<::arrow::RecordBatchReader>* out){
  return GetRecordBatchReader(row_group_indices, get_all_column_indices(), out);
}

::arrow::Status DirectoryReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                     const std::vector<int>& column_indices,
                                     std::shared_ptr<::arrow::RecordBatchReader>* out){
  *out = std::make_shared<RecordBatchStreamReader>(
    context, std::string(btr_dir.c_str()), file_metadata, row_group_indices, column_indices);
  return ::arrow::Status::OK();
}

::arrow::Status DirectoryReader::ReadTable(std::shared_ptr<::arrow::Table>* out){
  return ReadTable(get_all_column_indices(), out);
}

::arrow::Status DirectoryReader::ReadTable(const std::vector<int>& column_indices,
                          std::shared_ptr<::arrow::Table>* out){
  std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;
  ARROW_RETURN_NOT_OK(GetRecordBatchReader(get_all_row_group_indices(), column_indices, &recordBatchReader));
  auto result = recordBatchReader->ToTable();
  if (result.ok()) *out = result.ValueOrDie();
  return result.status();
}

int DirectoryReader::num_row_groups() const {
  return file_metadata->num_chunks;
}

