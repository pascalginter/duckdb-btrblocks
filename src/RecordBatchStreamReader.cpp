//--------------------------------------------------------------------------------------------------
#include "RecordBatchStreamReader.hpp"
#include "arrow/ChunkToArrowArrayConverter.hpp"
#include "arrow/Util.hpp"

#include <common/Utils.hpp>
//--------------------------------------------------------------------------------------------------
RecordBatchStreamReader::RecordBatchStreamReader(
  duckdb::ClientContext& context,
  std::string directory,
  const btrblocks::FileMetadata* file_metadata,
  const std::vector<int>& row_group_indices,
  const std::vector<int>& column_indices) :
      dir(std::move(directory)), chunks(row_group_indices) {
  auto tempSchema = btrblocks::arrow::util::CreateSchemaFromFileMetadata(file_metadata);

  read_states.reserve(column_indices.size());
  std::vector<std::shared_ptr<::arrow::Field>> fields;
  for (int column_index : column_indices) {
    read_states.emplace_back(context, file_metadata, column_index, row_group_indices[0], dir);
    fields.push_back(tempSchema->field(column_index));
  }
  schema_ = ::arrow::schema(fields);
}
//--------------------------------------------------------------------------------------------------
::arrow::Status RecordBatchStreamReader::ReadNext(
    std::shared_ptr<::arrow::RecordBatch>* record_batch) {
  if (current_chunk == chunks.back()) {
    *record_batch = nullptr;
    return ::arrow::Status::OK();
  }
  current_chunk = chunks[++chunk_index];
  for (auto& read_state : read_states) read_state.advance(current_chunk);
  ::arrow::ArrayVector arrays(schema_->num_fields());
  for (btrblocks::units::u32 i = 0; i != schema_->num_fields(); i++) {
    arrays[i] = read_states[i].decompressCurrentChunk().ValueOrDie();
  }
  *record_batch = ::arrow::RecordBatch::Make(schema_, arrays[0]->length(), arrays);
  return ::arrow::Status::OK();
}
//--------------------------------------------------------------------------------------------------
::arrow::Status RecordBatchStreamReader::Close() {
  return ::arrow::Status::OK();
}
//--------------------------------------------------------------------------------------------------
