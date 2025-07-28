#pragma once

#include <arrow/api.h>

#include "ColumnReadState.hpp"
#include <compression/Datablock.hpp>
#include <compression/BtrReader.hpp>
#include <duckdb.hpp>
//--------------------------------------------------------------------------------------------------
class RecordBatchStreamReader final : public ::arrow::RecordBatchReader {
  //--------------------------------------------------------------------------------------------------
  std::string dir;
  std::vector<int> chunks;
  std::shared_ptr<::arrow::Schema> schema_;
  std::vector<ColumnReadState> read_states;
  int current_chunk = -1;
  int chunk_index = -1;

public:
  RecordBatchStreamReader(duckdb::ClientContext& context, std::string directory, const btrblocks::FileMetadata* file_metadata,
    const std::vector<int>& row_group_indices, const std::vector<int>& column_indices);
  ~RecordBatchStreamReader() override = default;
  [[nodiscard]] std::shared_ptr<::arrow::Schema> schema() const override { return schema_; };
  ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* record_batch) override;
  inline ::arrow::Status Close() override;
};
//--------------------------------------------------------------------------------------------------