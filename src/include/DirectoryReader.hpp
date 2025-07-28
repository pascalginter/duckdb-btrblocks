#pragma once
// ------------------------------------------------------------------------------
// Own headers
// -------------------------------------------------------------------------------------
#include <arrow/api.h>
// -------------------------------------------------------------------------------------
#include <filesystem>
#include <vector>
#include "compression/Datablock.hpp"

#include <compression/BtrReader.hpp>
#include <duckdb.hpp>
// -------------------------------------------------------------------------------------

class DirectoryReader {
  duckdb::ClientContext& context;
  std::filesystem::path btr_dir;

  std::shared_ptr<::arrow::Schema> schema;

  duckdb::AllocatedData raw_file_metadata;
  const btrblocks::FileMetadata* file_metadata;

public:
  [[nodiscard]] std::vector<int> get_all_row_group_indices() const;
  [[nodiscard]] std::vector<int> get_all_column_indices() const;

  explicit DirectoryReader(duckdb::ClientContext& context, std::string dir);

  [[nodiscard]] const btrblocks::FileMetadata* metadata() const { return file_metadata; }

  ::arrow::Status GetSchema(std::shared_ptr<::arrow::Schema>* out);
  ::arrow::Status ReadColumn(int i, std::shared_ptr<::arrow::ChunkedArray>* out);

  ::arrow::Status GetRecordBatchReader(std::shared_ptr<::arrow::RecordBatchReader>* out);
  ::arrow::Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                       std::shared_ptr<::arrow::RecordBatchReader>* out);
  ::arrow::Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                       const std::vector<int>& column_indices,
                                       std::shared_ptr<::arrow::RecordBatchReader>* out);

  // Not implemented method: GetRecordBatchGenerator
  ::arrow::Status ReadTable(std::shared_ptr<::arrow::Table>* out);
  ::arrow::Status ReadTable(const std::vector<int>& column_indices,
                            std::shared_ptr<::arrow::Table>* out);

  // Not implemented method: ScanContents
  // Not implemented method: RowGroupReader

  int num_row_groups() const;

  // Not implemented method set_use_threads
  // Not implemented method set_batch_size
};
// -------------------------------------------------------------------------------------

