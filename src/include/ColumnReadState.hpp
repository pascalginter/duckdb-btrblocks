 #pragma once
#include <arrow/api.h>
//--------------------------------------------------------------------------------------------------
#include <compression/Datablock.hpp>
#include <compression/BtrReader.hpp>
#include <duckdb.hpp>
//--------------------------------------------------------------------------------------------------
class ColumnReadState {
	  duckdb::FileSystem& fs;

    int global_chunk_i = -1;
    int chunk_i = -1;
    int part_i = -1;

    const btrblocks::ColumnInfo& column_info;
    btrblocks::ColumnPartInfo part_info;
    const btrblocks::FileMetadata* metadata;
    std::optional<btrblocks::BtrReader> reader = std::nullopt;

    std::string path_prefix;
    std::vector<duckdb::AllocatedData> parts;

    template <typename T>
    ::arrow::Result<std::shared_ptr<::arrow::Array>> decompressNumericChunk();
    ::arrow::Result<std::shared_ptr<::arrow::Array>> decompressStringChunk();

    static char* mmapFile(std::string fileName);
  public:
    ColumnReadState(duckdb::ClientContext& context, const btrblocks::FileMetadata* file_metadata, int column_i, int chunk_i, const std::string& dir);

    ColumnReadState(ColumnReadState&) = delete;
    ColumnReadState(ColumnReadState&&) noexcept = default;

    ::arrow::Result<std::shared_ptr<::arrow::Array>> decompressCurrentChunk();
    void advance(int chunk_i);
    void reset();
  };
//--------------------------------------------------------------------------------------------------