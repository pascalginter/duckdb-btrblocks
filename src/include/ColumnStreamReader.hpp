#pragma once

#include <arrow/api.h>

#include <arrow/ColumnReadState.hpp>
#include <compression/Datablock.hpp>
#include <compression/BtrReader.hpp>
//--------------------------------------------------------------------------------------------------
class ColumnStreamReader {
  ColumnReadState state;
  int last_chunk = -1;
public:
  ColumnStreamReader(std::string directory, const FileMetadata* file_metadata, int column_i) :
     state(file_metadata, column_i, -1, directory){}
  ::arrow::Status Read(int chunk_i, std::shared_ptr<::arrow::Array>* array){
    if (chunk_i < last_chunk) {
      state.reset();
    }
    state.advance(chunk_i);
    auto result = state.decompressCurrentChunk();
    if (result.ok()){
      *array = result.ValueOrDie();
      last_chunk = chunk_i;
      return ::arrow::Status::OK();
    }
    array = nullptr;
    return result.status();
  }
};
//--------------------------------------------------------------------------------------------------