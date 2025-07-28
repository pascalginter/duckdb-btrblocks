#include "ColumnReadState.hpp"
#include "arrow/ChunkToArrowArrayConverter.hpp"
#include "common/Utils.hpp"
//--------------------------------------------------------------------------------------------------
ColumnReadState::ColumnReadState(
  duckdb::ClientContext& context,
  const btrblocks::FileMetadata* file_metadata,
  int column_i,
  int chunk_i,
  const std::string& dir) :
    fs(duckdb::FileSystem::GetFileSystem(context)),
	column_info(file_metadata->columns[column_i]),
    part_info(file_metadata->parts[column_info.part_offset]),
    metadata(file_metadata),
    path_prefix(dir + "/" + "column" + std::to_string(column_i) + "_part"){
  parts.reserve(column_info.num_parts);
  for (int i=0; i!=column_info.num_parts; i++) {
    std::string fileName = path_prefix + std::to_string(i);
    auto handle = fs.OpenFile(fileName, duckdb::FileFlags::FILE_FLAGS_READ);
    auto data = duckdb::Allocator::DefaultAllocator().Allocate(handle->GetFileSize());
    parts.emplace_back(std::move(data));
    handle->Read(parts[i].get(), handle->GetFileSize());
  }
	advance(chunk_i);
}
//--------------------------------------------------------------------------------------------------
template <typename T>
::arrow::Result<std::shared_ptr<::arrow::Array>> ColumnReadState::decompressNumericChunk(){
  auto outputBuffer = ::arrow::AllocateBuffer(reader->getDecompressedSize(chunk_i), ::arrow::default_memory_pool()).ValueOrDie();
  const bool requiresCopy = reader->readColumn(outputBuffer->mutable_data(), chunk_i);
  assert(!requiresCopy);
  const btrblocks::units::u32 tupleCount = reader->getTupleCount(chunk_i);
  auto* bitmap = reader->getBitmap(chunk_i);
  return btrblocks::arrow::ChunkToArrowArrayConverter::convertNumericChunk<T>(
    std::move(outputBuffer), tupleCount, bitmap);
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ColumnReadState::decompressStringChunk(){
  btrblocks::units::u32 tupleCount = reader->getTupleCount(chunk_i);
  const auto bitmap = reader->getBitmap(chunk_i);
  auto outputBuffer = ::arrow::AllocateBuffer(reader->getDecompressedSize(chunk_i), ::arrow::default_memory_pool()).ValueOrDie();
  const bool requiresCopy = reader->readColumn(outputBuffer->mutable_data(), chunk_i);
  if (requiresCopy) {
    return btrblocks::arrow::ChunkToArrowArrayConverter::convertStringChunkCopy(std::move(outputBuffer), tupleCount, bitmap);
  } else {
    return btrblocks::arrow::ChunkToArrowArrayConverter::convertStringChunkNoCopy(std::move(outputBuffer), tupleCount, bitmap);
  }
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ColumnReadState::decompressCurrentChunk() {
  switch (column_info.type) {
    case btrblocks::ColumnType::INTEGER:
      return decompressNumericChunk<::arrow::Int32Type>();
    case btrblocks::ColumnType::DOUBLE:
      return decompressNumericChunk<::arrow::DoubleType>();
    case btrblocks::ColumnType::STRING:
      return decompressStringChunk();
    default:
      return ::arrow::Status::Invalid("Type can not be converted");
  }
}
//--------------------------------------------------------------------------------------------------
void ColumnReadState::advance(int next_chunk_i) {
  bool part_i_changed = false;
  while (global_chunk_i != next_chunk_i) {
    assert(global_chunk_i < next_chunk_i);
    if (part_i == -1 || part_info.num_chunks == ++chunk_i) {
      part_info = metadata->parts[column_info.part_offset + ++part_i];
      part_i_changed = true;
      assert(part_i < column_info.num_parts);
      chunk_i = 0;
    }
    global_chunk_i++;
  }
  if (part_i_changed) {
    reader = btrblocks::BtrReader(parts[part_i].get());
  }
}
//--------------------------------------------------------------------------------------------------
void ColumnReadState::reset() {
  part_i = -1;
  chunk_i = -1;
  global_chunk_i = -1;
}
//--------------------------------------------------------------------------------------------------
