#include "SliceReadBuffer.h"

namespace DB
{

SliceReadBuffer::SliceReadBuffer(
    std::unique_ptr<ReadBufferFromFileBase> slice_file_,
    std::shared_ptr<IDisk> local_cache_,
    std::shared_ptr<IDisk> remote_cache_,
    std::unique_ptr<ReadBufferFromFileBase> remote_data_file_)
    : slice_file(std::move(slice_file_))
    , local_cache(std::move(local_cache_))
    , remote_cache(std::move(remote_cache_))
    , remote_data_file(std::move(remote_data_file_))
{
    int i = 0;
    Slice tmp_slice;
    while (!slice_file->eof())
    {
        slice_file->read(reinterpret_cast<char *>(&tmp_slice), sizeof(tmp_slice));
        vec_slice.push_back(tmp_slice);
        LOG_TRACE(
            trace_log,
            "[slices_id:{}][num_block:{}][uncompressed_size:{}][compressed_size:{}][offset:{}]",
            i,
            tmp_slice.num_blocks,
            tmp_slice.uncompressed_size,
            tmp_slice.compressed_size,
            tmp_slice.offset_in_compressed_file);
        i++;
    }
    swap(*remote_data_file);
}

off_t SliceReadBuffer::getPosition()
{
    swap(*remote_data_file);
    auto position = remote_data_file->getPosition();
    swap(*remote_data_file);
    return position;
}

off_t SliceReadBuffer::seek(off_t off, int whence)
{
    swap(*remote_data_file);
    auto result = remote_data_file->seek(off, whence);
    swap(*remote_data_file);
    return result;
}

bool SliceReadBuffer::nextImpl()
{
    swap(*remote_data_file);
    auto result = remote_data_file->next();
    swap(*remote_data_file);
    return result;
}

};
