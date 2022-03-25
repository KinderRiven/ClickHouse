#include "SliceReadBuffer.h"
#include <Common/Exception.h>
#include "SliceManagement.h"

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
            "[init][file:{}][slices_id:{}][num_block:{}][uncompressed_size:{}][compressed_size:{}][offset:{}]",
            remote_data_file->getFileName(),
            i,
            tmp_slice.num_blocks,
            tmp_slice.uncompressed_size,
            tmp_slice.compressed_size,
            tmp_slice.offset_in_compressed_file);
        i++;
    }
    swap(*remote_data_file);
}


String SliceReadBuffer::getSliceName(const String & path, int slice_id)
{
    String slice_name = path + ".slice" + "_" + std::to_string(slice_id);
    for(size_t i = 0; i < slice_name.length(); i++) {
        if (slice_name[i] == '/') {
            slice_name[i] = '_';
        }
    }
    return slice_name;
}


int SliceReadBuffer::getSliceFromOffset(off_t off)
{
    int slice_id = 0;
    for (auto & tmp_slice : vec_slice)
    {
        if ((tmp_slice.offset_in_compressed_file <= static_cast<size_t>(off))
            && (static_cast<size_t>(off) < (tmp_slice.offset_in_compressed_file + tmp_slice.compressed_size)))
        {
            return slice_id;
        }
        slice_id++;
    }
    return -1;
}


off_t SliceReadBuffer::switchToSlice(int slice_id, off_t off)
{
    /// 1.TODO check slice_map<slice_id, file>
    /// 2.TODO donwnload slice file
    /// 3.TODO switch to new slice file
    current_slice = slice_id;
    String file_name = remote_data_file->getFileName();
    String slice_name = getSliceName(file_name, current_slice);
    auto metadata = SliceManagement::instance().acquireDownloadSlice(slice_name);
    metadata->mutex.lock();
    LOG_TRACE(trace_log, "[switch][file:{}][slice:{}][offset:{}]", slice_name, slice_id, off);
    metadata->mutex.unlock();
    return off;
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
    offset_in_compressed_file = off;
    int new_slice_id = getSliceFromOffset(offset_in_compressed_file);
    /// There is no matching slice. It may be switched to an offset exceeding the file size.
    if (new_slice_id == -1)
    {
        return -1;
    }
    /// We need to switch to new slice.
    else if (new_slice_id != current_slice)
    {
        switchToSlice(new_slice_id, offset_in_compressed_file);
    }

    swap(*remote_data_file);
    auto result = remote_data_file->seek(off, whence);
    swap(*remote_data_file);
    return result;
}


bool SliceReadBuffer::nextImpl()
{
    offset_in_compressed_file += offset();
    ///
    /// When running here, we may perform the operation of moving the pointer such as
    /// seek(), thus we need to judge whether we need to switch the partition every time.
    ///
    int new_slice_id = getSliceFromOffset(offset_in_compressed_file);
    /// There is no matching slice. It may be switched to an offset exceeding the file size.
    if (new_slice_id == -1)
    {
        LOG_TRACE(
            trace_log,
            "[nextImpl][file:{}][slice_id:{}][offset:{}]",
            remote_data_file->getFileName(),
            current_slice,
            offset_in_compressed_file);
    }
    /// We need to switch to new slice.
    else if (new_slice_id != current_slice)
    {
        switchToSlice(new_slice_id, offset_in_compressed_file);
    }

    swap(*remote_data_file);
    auto result = remote_data_file->next();
    swap(*remote_data_file);
    return result;
}

};
