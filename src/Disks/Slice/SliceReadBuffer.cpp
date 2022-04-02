#include "SliceReadBuffer.h"
#include <IO/copyData.h>
#include <Common/Exception.h>
#include "SliceManagement.h"

namespace DB
{

SliceReadBuffer::SliceReadBuffer(
    std::unique_ptr<ReadBufferFromFileBase> slice_file_,
    std::shared_ptr<DiskLocal> local_cache_,
    std::shared_ptr<IDisk> remote_cache_,
    std::unique_ptr<ReadBufferFromFileBase> remote_data_file_,
    const ReadSettings & settings_,
    std::optional<size_t> size_)
    : slice_file(std::move(slice_file_))
    , local_cache(std::move(local_cache_))
    , remote_cache(std::move(remote_cache_))
    , remote_data_file(std::move(remote_data_file_))
    , read_settings(settings_)
    , read_size(size_)
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
    swap(*remote_data_file); /// start with main data file
    /// hold buffer is remote_data_file,
    /// external buffer is remote_data_file, its buffer is NULL.
}


String SliceReadBuffer::getRemoteSliceName(const String & path, int slice_id)
{
    String slice_name = path + ".slice" + "_" + std::to_string(slice_id);
    for (size_t i = 0; i < slice_name.length(); i++)
    {
        if (slice_name[i] == '/')
        {
            slice_name[i] = '_';
        }
    }
    return slice_name;
}


String SliceReadBuffer::getLocalSlicePath(const String & path, int slice_id)
{
    String slice_path = path + ".slice" + "_" + std::to_string(slice_id);
    return slice_path;
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


void SliceReadBuffer::downloadSliceFile(const String & path, int slice_id)
{
    auto dir_path = directoryPath(path);
    if (!local_cache->exists(dir_path))
    {
        local_cache->createDirectories(dir_path);
    }

    String tmp_path = path + ".tmp";
    auto writer = local_cache->writeFile(tmp_path, read_settings.local_fs_buffer_size, WriteMode::Rewrite);

    off_t old_off = remote_data_file->getPosition();
    LOG_TRACE(
        trace_log,
        "[download_slice_file][start][slice_id:{}][slice_offset:{}][slice_size:{}][path:{}][current_offset:{}]",
        slice_id,
        vec_slice[slice_id].offset_in_compressed_file,
        vec_slice[slice_id].compressed_size,
        path,
        old_off);

    LOG_TRACE(
        trace_log,
        "[download_slice_file][seek][slice_id:{}][slice_offset:{}][slice_size:{}][path:{}][current_offset:{}]",
        slice_id,
        vec_slice[slice_id].offset_in_compressed_file,
        vec_slice[slice_id].compressed_size,
        path,
        old_off);
    remote_data_file->seek(static_cast<off_t>(vec_slice[slice_id].offset_in_compressed_file), SEEK_SET);

    LOG_TRACE(
        trace_log,
        "[download_slice_file][copy][slice_id:{}][slice_offset:{}][slice_size:{}][path:{}][current_offset:{}]",
        slice_id,
        vec_slice[slice_id].offset_in_compressed_file,
        vec_slice[slice_id].compressed_size,
        path,
        old_off);
    copyData(*remote_data_file, *writer, vec_slice[slice_id].compressed_size);

    LOG_TRACE(
        trace_log,
        "[download_slice_file][move][slice_id:{}][slice_offset:{}][slice_size:{}][path:{}][current_offset:{}]",
        slice_id,
        vec_slice[slice_id].offset_in_compressed_file,
        vec_slice[slice_id].compressed_size,
        path,
        old_off);
    local_cache->moveFile(tmp_path, path); /// atomic

    LOG_TRACE(
        trace_log,
        "[download_slice_file][return][slice_id:{}][slice_offset:{}][slice_size:{}][path:{}][current_offset:{}]",
        slice_id,
        vec_slice[slice_id].offset_in_compressed_file,
        vec_slice[slice_id].compressed_size,
        path,
        old_off);
}


void SliceReadBuffer::uploadSliceFile(const String & local_path, const String & remote_path)
{
    LOG_TRACE(trace_log, "[upload][local_file:{}][remote_path:{}]", local_path, remote_path);
    auto remote_file = SliceManagement::instance().createRemoteFileToUpload(remote_path);
    if (remote_file != nullptr)
    {
        auto local_file = local_cache->readFile(local_path, read_settings, read_size);
        copyData(*local_file, *remote_file);
    }
    else
    {
        LOG_TRACE(trace_log, "[upload] Cannot get remote file to write.");
    }
}


bool SliceReadBuffer::isTemp(const String & path)
{
    String parent = parentPath(path);
    if (parent[0] == 't' && parent[1] == 'm' && parent[2] == 'p' && parent[3] == '_')
    {
        LOG_TRACE(trace_log, "{} is a temp slice, thus we don't upload it.", path);
        return true;
    }
    return false;
}


off_t SliceReadBuffer::switchToSlice(int slice_id, off_t off)
{
    /// 1.generate slice key
    current_slice = slice_id;
    String file_path = remote_data_file->getFileName();
    String remote_slice_name = getRemoteSliceName(file_path, current_slice);
    String local_slice_path = getLocalSlicePath(file_path, current_slice);

    if (local_cache->exists(local_slice_path))
    {
        /// 1. direct read local cache.
        current_slice_file = local_cache->readFile(local_slice_path, read_settings, read_size);
        LOG_TRACE(trace_log, "[switch][exists][file:{}][slice:{}][offset:{}]", local_slice_path, slice_id, off);
    }
    else
    {
        auto remote_slice_file = SliceManagement::instance().tryToReadSliceFromRemote(remote_slice_name, read_settings, read_size);
        if (remote_slice_file != nullptr)
        {
            /// 2. Has cached slice in remote cache.
            auto metadata = SliceManagement::instance().acquireDownloadSlice(remote_slice_name);
            metadata->mutex.lock();
            if (metadata->status == SliceManagement::SliceDownloadStatus::DOWNLOADED)
            {
                /// A thread may have loaded this slice.
                LOG_TRACE(trace_log, "[switch][downloaded][file:{}][slice:{}][offset:{}]", remote_slice_name, slice_id, off);
            }
            else
            {
                /// 2.1 Download slice file from remote cache to local cache.
                LOG_TRACE(trace_log, "[switch][downloading][file:{}][slice:{}][offset:{}]", remote_slice_name, slice_id, off);
                {
                    String tmp_path = local_slice_path + ".tmp";
                    auto dir_path = directoryPath(tmp_path);
                    if (!local_cache->exists(dir_path))
                    {
                        local_cache->createDirectories(dir_path);
                    }
                    auto writer = local_cache->writeFile(tmp_path, read_settings.local_fs_buffer_size, WriteMode::Rewrite);
                    copyData(*remote_slice_file, *writer);
                    local_cache->moveFile(tmp_path, local_slice_path); /// atomic
                }
                metadata->status = SliceManagement::SliceDownloadStatus::DOWNLOADED;
            }
            LOG_TRACE(trace_log, "[switch][readFile][file:{}][slice:{}][offset:{}]", local_slice_path, slice_id, off);
            current_slice_file = local_cache->readFile(local_slice_path, read_settings, read_size);
            metadata->mutex.unlock();
        }
        else
        {
            /// 3. No cahced slice in local or remote cache, thus we need to download from storage layer.
            auto metadata = SliceManagement::instance().acquireDownloadSlice(remote_slice_name);
            metadata->mutex.lock();
            if (metadata->status == SliceManagement::SliceDownloadStatus::DOWNLOADED)
            {
                /// A thread may have loaded this slice.
                LOG_TRACE(trace_log, "[switch][downloaded][file:{}][slice:{}][offset:{}]", remote_slice_name, slice_id, off);
            }
            else
            {
                /// Download slice file from storage layer.
                LOG_TRACE(trace_log, "[switch][downloading][file:{}][slice:{}][offset:{}]", remote_slice_name, slice_id, off);
                downloadSliceFile(local_slice_path, current_slice);
                /// if (!isTemp(local_slice_path))
                /// {
                ///     uploadSliceFile(local_slice_path, remote_slice_name);
                /// }
                metadata->status = SliceManagement::SliceDownloadStatus::DOWNLOADED;
            }
            LOG_TRACE(trace_log, "[switch][readFile][file:{}][slice:{}][offset:{}]", local_slice_path, slice_id, off);
            current_slice_file = local_cache->readFile(local_slice_path, read_settings, read_size);
            metadata->mutex.unlock();
        }
    }
    /// important !!!
    swap(*current_slice_file);
    return off;
}


off_t SliceReadBuffer::getPosition()
{
    return offset_in_compressed_file;
}


off_t SliceReadBuffer::seek(off_t off, int whence)
{
    LOG_TRACE(trace_log, "[seek][file:{}][offset:{}][whence:{}]", remote_data_file->getFileName(), off, whence);

    if (whence == SEEK_SET)
    {
        offset_in_compressed_file = off;
    }
    else if (whence == SEEK_CUR)
    {
        offset_in_compressed_file += off;
    }

    /// fist run to here
    if (current_slice == -1)
    {
        /// hold buffer is remote_data_file.
        swap(*remote_data_file);
        /// hold buffer is NULL.
    }

    int new_slice_id = getSliceFromOffset(offset_in_compressed_file);
    /// There is no matching slice. It may be switched to an offset exceeding the file size.
    if (new_slice_id == -1)
    {
        LOG_TRACE(trace_log, "[seek][file:{}][bad_offset:{}]", remote_data_file->getFileName(), offset_in_compressed_file);
        return -1;
    }
    else
    {
        /// hold buffer is NULL or old_slice_file.
        switchToSlice(new_slice_id, offset_in_compressed_file);
        /// hold buffer is new_slice_file.
    }

    /// hold buffer is new_slice_file.
    swap(*current_slice_file);
    /// hold buffer is NULL.
    off_t need_to_seek = off - static_cast<off_t>(vec_slice[current_slice].offset_in_compressed_file);
    current_slice_file->seek(need_to_seek, whence);
    /// hold buffer is NULL.
    swap(*current_slice_file);
    /// hold buffer is new_slice_file.
    return offset_in_compressed_file;
}


bool SliceReadBuffer::nextImpl()
{
    if (current_slice == -1) /// fist run eof()
    {
        swap(*remote_data_file);
    }

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
            "[nextImpl][last][file:{}][slice_id:{}][offset:{}]",
            remote_data_file->getFileName(),
            current_slice,
            offset_in_compressed_file);
        return false;
    }
    /// We need to switch to new slice.
    else if (new_slice_id != current_slice)
    {
        switchToSlice(new_slice_id, offset_in_compressed_file);
    }
    swap(*current_slice_file);
    current_slice_file->next();
    swap(*current_slice_file);
    return true;
}

};
