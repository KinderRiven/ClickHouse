#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <base/logger_useful.h>

namespace DB
{

class SliceWriteBuffer : public WriteBuffer
{
public:
    struct Slice
    {
    public:
        Slice(size_t num_blocks_, size_t uncompressed_size_, size_t compressed_size_, size_t offset_in_compressed_file_)
            : num_blocks(num_blocks_)
            , uncompressed_size(uncompressed_size_)
            , compressed_size(compressed_size_)
            , offset_in_compressed_file(offset_in_compressed_file_)
        {
        }

    public:
        size_t num_blocks = 0;
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        size_t offset_in_compressed_file = 0;
    };

public:
    SliceWriteBuffer(WriteBuffer * slice_, WriteBuffer * uncompressed_block_buffer_, WriteBuffer * compressed_file_buffer_)
        : WriteBuffer(nullptr, 0)
        , slice(slice_)
        , uncompressed_block_buffer(uncompressed_block_buffer_)
        , compressed_file_buffer(compressed_file_buffer_)
    {
        /// If something has already been written to `uncompressed_block_buffer_` before us,
        /// we will not let the remains of this data affect the slice
        uncompressed_block_buffer->next();
        working_buffer = uncompressed_block_buffer->buffer();
        pos = working_buffer.begin();
        current_slice_start_offset_in_compressed_file = compressed_file_buffer->count();
    }

    void finalize() override
    {
        /// We need to ensure there is no data in compressed buffer to handle,
        /// ------------------------------------------------------------------
        /// | - finalized()
        ///     | - next()
        ///         | - nextImpl()
        ///            | compressed->next()
        ///                 | - compressed->nextImpl()
        /// ------------------------------------------------------------------
        WriteBuffer::finalize();

        /// Before save slice info, we need to sync last slice.
        vec_slice.push_back(Slice(
            num_blocks_in_current_slice,
            uncompressed_bytes_in_current_slice,
            compressed_bytes_in_current_slice,
            current_slice_start_offset_in_compressed_file));

        /// Now we write slice metadata to slice file (*.slice).
        for (auto & single_slice : vec_slice)
        {
            slice->write(reinterpret_cast<const char *>(&single_slice), sizeof(single_slice));
        }
        slice->next(); /// clean slice buf
    }

private:
    void nextImpl() override
    {
        // This is a new block will be compressed,
        uncompressed_bytes_in_current_slice += offset();
        num_blocks++;
        num_blocks_in_current_slice++;

        uncompressed_block_buffer->position() = pos;
        uncompressed_block_buffer->next();
        working_buffer = uncompressed_block_buffer->buffer();

        compressed_bytes_in_current_slice = compressed_file_buffer->count() - current_slice_start_offset_in_compressed_file;

        if (compressed_bytes_in_current_slice >= (4UL * 1024 * 1024)) /// 4MB
        {
            LOG_TRACE(
                trace_log,
                "[slices_id:{}][num_block:{}][offset:{}][uncompressed_size:{}][compressed_size:{}]",
                num_slices,
                num_blocks,
                current_slice_start_offset_in_compressed_file,
                uncompressed_bytes_in_current_slice,
                compressed_bytes_in_current_slice);

            /// generate a slice and push to vec_slice
            vec_slice.push_back(Slice(
                num_blocks_in_current_slice,
                uncompressed_bytes_in_current_slice,
                compressed_bytes_in_current_slice,
                current_slice_start_offset_in_compressed_file));

            current_slice_start_offset_in_compressed_file = compressed_file_buffer->count();
            uncompressed_bytes_in_current_slice = 0;
            num_blocks_in_current_slice = 0;
            num_slices++;
        }
    }

private:
    size_t num_slices = 0;

    size_t num_blocks = 0;

    size_t num_blocks_in_current_slice = 0;

    size_t uncompressed_bytes_in_current_slice = 0;

    size_t compressed_bytes_in_current_slice = 0;

    size_t current_slice_start_offset_in_compressed_file = 0;

    std::vector<Slice> vec_slice;

    WriteBuffer * slice;

    WriteBuffer * uncompressed_block_buffer;

    WriteBuffer * compressed_file_buffer;

    Poco::Logger * trace_log = &Poco::Logger::get("[slice]");
};

};