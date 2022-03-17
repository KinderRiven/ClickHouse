#pragma once

#include <memory>
#include <vector>
#include <base/logger_useful.h>
#include <base/types.h>

#include <Disks/KV/SimpleKV.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class WriteBufferFromKV : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferFromKV(SimpleKV * kv, String & key_, size_t value_length_);

    void nextImpl() override;

    void finalize() override;

private:
    SimpleKV * kv_store;

    String key;

    String value;

    size_t value_length;
};

};