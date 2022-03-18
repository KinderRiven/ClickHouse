#pragma once

#include <memory>
#include <vector>
#include <base/logger_useful.h>
#include <base/types.h>

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class WriteBufferFromKVBase : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferFromKVBase(size_t value_length_);

    ~WriteBufferFromKVBase() override;

    virtual std::string getKeyString() = 0;

private:
    size_t value_length;
};

};