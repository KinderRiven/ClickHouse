#pragma once

#include <memory>
#include <vector>
#include <base/logger_useful.h>
#include <base/types.h>

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class ReadBufferFromKVBase : public BufferWithOwnMemory<ReadBuffer>
{
public:
    ReadBufferFromKVBase();

    ~ReadBufferFromKVBase() override;

    virtual std::string getKeyString() const = 0;
};

};