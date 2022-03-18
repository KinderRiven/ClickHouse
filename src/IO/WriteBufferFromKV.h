#pragma once

#include <memory>
#include <vector>
#include <base/logger_useful.h>
#include <base/types.h>

#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromKVBase.h>

namespace DB
{

/// key-value stream
class WriteBufferFromKV : public WriteBufferFromFileBase
{
public:
    WriteBufferFromKV(std::unique_ptr<WriteBufferFromKVBase> impl_);

    ~WriteBufferFromKV() = default;

    void sync() override;

    void finalize() override;

    std::string getFileName() const override;

protected:
    std::unique_ptr<WriteBufferFromKVBase> impl;

    bool finalized = false;

private:
    void nextImpl() override;
};

};