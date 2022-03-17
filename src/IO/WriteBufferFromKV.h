#pragma once

#include <memory>
#include <vector>
#include <base/logger_useful.h>
#include <base/types.h>

#include <Disks/KV/SimpleKV.h>
#include <IO/WriteBufferFromFileBase.h>

namespace DB
{

class WriteBufferFromKV : public WriteBufferFromFileBase
{
public:
    WriteBufferFromKV(SimpleKV * kv, String & key_, size_t value_length_);

    ~WriteBufferFromKV() = default;

    void sync() override;

    void nextImpl() override;

    void finalize() override;

    std::string getFileName() const override { return key; };

private:
    SimpleKV * kv_store;

    String key;

    String value;

    size_t value_length;

    bool finalized = false;
};

};