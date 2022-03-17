#pragma once

#include <memory>
#include <vector>
#include <base/logger_useful.h>
#include <base/types.h>

#include <Disks/KV/SimpleKV.h>
#include <IO/ReadBufferFromFileBase.h>

namespace DB
{

class ReadBufferFromKV : public ReadBufferFromFileBase
{
public:
    ReadBufferFromKV(SimpleKV * kv, String & key_, size_t value_length_);

    ~ReadBufferFromKV() = default;

    bool nextImpl() override;

    std::string getFileName() const override { return key; };

private:
    SimpleKV * kv_store = nullptr;

    String key;

    String value;

    size_t value_length;

    bool finalized = false;
};

};