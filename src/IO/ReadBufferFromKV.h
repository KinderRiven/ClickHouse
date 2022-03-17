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
    ReadBufferFromKV(SimpleKV * kv, const String & key_);

    ~ReadBufferFromKV() = default;

    bool nextImpl() override;

    off_t getPosition() override;

    off_t seek(off_t off, int whence) override;

    std::string getFileName() const override { return key; };

private:
    SimpleKV * kv_store = nullptr;

    String key;

    String value;

    size_t value_length = 0;

    bool finalized = false;
};

};