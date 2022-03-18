#pragma once

#include <memory>
#include <vector>
#include <base/logger_useful.h>
#include <base/types.h>

#include <IO/KV/KVBase.h>
#include <IO/KV/SimpleKV.h>
#include <IO/WriteBufferFromKVBase.h>

namespace DB
{

class SyncWriteBufferFromKV : public WriteBufferFromKVBase
{
public:
    SyncWriteBufferFromKV(std::shared_ptr<KVBase> kv, const String & key, size_t value_length);

    ~SyncWriteBufferFromKV() override;

    void sync() override;

    void nextImpl() override;

    void finalize() override;

    std::string getKeyString() override { return key; }

private:
    std::shared_ptr<KVBase> kv_impl = nullptr;

    String key;

    String value;
};

};