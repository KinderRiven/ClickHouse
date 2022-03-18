#pragma once

#include <memory>
#include <vector>
#include <base/logger_useful.h>
#include <base/types.h>

#include <IO/KV/KVBase.h>
#include <IO/KV/SimpleKV.h>
#include <IO/ReadBufferFromKVBase.h>

namespace DB
{

class SyncReadBufferFromKV : public ReadBufferFromKVBase
{
public:
    SyncReadBufferFromKV(std::shared_ptr<KVBase> kv, const String & key);

    ~SyncReadBufferFromKV() override;

    bool nextImpl() override;

    std::string getKeyString() const override { return key; }

private:
    std::shared_ptr<KVBase> kv_impl = nullptr;

    char * copy_buf = nullptr;

    String key;

    String value;

    size_t value_length = 0;

    bool finalized = false;
};

};