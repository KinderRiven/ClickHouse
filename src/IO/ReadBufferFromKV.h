#pragma once

#include <memory>
#include <vector>
#include <base/logger_useful.h>
#include <base/types.h>

#include <IO/KV/KVBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromKVBase.h>


namespace DB
{

class ReadBufferFromKV : public ReadBufferFromFileBase
{
public:
    ReadBufferFromKV(std::unique_ptr<ReadBufferFromKVBase> impl);

    ~ReadBufferFromKV();

    bool nextImpl() override;

    off_t getPosition() override;

    off_t seek(off_t off, int whence) override;

    std::string getFileName() const override { return kv_impl->getKeyString(); }

private:
    std::unique_ptr<ReadBufferFromKVBase> kv_impl = nullptr;
};

};