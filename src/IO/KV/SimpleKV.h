#pragma once

#include "KVBase.h"

/// This is a locak key-value storage just for IDISK test
namespace DB
{
class SimpleKV : public KVBase
{
public:
    SimpleKV() = default;

    ~SimpleKV() override;

    std::string getName() override { return String("SimpleKV"); }

public:
    bool InsertOrUpdate(const String & key, const String & value) override;

    bool InsertIfNotExists(const String & key, const String & value) override;

    bool UpdateIfExists(const String & key, const String & value) override;

    bool Put(const String & key, const String & value) override;

    bool Insert(const String & key, const String & value) override;

    bool Update(const String & key, const String & value) override;

    bool Get(const String & key, String & value) const override;

    bool Exists(const String & key) const override;

    bool Delete(const String & key) override;

private:
    std::map<String, String> kv_store;
};
};