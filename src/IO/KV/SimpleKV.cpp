#include "SimpleKV.h"

namespace DB
{

SimpleKV::~SimpleKV() = default;

bool SimpleKV::InsertOrUpdate(const String & key, const String & value)
{
    kv_store[key] = value;
    return true;
}

bool SimpleKV::InsertIfNotExists(const String & key, const String & value)
{
    if (kv_store.find(key) == kv_store.end())
    {
        kv_store[key] = value;
        return true;
    }
    return false;
}

bool SimpleKV::UpdateIfExists(const String & key, const String & value)
{
    if (kv_store.find(key) != kv_store.end())
    {
        kv_store[key] = value;
        return true;
    }
    return false;
}

bool SimpleKV::Put(const String & key, const String & value)
{
    return InsertOrUpdate(key, value);
}

bool SimpleKV::Insert(const String & key, const String & value)
{
    return InsertIfNotExists(key, value);
}

bool SimpleKV::Update(const String & key, const String & value)
{
    return UpdateIfExists(key, value);
}

bool SimpleKV::Get(const String & key, String & value) const
{
    auto iter = kv_store.find(key);
    if (iter != kv_store.end())
    {
        value = iter->second;
        return true;
    }
    return false;
}

bool SimpleKV::Exists(const String & key) const
{
    if (kv_store.find(key) != kv_store.end())
    {
        return true;
    }
    return false;
}

bool SimpleKV::Delete(const String & key)
{
    kv_store.erase(key);
    return true;
}

};