#include "SimpleKV.h"

using namespace DB;

bool SimpleKV::put(const String & key, const String & value)
{
    kv_store[key] = value;
    return true;
}

bool SimpleKV::get(const String & key, String & value) const
{
    auto iter = kv_store.find(key);
    if (iter != kv_store.end())
    {
        value = iter->second;
        return true;
    }
    return false;
}

bool SimpleKV::erase(const String & key)
{
    kv_store.erase(key);
    return true;
}

bool SimpleKV::exists(const String & key) const
{
    if (kv_store.find(key) != kv_store.end())
    {
        return true;
    }
    return false;
}