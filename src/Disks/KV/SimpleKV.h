#pragma once

#include <string>
#include <map>
#include <base/types.h>

/// This is a locak key-value storage just for IDISK test
namespace DB {
class SimpleKV {
public:
    SimpleKV() = default;

    bool put(const String &key, const String &value);

    bool get(const String &key, String &value) const;

    bool erase(const String &key);

    bool exists(const String &key) const;

private:
    std::map<String, String> kv_store;
};
};