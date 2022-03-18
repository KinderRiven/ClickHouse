#pragma once

#include <map>
#include <string>

namespace DB
{

using String = std::string;

class KVBase
{
public:
    virtual ~KVBase() = 0;

    virtual std::string getName() = 0;

public:
    virtual bool InsertOrUpdate(const String & key, const String & value) = 0;

    virtual bool InsertIfNotExists(const String & key, const String & value) = 0;

    virtual bool UpdateIfExists(const String & key, const String & value) = 0;

    /// Put = InsertOrUpdate
    virtual bool Put(const String & key, const String & value) { return InsertOrUpdate(key, value); }

    /// Insert = InsertIfNotExists
    virtual bool Insert(const String & key, const String & value) { return InsertIfNotExists(key, value); }

    /// Update = UpdateIfExists
    virtual bool Update(const String & key, const String & value) { return UpdateIfExists(key, value); }

public:
    virtual bool Get(const String & key, String & value) const = 0;

    virtual bool Exists(const String & key) const = 0;

public:
    virtual bool Delete(const String & key) = 0;
};

};