#pragma once

#include <string>

namespace DB
{

using String = std::string;

struct EmbeddedKeyValueStorageOptions
{
};

class IEmbeddedKeyValueStorage
{
public:
    virtual ~IEmbeddedKeyValueStorage() = default;

    class ReadIterator
    {
    public:
        virtual ~ReadIterator() = default;

        virtual String key();
        virtual String value();

        virtual void seekToFirst();

        virtual void next();
        virtual bool valid();
    };

    class WriteIterator
    {
    public:
        virtual ~WriteIterator() = default; 

        virtual bool put(String & key, String & value);

        virtual bool commit();
    };

    virtual void initDB(EmbeddedKeyValueStorageOptions & options) = 0;

    virtual std::unique_ptr<ReadIterator> getReader() const = 0;

    virtual std::unique_ptr<WriteIterator> getWriter() = 0;
};

};
