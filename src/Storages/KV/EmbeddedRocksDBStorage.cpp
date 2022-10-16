#include <Storages/KV/EmbeddedRocksDBStorage.h>

namespace DB
{

String EmbeddedRocksDBStorage::ReadIterator::key()
{
    return iterator->key().ToString();
}

String EmbeddedRocksDBStorage::ReadIterator::value()
{
    return iterator->value().ToString();
}

void EmbeddedRocksDBStorage::ReadIterator::seekToFirst()
{
    iterator->SeekToFirst();
}

void EmbeddedRocksDBStorage::ReadIterator::next()
{
    iterator->Next();
}

bool EmbeddedRocksDBStorage::ReadIterator::valid()
{
    return iterator->Valid();
}

bool EmbeddedRocksDBStorage::WriteIterator::put(String & key, String & value)
{
    rocksdb::Status status = batch.Put(key, value);
    return status.ok();
}

bool EmbeddedRocksDBStorage::WriteIterator::commit()
{
    rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    return true;
}

EmbeddedRocksDBStorage::EmbeddedRocksDBStorage()
{
}

void EmbeddedRocksDBStorage::initDB(EmbeddedKeyValueStorageOptions &)
{
}

std::unique_ptr<IEmbeddedKeyValueStorage::ReadIterator> EmbeddedRocksDBStorage::getReader() const
{
    return std::make_unique<EmbeddedRocksDBStorage::ReadIterator>(
        std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(rocksdb::ReadOptions())));
}

std::unique_ptr<IEmbeddedKeyValueStorage::WriteIterator> EmbeddedRocksDBStorage::getWriter()
{
    return std::make_unique<EmbeddedRocksDBStorage::WriteIterator>(rocksdb_ptr);
}

};
