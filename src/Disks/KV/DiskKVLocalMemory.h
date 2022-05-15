#pragma once

#include <Disks/IDiskKV.h>

namespace DB
{

class DiskKVLocalMemory : public IDiskKV
{
public:
    DiskKVLocalMemory(String name_, ContextPtr context_);

    bool exists(const String & path) const override;

    /// Put a KV pair with empty value.
    void createFile(const String & path) override;

    /// Give a key to judge whether the KV pair size, which can be converted into a get (k, V) call.
    size_t getFileSize(const String & path) const override;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, an exception will be thrown .
    /// It should be noted that in DiskKV, this kind of move is special, from_path and to_path is a different key.
    /// We call put(to_path, value) to complete the base note, then call delete(from_path, value) to delete the old KV pair.
    void moveFile(const String & from_path, const String & to_path) override;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, it will be replaced.
    /// Like moveFile().
    void replaceFile(const String & from_path, const String & to_path) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile( /// NOLINT
        const String & path,
        const ReadSettings & settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
        const String & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        const WriteSettings & settings = {}) override;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    /// Call delete(k, v) to delete a KV pair.
    void removeFile(const String & path) override;

    /// Remove file if it exists.
    /// Call delete(k, v) to delete a KV pair.
    void removeFileIfExists(const String & path) override;

    /// Involves network interaction.
    bool isRemote() const override { return false; }

    /// Whether this disk support zero-copy replication.
    /// Overrode in remote fs disks.
    bool supportZeroCopyReplication() const override { return false; }

    /// Whether this disk support parallel write
    /// Overrode in remote fs disks.
    bool supportParallelWrite() const override { return false; }

private:
    ContextPtr context;
};

};
