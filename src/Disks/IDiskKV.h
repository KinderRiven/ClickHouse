#pragma once

#include "IDisk.h"

namespace DB
{


class IDiskKV : public IDisk
{
public:
    const String & getName() const final override { return name; }

    /// DiskKV uses a flat KV data model to access data, thus it has no concept of directory.
    const String & getPath() { return path; }

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    /// Amount of bytes which should be kept free on the disk.
    UInt64 getKeepingFreeSpace() const { return 0; }

    /// Give a key to judge whether the kV pair exists, which can be converted into a get (k, V) call.
    /// TODO in child class.
    /// virtual bool exists(const String & path) const = 0;

    /// Always return correct, because there is no concept of directory in DiskKV,
    /// and all files are flat stored as KV
    bool isFile(const String &) const override { return true; }

    /// Likes isFile(), there is no concept of directory in DiskKV.
    bool isDirectory(const String &) const override { return false; }

    /// Give a key to judge whether the KV pair size, which can be converted into a get (k, V) call.
    /// TODO in child class.
    /// virtual size_t getFileSize(const String & path) const = 0;

    /// Because DiskKV has no concept of directory, this function should not be called in DiskKV.
    void createDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `createDirectory() not implemented for disk: {}`", getType());
    }

    /// Because DiskKV has no concept of directory, this function should not be called in DiskKV.
    void createDirectories(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `createDirectories() not implemented for disk: {}`", getType());
    }

    /// Because DiskKV has no concept of directory, this function should not be called in DiskKV.
    void clearDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `clearDirectory() not implemented for disk: {}`", getType());
    }

    /// Because DiskKV has no concept of directory, this function should not be called in DiskKV.
    void moveDirectory(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `moveDirectory() not implemented for disk: {}`", getType());
    }

    /// Return iterator to the contents of the specified directory.
    DiskDirectoryIteratorPtr iterateDirectory(const String &) override { return nullptr; }

    /// Because DiskKV has no concept of directory, this function should not be called in DiskKV.
    bool isDirectoryEmpty(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `isDirectoryEmpty() not implemented for disk: {}`", getType());
    }

    /// Put a KV pair with empty value.
    /// TODO in child class.
    /// virtual void createFile(const String & path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, an exception will be thrown .
    /// It should be noted that in DiskKV, this kind of move is special, from_path and to_path is a different key.
    /// We call put(to_path, value) to complete the base note, then call delete(from_path, value) to delete the old KV pair.
    /// TODO in child class.
    /// virtual void moveFile(const String & from_path, const String & to_path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, it will be replaced.
    /// Like moveFile().
    /// TODO in child class.
    /// virtual void replaceFile(const String & from_path, const String & to_path) = 0;

    /// List files at `path` and add their names to `file_names`
    void listFiles(const String &, std::vector<String> &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `listFiles() not implemented for disk: {}`", getType());
    }

    /// Open the file for read and return ReadBufferFromFileBase object.
    /// TODO in child class.
    /*  virtual std::unique_ptr<ReadBufferFromFileBase> readFile( /// NOLINT
        const String & path,
        const ReadSettings & settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const = 0; */

    /// Open the file for write and return WriteBufferFromFileBase object.
    /// TODO in child class.
    /*  virtual std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
        const String & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        const WriteSettings & settings = {}) = 0; */

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    /// Call delete(k, v) to delete a KV pair.
    /// TODO in child.
    /// virtual void removeFile(const String & path) = 0;

    /// Remove file if it exists.
    /// Call delete(k, v) to delete a KV pair.
    /// TODO in child.
    /// virtual void removeFileIfExists(const String & path) = 0;

    /// Remove directory. Throws exception if it's not a directory or if directory is not empty.
    void removeDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `removeDirectory() not implemented for disk: {}`", getType());
    }

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    void removeRecursive(const String & path) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `removeRecursive() not implemented for disk: {}`", getType());
    }

    /// Set last modified time to file or directory at `path`.
    void setLastModified(const String &, const Poco::Timestamp &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `setLastModified() not implemented for disk: {}`", getType());
    }

    /// Get last modified time of file or directory at `path`.
    Poco::Timestamp getLastModified(const String &) override
    {
        Poco::Timestamp result;
        return result;
    }

    /// Set file at `path` as read-only.
    void setReadOnly(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `setReadOnly() not implemented for disk: {}`", getType());
    }

    /// Create hardlink from `src_path` to `dst_path`.
    void createHardLink(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `createHardLink() not implemented for disk: {}`", getType());
    }

    /// Truncate file to specified size.
    void truncateFile(const String &, size_t) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `truncateFile() not implemented for disk: {}`", getType());
    }

    /// Return disk type - "local", "s3", etc.
    DiskType getType() const override { return DiskType::KV; }

    /// Involves network interaction.
    /// TODO in child class.
    /// virtual bool isRemote() const = 0;

    /// Whether this disk support zero-copy replication.
    /// Overrode in remote fs disks.
    /// TODO in child class.
    /// virtual bool supportZeroCopyReplication() const = 0;

    /// Whether this disk support parallel write
    /// Overrode in remote fs disks.
    /// TODO in child class.
    /// virtual bool supportParallelWrite() const { return false; }

    ReservationPtr reserve(UInt64) { return {}; }

    /// Return some uniq string for file, overrode for IDiskRemote
    /// Required for distinguish different copies of the same part on remote disk
    String getUniqueId(const String & path) const override { return path; }

    /// Check file exists and ClickHouse has an access to it
    /// Overrode in remote FS disks (s3/hdfs)
    /// Required for remote disk to ensure that replica has access to data written by other node
    bool checkUniqueId(const String & id) const override { return exists(id); }


private:
    Poco::Logger * log;
    const String name;
    const String path = "";
};

};
