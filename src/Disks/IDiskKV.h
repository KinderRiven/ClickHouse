#pragma once

#include "IDisk.h"

namespace DB
{
class IDiskKV : public IDisk
{
public:
    IDiskKV(const String & name_, const String & log_name_);

    const String & getName() const final override { return name; }

    /// DiskKV uses a flat KV data model to access data, thus it has no concept of directory.
    const String & getPath() const final override { return path; }

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    /// Always return correct, because there is no concept of directory in DiskKV,
    /// and all files are flat stored as KV
    bool isFile(const String &) const override { return true; }

    /// Likes isFile(), there is no concept of directory in DiskKV.
    bool isDirectory(const String &) const override { return false; }

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

    /// List files at `path` and add their names to `file_names`
    void listFiles(const String &, std::vector<String> &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `listFiles() not implemented for disk: {}`", getType());
    }

    /// Remove directory. Throws exception if it's not a directory or if directory is not empty.
    void removeDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `removeDirectory() not implemented for disk: {}`", getType());
    }

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    void removeRecursive(const String &) override
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

    ReservationPtr reserve(UInt64) override { return {}; }

    /// Return some uniq string for file, overrode for IDiskRemote
    /// Required for distinguish different copies of the same part on remote disk
    String getUniqueId(const String & path_) const override { return path_; }

    /// Check file exists and ClickHouse has an access to it
    /// Overrode in remote FS disks (s3/hdfs)
    /// Required for remote disk to ensure that replica has access to data written by other node
    bool checkUniqueId(const String & id) const override { return exists(id); }

private:
    Poco::Logger * log;
    const String name;
    const String path{};
};

};
