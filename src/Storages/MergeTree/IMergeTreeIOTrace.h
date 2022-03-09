#pragma once

#include <base/logger_useful.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB {

class IMergeTreeDataPart;
class MergeTreeDataPartWide;
class MergeTreeDataPartCompact;
class MergeTreeDataPartInMemory;

class IMergeTreeIOTrace {
public:
    using DataPartWidePtr = std::shared_ptr<const MergeTreeDataPartWide>;

    static IMergeTreeIOTrace & instance();

    void addMarkTrace(DataPartWidePtr data_part, ColumnPtr column, size_t from_mark);

private:
    Poco::Logger * trace_log = &Poco::Logger::get("[MergeTreeIOTrace]");

    IMergeTreeIOTrace() = default;
};
};