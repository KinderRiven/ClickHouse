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
    using DataPart = IMergeTreeDataPart;
    using DataPartPtr = std::shared_ptr<const DataPart>;

    static IMergeTreeIOTrace & instance();

    void addMarkTrace(DataPartPtr data_part, ColumnPtr column, size_t from_mark);

private:
    Poco::Logger * trace_log = &Poco::Logger::get("[MergeTreeIOTrace]");

    IMergeTreeIOTrace() = default;
};
};