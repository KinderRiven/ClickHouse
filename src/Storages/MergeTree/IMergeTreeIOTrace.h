#pragma once

#include <base/logger_useful.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/IMergeTreeReader.h>

namespace DB {

class MergeTreeDataPartWide;

class IMergeTreeIOTrace {
public:

    static IMergeTreeIOTrace & instance();

    void addMarkTrace(MergeTreeData::DataPartPtr data_part, ColumnPtr column, size_t from_mark);

private:
    Poco::Logger * trace_log = &Poco::Logger::get("[MergeTreeIOTrace]");

    IMergeTreeIOTrace() = default;
};
};