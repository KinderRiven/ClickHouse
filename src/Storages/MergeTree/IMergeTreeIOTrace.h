#pragma once
#include <base/logger_useful.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/IMergeTreeReader.h>

namespace DB {

#define LIGHT_DEBUG_IN_RANGE_READER
// #define LIGHT_DEBUG_IN_READER_STREAM

#define DEBUG_IN_RANGE_READER
// #define DEBUG_IN_RANGE_READER_STREAM
// #define DEBUG_IN_RANGE_READER_DELAYED_STREAM
// #define LIGHT_DEBUG_IN_READER_WIDE
// #define DEBUG_IN_READER_WIDE
// #define DEBUG_IN_READER_STREAM

class MergeTreeDataPartWide;

class IMergeTreeIOTrace {
public:
    static IMergeTreeIOTrace & instance();

    void addMarkTrace(String trace, MergeTreeData::DataPartPtr data_part, Columns & column, size_t mark);

    void addMarkTrace(String trace, MergeTreeData::DataPartPtr data_part, Columns & column, MarkRange mark_range);

private:
    Poco::Logger * trace_log = &Poco::Logger::get("[MergeTreeIOTrace]");

    IMergeTreeIOTrace() = default;
};
};