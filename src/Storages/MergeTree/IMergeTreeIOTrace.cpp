#include <Storages/MergeTree/IMergeTreeIOTrace.h>

using namespace DB;

IMergeTreeIOTrace & IMergeTreeIOTrace::instance()
{
    static IMergeTreeIOTrace ret;
    return ret;
}

void IMergeTreeIOTrace::addMarkTrace(String trace, MergeTreeData::DataPartPtr data_part, Columns & columns, size_t mark)
{
    String table_name = data_part->storage.getStorageID().table_name;
    String part_path = data_part->getFullRelativePath();
    String column_name = "";
    size_t column_count = 0;
    if (columns != nullptr)
    {
        column_count = columns.size();
    } else {
        column_count = 0;
        column_name = "NULL";
    }
    LOG_TRACE(trace_log, "[{}][TableName:{}][DataPartPath:{}][ColumnCount:{}][MarkRange:({},{})]",
              trace, table_name, part_path, column_count, mark, mark);
}

void IMergeTreeIOTrace::addMarkTrace(String trace, MergeTreeData::DataPartPtr data_part, Columns & columns, MarkRange mark_range)
{
    String table_name = data_part->storage.getStorageID().table_name;
    String part_path = data_part->getFullRelativePath();
    String column_name;
    if (columns != nullptr)
    {
        column_name = column->getName();
    } else {
        column_name = "UnKnow";
    }
    LOG_TRACE(trace_log, "[{}][TableName:{}][DataPartPath:{}][Column:{}][MarkRange:({},{})]",
              trace, table_name, part_path, column_name, mark_range.begin, mark_range.end);
}