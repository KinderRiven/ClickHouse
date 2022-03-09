#include <Storages/MergeTree/IMergeTreeIOTrace.h>

using namespace DB;

IMergeTreeIOTrace & IMergeTreeIOTrace::instance()
{
    static IMergeTreeIOTrace ret;
    return ret;
}

void IMergeTreeIOTrace::addMarkTrace(DataPartWidePtr data_part, ColumnPtr column, size_t from_mark)
{
    String table_name = data_part->storage.getStorageID().table_name;
    String part_path = data_part->getFullRelativePath();
    String column_name = column->getName();
    LOG_TRACE(trace_log, "[TableName:{}][DataPartPath:{}][Column:{}][Mark:{}]",
                                     table_name, part_path, column_name, from_mark);
}