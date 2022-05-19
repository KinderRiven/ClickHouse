#include "FileCacheSettings.h"

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

void FileCacheSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    max_size = config.getUInt64(config_prefix + ".data_cache_max_size", REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_CACHE_SIZE);
    max_elements = config.getUInt64(config_prefix + ".data_cache_max_elements", REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS);
    max_file_segment_size
        = config.getUInt64(config_prefix + ".max_file_segment_size", REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE);
    cache_on_write_operations = config.getUInt64(config_prefix + ".cache_on_write_operations", false);
    can_download_hits = config.getUInt64(config_prefix + ".can_download_hits", 5);
    upload_to_remote_cache_threshold = config.getUInt64(config_prefix + ".upload_to_remote_cache_threshold", 5);
    remote_cache = config.getString(config_prefix + ".remote_cache", {});
}

}
