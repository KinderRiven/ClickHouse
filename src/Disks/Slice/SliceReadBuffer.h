#pragma

#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>

namespace DB
{
///
/// SliceReadBuffer holds the fragment information of the original file, the disk
/// that saves the local cache, the remotedisk that saves the fragment cache, and
/// the remotedisk that saves the complete data.
///
class SliceReadBuffer : public ReadBufferFromFileBase
{
public:
    SliceReadBuffer(std::shared_ptr<IDisk> local_cache, std::shared_ptr<IDisk> remote_cache, std::shared_ptr<IDisk> remote_storage);
};
};