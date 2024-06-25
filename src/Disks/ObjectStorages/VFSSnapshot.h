#pragma once
#include <optional>
#include <fstream>
#include <IO/Lz4DeflatingWriteBuffer.h>
#include <IO/Lz4InflatingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <boost/container/flat_set.hpp>
#include "StoredObject.h"

namespace DB
{
class IObjectStorage;
class ReadBuffer;
class WriteBuffer;

/////// START OF TMP
struct WALItem
{
    String remote_path;
    Int64 delta_link_count;
    String status;
    time_t last_update_timestamp;
    UInt64 last_update_wal_pointer;
    UInt64 wal_uuid;
    WALItem(String path, Int64 delta)
        : remote_path(path), delta_link_count(delta), status(""), last_update_timestamp(0), last_update_wal_pointer(0), wal_uuid(0)
    {
    }
    WALItem()
        : remote_path("")
        , delta_link_count(0)
        , status(""), last_update_timestamp(0)
        , last_update_wal_pointer(0)
        , wal_uuid(0)
    {
    }

};

using WALItems = std::vector<WALItem>;

struct WAL
{
    explicit WAL(const String& path)
    {
        std::ifstream istr(path);
        String remote_path;
        Int64 delta;
        while (istr >> remote_path >> delta)
        {
            wal_items.push_back({remote_path, delta});        
        }
    }

    WALItems popBatch(size_t batch_size)
    {
        batch_size = std::min(batch_size, wal_items.size());
        WALItems result(wal_items.end() - batch_size, wal_items.end());

        size_t target_size = wal_items.size() - batch_size; 
        wal_items.resize(target_size);
        return result;
    }
    
    WALItems wal_items;
};
/////// END OF TMP

struct VFSSnapshotEntry
{
    String remote_path;
    int link_count = 0;

    bool operator==(const VFSSnapshotEntry & entry) const;
    static std::optional<VFSSnapshotEntry> deserialize(ReadBuffer & buf);
    void serialize(WriteBuffer & buf) const;
};

using VFSSnapshotEntries = std::vector<VFSSnapshotEntry>;

VFSSnapshotEntries mergeWithWals(WALItems & wal_items, ReadBuffer & read_buffer, WriteBuffer & write_buffer);

}
