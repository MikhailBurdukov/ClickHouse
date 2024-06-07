#pragma once
#include <optional>
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

// to do: remove Tmp WAL item
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
};
using WALItems = std::vector<WALItem>;


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
