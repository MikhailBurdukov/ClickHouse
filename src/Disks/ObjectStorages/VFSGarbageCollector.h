#pragma once
#include <vector>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include "IO/ReadHelpers.h"
#include <IO/ReadBuffer.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

#include <fmt/chrono.h>
#include "VFSSnapshot.h"
#include "VFSSettings.h"

namespace DB
{

class Context;

struct GarbageCollectorSettings
{   
    String zk_gc_path;
    size_t gc_sleep_ms;
    double keeper_fault_injection_probability;
    UInt64 keeper_fault_injection_seed;
    size_t batch_size;
};

// Structure for keeping metadata about shapshot.
struct SnapshotMetadata 
{
    uint64_t metadata_version;
    String object_storage_key;
    uint64_t total_size;
    uint64_t snapshot_version;
    
    SnapshotMetadata(
        uint64_t metadata_version_ = 0ull,
        const String & object_storage_key_ = "",
        uint64_t total_size_ = 0ull,
        uint64_t snapshot_version_ = 0ull
    )
        : metadata_version(metadata_version_)
        , object_storage_key(object_storage_key_)
        , total_size(total_size_)
        , snapshot_version(snapshot_version_)
    {

    }

    void update(const String& new_snapshot_key)
    {
        object_storage_key = new_snapshot_key;
        ++snapshot_version;
    }

    String serialize() const
    {
        return fmt::format("{} {} {} {}", metadata_version, object_storage_key, total_size, snapshot_version);
    }

    static SnapshotMetadata deserialize(const String& str) {
        SnapshotMetadata result;
        ReadBufferFromString rb(str);
        
        readIntTextUnsafe(result.metadata_version, rb);
        checkChar(' ', rb);
        readStringUntilWhitespace(result.object_storage_key, rb);
        checkChar(' ', rb);
        readIntTextUnsafe(result.total_size, rb);
        checkChar(' ', rb);
        readIntTextUnsafe(result.snapshot_version, rb);

        return result;
    }
};

class ZooKeeperWithFaultInjection;
using ZooKeeperWithFaultInjectionPtr = std::shared_ptr<ZooKeeperWithFaultInjection>;

class VFSGarbageCollector : private BackgroundSchedulePoolTaskHolder
{
public:
    VFSGarbageCollector(const String& gc_name_, ObjectStoragePtr object_storage_, WAL& wal_, BackgroundSchedulePool & pool, 
                        const GarbageCollectorSettings& settings_);
private:
    void run() const;
    void updateSnapshot() const;
    

    String getZKSnapshotPath() const;
    // Get current shapshot object path from zookeeper.
    SnapshotMetadata getCurrentSnapshotObjectPath() const;
    std::unique_ptr<ReadBuffer> getShapshotReadBuffer (const SnapshotMetadata& snapshot_meta) const;
    void removeShapshotEntires (const VFSSnapshotEntries & entires_to_remove) const;
    void updateShapshotMetadata (const SnapshotMetadata& new_snapshot) const;
    void createGCZookeeperNodes() const;
    ZooKeeperWithFaultInjectionPtr getZookeeper() const;


    String gc_name;
    ObjectStoragePtr object_storage;
    WAL & wal;
    
    const GarbageCollectorSettings settings;
    LoggerPtr log;
};
}
