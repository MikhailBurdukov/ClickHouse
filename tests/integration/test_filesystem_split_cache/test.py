import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/split_cache.xml"],
    stay_alive=True,
    with_minio=True,
)


def wait_for_cache_initialized(node, cache_name, max_attempts=50):
    initialized = False
    attempts = 0
    while not initialized:
        initialized = bool(node.query(
            f"SELECT is_initialized FROM system.filesystem_cache_settings WHERE is_initialized and cache_name='{cache_name}'"
        ))

        if initialized:
            break

        time.sleep(0.1)
        attempts += 1
        if attempts >= max_attempts:
            raise "Stopped waiting for cache to be initialized"

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_split_cache_silly_test(started_cluster):
    node.query("DROP TABLE IF EXISTS t0")
    node.query(
        """CREATE TABLE t0 (
            key UInt64,
            value UInt64
        )
        ENGINE = MergeTree
        PRIMARY KEY key
        SETTINGS storage_policy = 'split_cache'
        """
    )
    node.query("INSERT INTO t0 SELECT rand()%1000, rand()%1000 FROM numbers(10000)")
    node.query("SELECT * FROM t0;")

    node.query("DROP TABLE t0")

def test_split_cache_restart(started_cluster):
    node.query("DROP TABLE IF EXISTS t0")
    node.query(
        """CREATE TABLE t0 (
            key UInt64,
            value UInt64
        )
        ENGINE = MergeTree
        PRIMARY KEY key
        SETTINGS storage_policy = 'split_cache'
        """
    )
    node.query("INSERT INTO t0 SELECT rand()%1000, rand()%1000 FROM numbers(10000)")
    node.query("OPTIMIZE TABLE t0")

    node.query("SYSTEM DROP FILESYSTEM CACHE 'split_cache'")
    node.restart_clickhouse()
    wait_for_cache_initialized(node, 'split_cache')

    cache_count = int(
        node.query("SELECT count() FROM system.filesystem_cache WHERE size > 0")
    )
    cache_state = node.query(
        "SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE size > 0 ORDER BY key, file_segment_range_begin, size"
    )

    node.restart_clickhouse()
    wait_for_cache_initialized(node, 'split_cache')
    assert int(node.query("SELECT count() FROM system.filesystem_cache WHERE size > 0")) == cache_count
    assert node.query("SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE size > 0 ORDER BY key, file_segment_range_begin, size") == cache_state

    node.query("DROP TABLE t0")

def test_split_cache_system_files_no_eviction(started_cluster):
    """
    Check that n
    """

    node.query("DROP TABLE IF EXISTS t0")
    node.query(
        """CREATE TABLE t0 (
            key UInt64,
            value UInt64
        )
        ENGINE = MergeTree
        PRIMARY KEY key
        SETTINGS storage_policy = 'split_cache',
        min_bytes_for_wide_part = 0
        """
    )
    node.query("SYSTEM STOP MERGES t0")

    for _ in range(100):
        node.query("""
                INSERT INTO t0 SELECT rand()%1000, rand()%1000 FROM numbers(1000000)
                """)

    node.query("SYSTEM DROP FILESYSTEM CACHE 'split_cache'")
    node.restart_clickhouse()
    wait_for_cache_initialized(node, 'split_cache')
    count = int(node.query("SELECT count(*) FROM system.filesystem_cache WHERE cache_path ILIKE '/cache1/Data/%'"))

    node.query("SELECT * FROM t0 FORMAT NULL")

    assert int(node.query("SELECT count(*) FROM system.filesystem_cache WHERE cache_path ILIKE '/cache1/Data/%'")) == count

    node.restart_clickhouse()
    wait_for_cache_initialized(node, 'split_cache')
    assert int(node.query("SELECT count(*) FROM system.filesystem_cache WHERE cache_path ILIKE '/cache1/Data/%'")) == count

    node.query("DROP TABLE t0")
