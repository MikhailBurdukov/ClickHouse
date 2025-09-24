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

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_split_cache(started_cluster):
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
    node.query("SYSTEM DROP FILESYSTEM CACHE 'split_cache'")
    node.query("INSERT INTO t0 SELECT rand()%1000, rand()%1000 FROM numbers(10000)")
    node.query("SELECT * FROM t0")
    node.restart_clickhouse()
    
    node.query("DETACH TABLE t0")
    node.query("ATTACH TABLE t0")
    node.restart_clickhouse()
    node.query("DROP TABLE t0")