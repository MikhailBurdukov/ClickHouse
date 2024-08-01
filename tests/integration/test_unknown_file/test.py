# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_latest = cluster.add_instance(
    "node_latest",
    with_minio=True,
    main_configs=["config.d/storage_configuration.xml"],
)

node_old = cluster.add_instance(
    "node_23.8",
    with_minio=True,
    image="clickhouse/clickhouse-server",
    tag="23.3",
    with_installed_binary=True,
    main_configs=["config.d/storage_configuration.xml"],
)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def check_unknown_file_merge(node):
    merge = node.grep_in_log('Done writing part of data into temporary file /var/lib/clickhouse/tmp/tmp') == node.grep_in_log('Done writing part of data into temporary file /var/lib/clickhouse/tmp')
    return merge

def check_unknown_file_aggregation(node):
    aggregation = node.grep_in_log('Writing part of aggregation data into temporary file /var/lib/clickhouse/tmp/tmp') == node.grep_in_log('Writing part of aggregation data into temporary file /var/lib/clickhouse/tmp')
    return aggregation

@pytest.mark.parametrize("node", [(node_latest), (node_old)])
def test_agregation_tmp_file(start_cluster, node):
    node.rotate_logs()
    query = "SELECT count(ignore(*)) FROM (SELECT number FROM system.numbers LIMIT 1e7) GROUP BY number"
    settings = {
        "max_bytes_before_external_group_by": 1 << 10,
        "max_bytes_before_external_sort": 1 << 10,
    }
    node.query(query, settings=settings)
    assert node.contains_in_log(
        "Writing part of aggregation data into temporary file"
    )
    assert check_unknown_file_aggregation(node)

@pytest.mark.parametrize("node", [(node_latest), (node_old)])
def test_agregation_tmp_file_cache(start_cluster, node):

    node.rotate_logs()
    node.query("DROP TABLE IF EXISTS test")
    node.query("CREATE TABLE test (a int, b int) ENGINE = MergeTree() ORDER BY a SETTINGS disk='s3_disk'")
    
    node.query("INSERT INTO test SELECT number, number + 1 FROM  numbers(40000000)")
   
    settings = {
        "max_bytes_before_external_group_by": 5000,
        "max_bytes_before_external_sort": 200,
    }
    node.query("SELECT a, b, count(*) FROM test GROUP BY a, b", settings=settings)

    assert node.contains_in_log(
        "Writing part of aggregation data into temporary file"
    )
    assert check_unknown_file_aggregation(node)

@pytest.mark.parametrize("node", [(node_latest), (node_old)])
def test_merge_tmp_file(start_cluster, node):
    node.rotate_logs()
    query = "SELECT ignore(*) FROM numbers(1024 * 1024) ORDER BY sipHash64(number)"
    settings = {
        "max_bytes_before_external_sort": 100,
    }
    node.query(query, settings=settings)
    assert node.contains_in_log(
        "Done writing part of data into temporary file"
    )
    assert check_unknown_file_merge(node)


@pytest.mark.parametrize("node", [(node_latest), (node_old)])
def test_merge_tmp_file_cache(start_cluster, node):
    node.rotate_logs()
    node.query("DROP TABLE IF EXISTS test")
    node.query("CREATE TABLE test (a int, b int) ENGINE = MergeTree() ORDER BY a SETTINGS disk='s3_disk'")
    
    node.query("INSERT INTO test SELECT number, sipHash64(number + 1) FROM  numbers(40000000)")
   
    query = "SELECT ignore(*) FROM test ORDER BY b"

    settings = {
        "max_bytes_before_external_sort": 100,
    }
    node.query(query, settings=settings)
    assert node.contains_in_log(
        "Done writing part of data into temporary file"
    )
    assert check_unknown_file_merge(node)
