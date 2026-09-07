import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

MOCK_PORT = 8083
NUM_ROWS = 100

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance("node", with_minio=True, stay_alive=True)
    try:
        cluster.start()
        start_mock_servers(
            cluster,
            os.path.join(os.path.dirname(__file__), "s3_mocks"),
            [("empty_list_page.py", "resolver", str(MOCK_PORT))],
        )
        yield cluster
    finally:
        cluster.shutdown()


def control_mock(command):
    response = cluster.exec_in_container(
        cluster.get_container_id("resolver"),
        ["curl", "-s", f"http://localhost:{MOCK_PORT}/{command}"],
    )
    assert response == "OK", f"unexpected reply from the mock: {response}"


def test_empty_listing_page_does_not_hide_parts(start_cluster):
    """An empty `ListObjectsV2` page must not make a `plain_rewritable` disk look empty.

    `MetadataStorageFromPlainRewritableObjectStorage::load` decides whether the disk holds
    anything with `existsOrHasAnyChild`, which lists with `max-keys=1`. If that listing stops at
    an empty page instead of following the continuation token, the disk comes up with no
    directories at all and the table silently loses every part it has.

    The disk is defined on the table rather than in the server config so that it is created
    after the mock proxy is listening; it is then recreated on every subsequent startup.
    """
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_empty_page SYNC")
    node.query(
        f"""
        CREATE TABLE test_empty_page (key Int32, value String)
        ENGINE = MergeTree()
        ORDER BY key
        SETTINGS disk = disk(
            name = disk_empty_page,
            type = s3_plain_rewritable,
            endpoint = 'http://resolver:{MOCK_PORT}/root/data/',
            access_key_id = minio,
            secret_access_key = 'ClickHouse_Minio_P@ssw0rd')
        """
    )
    node.query(
        f"INSERT INTO test_empty_page SELECT number, toString(number) FROM numbers({NUM_ROWS})"
    )

    assert int(node.query("SELECT count() FROM test_empty_page")) == NUM_ROWS

    # Arm while the server is down, so the disk load during startup is what consumes the injected
    # page rather than some background listing that happens to run first. From now on the first
    # listing of each prefix answers with no keys, `IsTruncated=true` and a continuation token.
    node.stop_clickhouse()
    control_mock("arm")
    node.start_clickhouse()

    assert int(node.query("SELECT count() FROM test_empty_page")) == NUM_ROWS
    assert (
        int(
            node.query(
                "SELECT count() FROM system.parts "
                "WHERE database = 'default' AND table = 'test_empty_page' AND active"
            )
        )
        > 0
    )

    control_mock("disarm")
    node.query("DROP TABLE test_empty_page SYNC")
