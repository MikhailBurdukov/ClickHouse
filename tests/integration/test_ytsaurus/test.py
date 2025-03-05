import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    with_ytsaurus=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


class YTsaurusCLI:
    def __init__(self, instance, proxy, port):
        self.instance = instance
        self.proxy = proxy
        self.port = port

    def create_table(self, table_path, data):
        cluster.exec_in_container(
            cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "echo '{}' | yt --proxy {}:{} write {} --format json".format(
                    data, self.proxy, self.port, table_path
                ),
            ],
        )


def test_yt_simple_table_engine(started_cluster):
    yt = YTsaurusCLI(instance, "ytsaurus_backend1", 80)
    yt.create_table("//tmp/table", '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    instance.query(
        "CREATE TABLE yt_test(a Int32, b Int32) ENGINE=Ytsaurus('ytsaurus_backend1:80', '//tmp/table')"
    )

    assert instance.query("SELECT * FROM yt_test") == "10\t20\n20\t40\n"
    assert instance.query("SELECT a,b FROM yt_test") == "10\t20\n20\t40\n"
    assert instance.query("SELECT a FROM yt_test") == "10\n20\n"

    assert instance.query("SELECT * FROM yt_test WHERE a > 15") == "20\t40\n"

    instance.query("DROP TABLE yt_test SYNC")


def test_yt_simple_table_function(started_cluster):
    yt = YTsaurusCLI(instance, "ytsaurus_backend1", 80)
    yt.create_table("//tmp/table", '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    assert (
        instance.query(
            "SELECT * FROM ytsaurus('ytsaurus_backend1:80','//tmp/table', 'a Int32, b Int32')"
        )
        == "10\t20\n20\t40\n"
    )
    assert (
        instance.query(
            "SELECT a,b FROM ytsaurus('ytsaurus_backend1:80','//tmp/table', 'a Int32, b Int32')"
        )
        == "10\t20\n20\t40\n"
    )
    assert (
        instance.query(
            "SELECT a FROM ytsaurus('ytsaurus_backend1:80','//tmp/table', 'a Int32, b Int32')"
        )
        == "10\n20\n"
    )
    assert (
        instance.query(
            "SELECT * FROM ytsaurus('ytsaurus_backend1:80','//tmp/table', 'a Int32, b Int32') WHERE a > 15"
        )
        == "20\t40\n"
    )
