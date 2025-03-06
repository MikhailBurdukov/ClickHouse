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

    def create_table(self, table_path, data, schema=None):

        schema_arribute = ""
        if schema:
            schema_arribute = (
                '--attributes "{schema= ['
                + ";".join(
                    f"{{name = {name}; type = {type}}}" for name, type in schema.items()
                )
                + ']}"'
            )

        print(schema_arribute)

        cluster.exec_in_container(
            cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "yt --proxy {}:{} create {} table {}".format(
                    self.proxy, self.port, schema_arribute, table_path
                ),
            ],
        )

        cluster.exec_in_container(
            cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "echo '{}' | yt --proxy {}:{} write-table {} --format json".format(
                    data, self.proxy, self.port, table_path
                ),
            ],
        )

    def remove_table(self, table_path):
        cluster.exec_in_container(
            cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "yt --proxy {}:{} remove {}".format(self.proxy, self.port, table_path),
            ],
        )


YT_HOST = "ytsaurus_backend1"
YT_PORT = 80


def test_yt_simple_table_engine(started_cluster):
    yt = YTsaurusCLI(instance, YT_HOST, YT_PORT)
    yt.create_table("//tmp/table", '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    instance.query(
        f"CREATE TABLE yt_test(a Int32, b Int32) ENGINE=Ytsaurus('{YT_HOST}:{YT_PORT}', '//tmp/table')"
    )

    assert instance.query("SELECT * FROM yt_test") == "10\t20\n20\t40\n"
    assert instance.query("SELECT a,b FROM yt_test") == "10\t20\n20\t40\n"
    assert instance.query("SELECT a FROM yt_test") == "10\n20\n"

    assert instance.query("SELECT * FROM yt_test WHERE a > 15") == "20\t40\n"

    instance.query("DROP TABLE yt_test SYNC")

    yt.remove_table("//tmp/table")


def test_yt_simple_table_function(started_cluster):
    yt = YTsaurusCLI(instance, YT_HOST, YT_PORT)
    yt.create_table("//tmp/table", '{"a":"10","b":"20"}\n{"a":"20","b":"40"}')

    assert (
        instance.query(
            f"SELECT * FROM ytsaurus('{YT_HOST}:{YT_HOST}','//tmp/table', 'a Int32, b Int32')"
        )
        == "10\t20\n20\t40\n"
    )
    assert (
        instance.query(
            f"SELECT a,b FROM ytsaurus('{YT_HOST}:{YT_HOST}','//tmp/table', 'a Int32, b Int32')"
        )
        == "10\t20\n20\t40\n"
    )
    assert (
        instance.query(
            f"SELECT a FROM ytsaurus('{YT_HOST}:{YT_HOST}','//tmp/table', 'a Int32, b Int32')"
        )
        == "10\n20\n"
    )
    assert (
        instance.query(
            f"SELECT * FROM ytsaurus('{YT_HOST}:{YT_HOST}','//tmp/table', 'a Int32, b Int32') WHERE a > 15"
        )
        == "20\t40\n"
    )
    yt.remove_table("//tmp/table")


@pytest.mark.parametrize(
    "yt_data_type, yt_data, ch_column_type, ch_data_expected",
    [
        pytest.param(
            "string",
            '"test string"',
            "String",
            "test string",
            id="String",
        ),
        pytest.param(
            "int32",
            "-1",
            "Int32",
            "-1",
            id="Int32",
        ),
        pytest.param(
            "uint32",
            "1",
            "UInt32",
            "1",
            id="UInt32",
        ),
        pytest.param(
            "double",
            "0.1",
            "Float64",
            "0.1",
            id="Float64",
        ),
        pytest.param(
            "boolean",
            "true",
            "Bool",
            "true",
            id="Bool",
        ),
        pytest.param(
            "any",
            "[1, 2]",
            "Array(Int32)",
            "[1,2]",
            id="Array_simple",
        ),
        pytest.param(
            "any",
            "[[1,1],[1,1]]",
            "Array(Array(Int32))",
            "[[1,1],[1,1]]",
            id="Array_complex",
        ),
    ],
)
def test_ytsaurus_types(
    started_cluster, yt_data_type, yt_data, ch_column_type, ch_data_expected
):
    yt = YTsaurusCLI(instance, "ytsaurus_backend1", 80)
    table_path = "//tmp/table"
    column_name = "a"
    yt_data_json = f'{{"{column_name}":{yt_data}}}\n'

    yt.create_table(table_path, yt_data_json, schema={column_name: yt_data_type})

    instance.query(
        f"CREATE TABLE yt_test(a {ch_column_type}) ENGINE=Ytsaurus('{YT_HOST}:{YT_PORT}', '{table_path}')"
    )
    assert instance.query("SELECT a FROM yt_test") == f"{ch_data_expected}\n"
    instance.query("DROP TABLE yt_test")
    yt.remove_table(table_path)
