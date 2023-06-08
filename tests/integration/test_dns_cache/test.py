import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)


def _fill_nodes(nodes, table_name):
    for node in nodes:
        node.query(
            """
            CREATE DATABASE IF NOT EXISTS test;
            CREATE TABLE IF NOT EXISTS {0}(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{0}', '{1}')
            ORDER BY id PARTITION BY toYYYYMM(date);
            """.format(
                table_name, node.name
            )
        )


node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/listen_host.xml"],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1111",
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/listen_host.xml", "configs/dns_update_long.xml"],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1112",
)


@pytest.fixture(scope="module")
def cluster_without_dns_cache_update():
    try:
        cluster.start()

        _fill_nodes([node1, node2], "test_table_drop")

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()
        pass


valid_ipv4='10.5.172.10'

node8 = cluster.add_instance(
    "node8",
    main_configs=["configs/listen_host.xml"],
    user_configs=["configs/test.xml"],
    with_zookeeper=True,
    ipv4_address=valid_ipv4,
)

node9 = cluster.add_instance(
    "node9",
    main_configs=["configs/listen_host.xml"],
    user_configs=["configs/test.xml"],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1119",
)

# node8 - source with table, have invalid ipv6
# node9 - destination, doing url query
def test_ip_remote_with_multiple_ips(cluster_without_dns_cache_update):
    node8.query(
        "CREATE TABLE test(t Date, label UInt8) ENGINE = MergeTree PARTITION BY t ORDER BY label;"
    )
    node8.query(
        "INSERT INTO test SELECT toDate('2022-12-28'), 1;"
    )
    wrong_ip = '2001:3984:3989::1:1118'
    node9.query("set use_hedged_requests=1")
    node8.query("set use_hedged_requests=1")
    
    node9.exec_in_container(
        (["bash", "-c", "echo '{} node8' >> /etc/hosts".format(wrong_ip)])
    )
    node9.exec_in_container(
        (["bash", "-c", "echo '{} node8' >> /etc/hosts".format(valid_ipv4)])
    )
    
    assert node8.query("SELECT count(*) from test") == "1\n"
    node9.query("SYSTEM DROP DNS CACHE")
    node8.query("SYSTEM DROP DNS CACHE")
    assert node9.query("SELECT count(*) FROM remote('node8', default, test) limit 1;") == "1\n"

valid__ipv4='10.5.172.11'

node10 = cluster.add_instance(
    "node10",
    main_configs=["configs/listen_host.xml"],
    with_zookeeper=True,
    ipv4_address=valid__ipv4,
)

node11 = cluster.add_instance(
    "node11",
    main_configs=["configs/listen_host.xml"],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1219",
)

# node8 - source with table, have invalid ipv6
# node9 - destination, doing url query
def test_ip_remote_with_multiple_ips_2(cluster_without_dns_cache_update):
    node10.query(
        "CREATE TABLE test(t Date, label UInt8) ENGINE = MergeTree PARTITION BY t ORDER BY label;"
    )
    node10.query(
        "INSERT INTO test SELECT toDate('2022-12-28'), 1;"
    )
    wrong_ip = '2001:3984:3989::1:1118'
    node11.query("set use_hedged_requests=1")
    node10.query("set use_hedged_requests=1")
    
    node11.exec_in_container(
        (["bash", "-c", "echo '{} node10' >> /etc/hosts".format(wrong_ip)])
    )
    node11.exec_in_container(
        (["bash", "-c", "echo '{} node10' >> /etc/hosts".format(valid__ipv4)])
    )
    
    assert node10.query("SELECT count(*) from test") == "1\n"
    node11.query("SYSTEM DROP DNS CACHE")
    node10.query("SYSTEM DROP DNS CACHE")
    assert node11.query("SELECT count(*) FROM remote('node10', default, test) limit 1;") == "1\n"
     
# node1 is a source, node2 downloads data
# node2 has long dns_cache_update_period, so dns cache update wouldn't work
def test_ip_change_drop_dns_cache(cluster_without_dns_cache_update):
    # First we check, that normal replication works
    node1.query(
        "INSERT INTO test_table_drop VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)"
    )
    assert node1.query("SELECT count(*) from test_table_drop") == "3\n"
    assert_eq_with_retry(node2, "SELECT count(*) from test_table_drop", "3")

    # We change source node ip
    cluster.restart_instance_with_ip_change(node1, "2001:3984:3989::1:7777")

    # Put some data to source node1
    node1.query(
        "INSERT INTO test_table_drop VALUES ('2018-10-01', 5), ('2018-10-02', 6), ('2018-10-03', 7)"
    )
    # Check that data is placed on node1
    assert node1.query("SELECT count(*) from test_table_drop") == "6\n"

    # Because of DNS cache dest node2 cannot download data from node1
    with pytest.raises(Exception):
        assert_eq_with_retry(node2, "SELECT count(*) from test_table_drop", "6")

    # drop DNS cache
    node2.query("SYSTEM DROP DNS CACHE")
    # Data is downloaded
    assert_eq_with_retry(node2, "SELECT count(*) from test_table_drop", "6")

    # Just to be sure check one more time
    node1.query("INSERT INTO test_table_drop VALUES ('2018-10-01', 8)")
    assert node1.query("SELECT count(*) from test_table_drop") == "7\n"
    assert_eq_with_retry(node2, "SELECT count(*) from test_table_drop", "7")


# node3 = cluster.add_instance(
#     "node3",
#     main_configs=["configs/listen_host.xml"],
#     with_zookeeper=True,
#     ipv6_address="2001:3984:3989::1:1113",
# )
# node4 = cluster.add_instance(
#     "node4",
#     main_configs=[
#         "configs/remote_servers.xml",
#         "configs/listen_host.xml",
#         "configs/dns_update_short.xml",
#     ],
#     with_zookeeper=True,
#     ipv6_address="2001:3984:3989::1:1114",
# )


# @pytest.fixture(scope="module")
# def cluster_with_dns_cache_update():
#     try:
#         cluster.start()

#         _fill_nodes([node3, node4], "test_table_update")

#         yield cluster

#     except Exception as ex:
#         print(ex)

#     finally:
#         cluster.shutdown()
#         pass


# # node3 is a source, node4 downloads data
# # node4 has short dns_cache_update_period, so testing update of dns cache
# def test_ip_change_update_dns_cache(cluster_with_dns_cache_update):
#     # First we check, that normal replication works
#     node3.query(
#         "INSERT INTO test_table_update VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)"
#     )
#     assert node3.query("SELECT count(*) from test_table_update") == "3\n"
#     assert_eq_with_retry(node4, "SELECT count(*) from test_table_update", "3")

#     # We change source node ip
#     cluster.restart_instance_with_ip_change(node3, "2001:3984:3989::1:8888")

#     # Put some data to source node3
#     node3.query(
#         "INSERT INTO test_table_update VALUES ('2018-10-01', 5), ('2018-10-02', 6), ('2018-10-03', 7)"
#     )

#     # Check that data is placed on node3
#     assert node3.query("SELECT count(*) from test_table_update") == "6\n"

#     curl_result = node4.exec_in_container(["bash", "-c", "curl -s 'node3:8123'"])
#     assert curl_result == "Ok.\n"
#     cat_resolv = node4.exec_in_container(["bash", "-c", "cat /etc/resolv.conf"])
#     print(("RESOLV {}".format(cat_resolv)))

#     assert_eq_with_retry(
#         node4, "SELECT * FROM remote('node3', 'system', 'one')", "0", sleep_time=0.5
#     )

#     # Because of DNS cache update, ip of node3 would be updated
#     assert_eq_with_retry(
#         node4, "SELECT count(*) from test_table_update", "6", sleep_time=3
#     )

#     # Just to be sure check one more time
#     node3.query("INSERT INTO test_table_update VALUES ('2018-10-01', 8)")
#     assert node3.query("SELECT count(*) from test_table_update") == "7\n"
#     assert_eq_with_retry(node4, "SELECT count(*) from test_table_update", "7")


# def set_hosts(node, hosts):
#     new_content = "\\n".join(["127.0.0.1 localhost", "::1 localhost"] + hosts)
#     node.exec_in_container(
#         ["bash", "-c", 'echo -e "{}" > /etc/hosts'.format(new_content)],
#         privileged=True,
#         user="root",
#     )


# def test_dns_cache_update(cluster_with_dns_cache_update):
#     set_hosts(node4, ["127.255.255.255 lost_host"])

#     with pytest.raises(QueryRuntimeException):
#         node4.query("SELECT * FROM remote('lost_host', 'system', 'one')")

#     node4.query(
#         "CREATE TABLE distributed_lost_host (dummy UInt8) ENGINE = Distributed(lost_host_cluster, 'system', 'one')"
#     )
#     with pytest.raises(QueryRuntimeException):
#         node4.query("SELECT * FROM distributed_lost_host")

#     set_hosts(node4, ["127.0.0.1 lost_host"])

#     # Wait a bit until dns cache will be updated
#     assert_eq_with_retry(
#         node4, "SELECT * FROM remote('lost_host', 'system', 'one')", "0"
#     )
#     assert_eq_with_retry(node4, "SELECT * FROM distributed_lost_host", "0")

#     assert TSV(
#         node4.query(
#             "SELECT DISTINCT host_name, host_address FROM system.clusters WHERE cluster='lost_host_cluster'"
#         )
#     ) == TSV("lost_host\t127.0.0.1\n")
#     assert TSV(node4.query("SELECT hostName()")) == TSV("node4")


# # Check SYSTEM DROP DNS CACHE on node5 and background cache update on node6
# node5 = cluster.add_instance(
#     "node5",
#     main_configs=["configs/listen_host.xml", "configs/dns_update_long.xml"],
#     user_configs=["configs/users_with_hostname.xml"],
#     ipv6_address="2001:3984:3989::1:1115",
# )
# node6 = cluster.add_instance(
#     "node6",
#     main_configs=["configs/listen_host.xml", "configs/dns_update_short.xml"],
#     user_configs=["configs/users_with_hostname.xml"],
#     ipv6_address="2001:3984:3989::1:1116",
# )


# @pytest.mark.parametrize("node", [node5, node6])
# def test_user_access_ip_change(cluster_with_dns_cache_update, node):
#     node_name = node.name
#     node_num = node.name[-1]
#     # getaddrinfo(...) may hang for a log time without this options
#     node.exec_in_container(
#         [
#             "bash",
#             "-c",
#             'echo -e "options timeout:1\noptions attempts:2" >> /etc/resolv.conf',
#         ],
#         privileged=True,
#         user="root",
#     )

#     assert (
#         node3.query("SELECT * FROM remote('{}', 'system', 'one')".format(node_name))
#         == "0\n"
#     )
#     assert (
#         node4.query("SELECT * FROM remote('{}', 'system', 'one')".format(node_name))
#         == "0\n"
#     )

#     set_hosts(
#         node,
#         [
#             "127.255.255.255 node3",
#             "2001:3984:3989::1:88{}4 unknown_host".format(node_num),
#         ],
#     )

#     cluster.restart_instance_with_ip_change(
#         node3, "2001:3984:3989::1:88{}3".format(node_num)
#     )
#     cluster.restart_instance_with_ip_change(
#         node4, "2001:3984:3989::1:88{}4".format(node_num)
#     )

#     with pytest.raises(QueryRuntimeException):
#         node3.query("SELECT * FROM remote('{}', 'system', 'one')".format(node_name))
#     with pytest.raises(QueryRuntimeException):
#         node4.query("SELECT * FROM remote('{}', 'system', 'one')".format(node_name))
#     # now wrong addresses are cached

#     set_hosts(node, [])
#     retry_count = 60
#     if node_name == "node5":
#         # client is not allowed to connect, so execute it directly in container to send query from localhost
#         node.exec_in_container(
#             ["bash", "-c", 'clickhouse client -q "SYSTEM DROP DNS CACHE"'],
#             privileged=True,
#             user="root",
#         )
#         retry_count = 1

#     assert_eq_with_retry(
#         node3,
#         "SELECT * FROM remote('{}', 'system', 'one')".format(node_name),
#         "0",
#         retry_count=retry_count,
#         sleep_time=1,
#     )
#     assert_eq_with_retry(
#         node4,
#         "SELECT * FROM remote('{}', 'system', 'one')".format(node_name),
#         "0",
#         retry_count=retry_count,
#         sleep_time=1,
#     )


# # def test_host_is_drop_from_cache_after_consecutive_failures(
# #     cluster_with_dns_cache_update,
# # ):
# #     with pytest.raises(QueryRuntimeException):
# #         node4.query(
# #             "SELECT * FROM remote('InvalidHostThatDoesNotExist', 'system', 'one')"
# #         )

# #     # Note that the list of hosts in variable since lost_host will be there too (and it's dropped and added back)
# #     # dns_update_short -> dns_max_consecutive_failures set to 6
# #     assert node4.wait_for_log_line(
# #         "Cannot resolve host \\(InvalidHostThatDoesNotExist\\), error 0: Host not found."
# #     )
# #     assert node4.wait_for_log_line(
# #         "Cached hosts not found:.*InvalidHostThatDoesNotExist**",
# #         repetitions=6,
# #         timeout=60,
# #         look_behind_lines=500,
# #     )
# #     assert node4.wait_for_log_line(
# #         "Cached hosts dropped:.*InvalidHostThatDoesNotExist.*"
# #     )
