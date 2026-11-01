import socket
import subprocess

import pytest

pytestmark = pytest.mark.failure


def test_follower_crash_and_catch_up(cluster, leader, follower, key):
    cluster.stop(follower, crash=True)
    cluster.put(leader, key, b"survives")
    cluster.start(follower)
    cluster.wait_leader()
    cluster.catch_up()
    cluster.get(follower, key, b"survives")


def test_isolated_leader_cannot_read_or_commit(cluster, leader, key):
    cluster.put(leader, key, b"original")
    cluster.catch_up()
    cluster.partition(leader)
    new = cluster.wait_leader([i for i in range(3) if i != leader])
    cluster.put(new, key, b"new-leader")
    assert cluster.status(leader)["role"] == "leader"
    assert cluster.request(leader, "GET", cluster.key_path(key), follow=False)[0] == 504
    assert cluster.request(leader, "PUT", cluster.key_path(key + "/tail"), b"uncommitted", follow=False)[0] == 504
    cluster.partition(None)
    current = cluster.wait_leader()
    cluster.catch_up()
    assert cluster.request(current, "GET", cluster.key_path(key + "/tail"))[0] == 404
    cluster.get(leader, key, b"new-leader")


def test_lost_quorum_and_client_cancellation(cluster, leader, key):
    others = [i for i in range(3) if i != leader]
    for i in others:
        cluster.stop(i)
    for method, body in [("GET", None), ("PUT", b"maybe")]:
        assert cluster.request(leader, method, cluster.key_path(key), body, follow=False)[0] == 504
    with pytest.raises((TimeoutError, socket.timeout)):
        cluster.request(leader, "PUT", cluster.key_path(key + "/canceled"), b"maybe", timeout=.05)
    for i in others:
        cluster.start(i)
    cluster.wait_leader()
    cluster.catch_up()
    cluster.put(leader, key, b"healthy")
    cluster.get(leader, key, b"healthy")


def test_leader_crash_and_new_writes(cluster, leader, key):
    cluster.put(leader, key, b"before")
    cluster.stop(leader, crash=True)
    new = cluster.wait_leader()
    cluster.get(new, key, b"before")
    cluster.ctl(new, "put", key, "after")
    cluster.start(leader)
    cluster.wait_leader()
    cluster.catch_up()
    cluster.get(leader, key, b"after")


def test_full_cluster_crash_and_wal_recovery(cluster, leader, key):
    values = {key + f"/{i}": bytes(range(256)) * (i + 1) for i in range(24)}
    for name, value in values.items():
        cluster.put(leader, name, value)
    cluster.catch_up()
    for i in range(3):
        cluster.stop(i, crash=True)
    for i in range(3):
        cluster.start(i)
    cluster.wait_leader()
    cluster.catch_up()
    for i, (name, value) in enumerate(values.items()):
        cluster.get(i % 3, name, value)


def test_wal_prevents_second_writer(cluster):
    result = subprocess.run([cluster.args.node, "--config", str(cluster.root / "node1.yaml")],
                            capture_output=True, timeout=5)
    assert result.returncode != 0
    assert b"already open" in result.stderr
