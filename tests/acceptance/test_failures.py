import json
import socket
import subprocess

import pytest

pytestmark = pytest.mark.failure


def test_follower_crash_and_catch_up(cluster, leader, follower, key):
    cluster.stop(follower, crash=True)
    cluster.put(leader, key, "survives")
    cluster.start(follower)
    cluster.wait_leader()
    cluster.catch_up()
    cluster.get(follower, key, "survives")


def test_isolated_leader_cannot_read_or_commit(cluster, leader, key):
    cluster.put(leader, key, "original")
    cluster.catch_up()
    cluster.partition(leader)
    new = cluster.wait_leader([i for i in range(3) if i != leader])
    cluster.put(new, key, "new-leader")
    assert cluster.status(leader)["role"] == "leader"
    assert cluster.request(leader, "GET", cluster.key_path(key), follow=False)[0] == 504
    assert cluster.request(leader, "POST", "/query", json.dumps({"key": key + "/tail", "value": "uncommitted"}).encode(), follow=False)[0] == 504
    cluster.partition(None)
    current = cluster.wait_leader()
    cluster.catch_up()
    assert cluster.request(current, "GET", cluster.key_path(key + "/tail"))[0] == 404
    cluster.get(leader, key, "new-leader")


def test_lost_quorum_and_client_cancellation(cluster, leader, key):
    others = [i for i in range(3) if i != leader]
    for i in others:
        cluster.stop(i)
    for method, path, body in [("GET", cluster.key_path(key), None), ("POST", "/query", json.dumps({"key": key, "value": "maybe"}).encode())]:
        assert cluster.request(leader, method, path, body, follow=False)[0] == 504
    with pytest.raises((TimeoutError, socket.timeout)):
        cluster.request(leader, "POST", "/query", json.dumps({"key": key + "/canceled", "value": "maybe"}).encode(), timeout=.05)
    for i in others:
        cluster.start(i)
    cluster.wait_leader()
    cluster.catch_up()
    cluster.put(leader, key, "healthy")
    cluster.get(leader, key, "healthy")


def test_leader_crash_and_new_writes(cluster, leader, key):
    cluster.put(leader, key, "before")
    cluster.stop(leader, crash=True)
    new = cluster.wait_leader()
    cluster.get(new, key, "before")
    cluster.ctl(new, "put", key, json.dumps("after"))
    cluster.start(leader)
    cluster.wait_leader()
    cluster.catch_up()
    cluster.get(leader, key, "after")


def test_full_cluster_crash_and_wal_recovery(cluster, leader, key):
    values = {key + f"/{i}": {"bytes": list(range(256)) * (i + 1)} for i in range(24)}
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
