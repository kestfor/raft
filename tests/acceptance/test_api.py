import concurrent.futures
import json

import pytest

pytestmark = pytest.mark.api


@pytest.mark.parametrize("node", range(3), ids=["node1", "node2", "node3"])
def test_local_status_and_swagger(cluster, node):
    assert cluster.status(node)["id"] == f"node{node+1}"
    assert json.loads(cluster.ctl(node, "--json", "status").stdout)["id"] == f"node{node+1}"
    code, _, body = cluster.request(node, "GET", "/openapi.json", follow=False)
    assert code == 200 and json.loads(body)["openapi"] == "3.1.0"
    assert set(json.loads(body)["paths"]) == {"/query", "/log"}
    code, _, body = cluster.request(node, "GET", "/swagger/", follow=False)
    assert code == 200 and b"/openapi.json" in body


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_follower_redirect_preserves_path_and_query(cluster, leader, follower, key, method):
    path = cluster.key_path(key + "/a?b#c") + "&trace=1" if method == "GET" else "/query?trace=1"
    data = json.dumps({"key": key, "value": [1, None]}).encode() if method == "POST" else None
    code, headers, _ = cluster.request(follower, method, path, data, follow=False)
    assert code == 307
    assert headers["Location"] == f"http://127.0.0.1:{cluster.http[leader]}" + path


@pytest.mark.parametrize("value", ["", "hello", 9007199254740993, -1.25, None,
                                   [True, None, {"n": 1}], {"nested": False}, "x" * (512 << 10)],
                         ids=["empty", "text", "large-integer", "number", "null", "array", "object", "512-kib"])
def test_put_get_overwrite(cluster, leader, follower, key, value):
    index = cluster.put(follower, key, value)
    assert cluster.status(leader)["applied_index"] >= index
    for node in range(3):
        cluster.get(node, key, value)
    cluster.put(leader, key, {"replacement": [1, None]})
    cluster.get(follower, key, {"replacement": [1, None]})


def test_large_json_replication(cluster, leader, key):
    value = {"nested": ["x" * (10 << 20)]}
    cluster.ctl(leader, "put", key, "--stdin", data=json.dumps(value).encode())
    cluster.get(leader, key, value)
    cluster.catch_up()
    for node in range(3):
        code, _, body = cluster.request(node, "GET", "/log", follow=False)
        assert code == 200
        assert any(record == {"key": key, "value": value} for record in json.loads(body))


@pytest.mark.parametrize("key", ["a/../ключ ?#%//.", "k" * 4097], ids=["encoded-key", "long-key"])
def test_key_encoding(cluster, follower, key):
    cluster.put(follower, key, "value")
    cluster.get(follower, key, "value")


@pytest.mark.parametrize("method,path,body,expected", [
    ("GET", "/query", None, 422),
    ("GET", "/query?key=", None, 422),
    ("POST", "/query", b'{"key":"k"}', 422),
    ("POST", "/query", b'{"key":"k","value":true}', 422),
    ("POST", "/query", b'{"key":1,"value":1}', 422),
    ("PUT", "/query", b"v", 405),
    ("DELETE", "/query?key=k", None, 405),
    ("GET", "/unknown", None, 404),
])
def test_invalid_requests(cluster, leader, method, path, body, expected):
    code, _, response = cluster.request(leader, method, path, body)
    assert code == expected
    if code == 422:
        for detail in json.loads(response)["detail"]:
            assert {"loc", "msg", "type"} <= set(detail)


def test_local_log_order_and_noops(cluster, follower, key):
    values = ["first", 2, {"last": None}]
    for value in values:
        cluster.put(follower, key, value)
    cluster.catch_up()
    for node in range(3):
        code, _, body = cluster.request(node, "GET", "/log", follow=False)
        assert code == 200
        records = [entry for entry in json.loads(body) if entry["key"] == key]
        assert records == [{"key": key, "value": value} for value in values]
        assert all(set(entry) == {"key", "value"} for entry in json.loads(body))


def test_concurrent_read_after_write_and_log_order(cluster, follower, key):
    def worker(i):
        name, value = key + f"/{i}", {"value": i}
        index = cluster.put(i % 3, name, value)
        cluster.get((i + 1) % 3, name, value)
        return index
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        indexes = list(pool.map(worker, range(48)))
    assert len(set(indexes)) == 48

    def shared(i):
        value = f"writer-{i}"
        return cluster.put(i % 3, key, value), value
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        writes = list(pool.map(shared, range(24)))
    cluster.get(follower, key, max(writes)[1])


def test_concurrent_get_does_not_append(cluster, leader, key):
    cluster.put(leader, key, "value")
    cluster.catch_up()
    before = cluster.status(leader)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: cluster.get(i % 3, key, "value"), range(40)))
    after = cluster.status(leader)
    assert after["term"] == before["term"]
    assert after["last_log_index"] == before["last_log_index"]
