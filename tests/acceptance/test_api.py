import concurrent.futures
import json

import pytest

pytestmark = pytest.mark.api


@pytest.mark.parametrize("node", range(3), ids=["node1", "node2", "node3"])
def test_local_status_and_swagger(cluster, node):
    assert cluster.status(node)["id"] == f"node{node+1}"
    assert json.loads(cluster.ctl(node, "--json", "status").stdout)["id"] == f"node{node+1}"
    for path, marker in [("/swagger/", b"SwaggerUIBundle"), ("/openapi.yaml", b"openapi: 3.0.3")]:
        code, _, body = cluster.request(node, "GET", path, follow=False)
        assert code == 200
        assert marker in body


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
def test_follower_redirect_preserves_path_and_query(cluster, leader, follower, key, method):
    path = cluster.key_path(key + "/a?b#c") + "?trace=1"
    code, headers, _ = cluster.request(follower, method, path, b"body" if method == "PUT" else None, follow=False)
    assert code == 307
    assert headers["Location"] == f"http://127.0.0.1:{cluster.http[leader]}" + path


@pytest.mark.parametrize("value", [b"", b"hello", bytes(range(256)), b"x" * (512 << 10)],
                         ids=["empty", "text", "binary", "max-size"])
def test_put_get_overwrite_delete(cluster, leader, follower, key, value):
    index = cluster.put(follower, key, value)
    assert cluster.status(leader)["applied_index"] >= index
    for node in range(3):
        cluster.get(node, key, value)
    cluster.put(leader, key, b"replacement")
    cluster.get(follower, key, b"replacement")
    assert cluster.request(follower, "DELETE", cluster.key_path(key))[0] == 200
    assert cluster.request(follower, "GET", cluster.key_path(key))[0] == 404
    assert cluster.request(follower, "DELETE", cluster.key_path(key))[0] == 200


@pytest.mark.parametrize("key", ["a/../ключ ?#%//.", "k" * 4096], ids=["encoded-key", "max-key"])
def test_key_encoding(cluster, follower, key):
    cluster.put(follower, key, b"value")
    cluster.get(follower, key, b"value")
    assert cluster.request(follower, "DELETE", cluster.key_path(key))[0] == 200


@pytest.mark.parametrize("method,path,body,expected", [
    ("PUT", "/v1/kv/", b"v", 400),
    ("PUT", "/v1/kv/" + "k" * 4097, b"v", 400),
    ("PUT", "/v1/kv/oversized", b"x" * ((512 << 10) + 1), 413),
    ("POST", "/v1/kv/key", b"v", 405),
    ("GET", "/unknown", None, 404),
], ids=["empty-key", "long-key", "large-value", "wrong-method", "unknown-route"])
def test_invalid_requests(cluster, leader, method, path, body, expected):
    code, _, _ = cluster.request(leader, method, path, body)
    assert code == expected


def test_concurrent_read_after_write_and_log_order(cluster, follower, key):
    def worker(i):
        name, value = key + f"/{i}", f"value-{i}".encode()
        index = cluster.put(i % 3, name, value)
        cluster.get((i + 1) % 3, name, value)
        return index
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        indexes = list(pool.map(worker, range(48)))
    assert len(set(indexes)) == 48

    def shared(i):
        value = f"writer-{i}".encode()
        return cluster.put(i % 3, key, value), value
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        writes = list(pool.map(shared, range(24)))
    cluster.get(follower, key, max(writes)[1])


def test_concurrent_get_does_not_append(cluster, leader, key):
    cluster.put(leader, key, b"value")
    cluster.catch_up()
    before = cluster.status(leader)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: cluster.get(i % 3, key, b"value"), range(40)))
    after = cluster.status(leader)
    assert after["term"] == before["term"]
    assert after["last_log_index"] == before["last_log_index"]
