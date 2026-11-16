import json
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.cli


def test_json_stdin_files_and_output(cluster, follower, key, tmp_path):
    value = {"items": list(range(256)) * 128, "nested": [True, None]}
    encoded = json.dumps(value).encode()
    cluster.ctl(follower, "put", key, "--stdin", data=encoded)
    assert json.loads(cluster.ctl(follower, "get", key).stdout) == value
    output = tmp_path / "value.json"
    cluster.ctl(follower, "get", key, "--output", str(output))
    assert json.loads(output.read_bytes()) == value
    cluster.ctl(follower, "put", key + "/file", "--file", str(output))
    result = json.loads(cluster.ctl(follower, "--json", "get", key + "/file").stdout)
    assert result == {"key": key + "/file", "value": value}
    records = json.loads(cluster.ctl(follower, "log").stdout)
    assert any(record == result for record in records)


@pytest.mark.parametrize("arguments", [
    ["put", "missing-value"],
    ["put", "key", "value", "--stdin"],
    ["put", "key", "invalid-json"],
    ["get", "key", "--json", "--output", "unused"],
])
def test_cli_invalid_arguments(cluster, leader, arguments):
    cluster.ctl(leader, *arguments, ok=False)


@pytest.mark.external
def test_curl_and_grpcurl(cluster, follower, key):
    for tool in ("curl", "grpcurl"):
        if not shutil.which(tool):
            pytest.skip(f"optional external tool not installed: {tool}")
    value = {"array": [1, True, None]}
    origin = f"http://127.0.0.1:{cluster.http[follower]}"
    subprocess.run(["curl", "--fail-with-body", "-sS", "-L", "-H", "Content-Type: application/json",
                    "--data-binary", json.dumps({"key": key, "value": value}), origin + "/query"],
                   check=True, capture_output=True, timeout=10)
    assert json.loads(cluster.ctl(follower, "get", key).stdout) == value
    response = subprocess.run(["curl", "--fail-with-body", "-sS", "-L", origin + cluster.key_path(key)],
                              check=True, capture_output=True, timeout=10)
    assert json.loads(response.stdout) == {"key": key, "value": value}
    for i in range(3):
        address = f"127.0.0.1:{cluster.grpc[i]}"
        state = subprocess.run(["grpcurl", "-plaintext", address, "raft.v1.RaftService/GetStatus"],
                               check=True, capture_output=True, timeout=10)
        assert json.loads(state.stdout)["id"] == f"node{i+1}"
        raw = subprocess.run(["grpcurl", "-plaintext", "-d", '{"data":"cmF3"}', address,
                              "raft.v1.RaftService/Propose"], capture_output=True, timeout=10)
        assert raw.returncode != 0
        assert b"InvalidArgument" in raw.stderr and b"KV REST API" in raw.stderr
