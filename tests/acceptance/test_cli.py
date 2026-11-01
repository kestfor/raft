import base64
import json
import os
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.cli


def test_binary_stdin_files_and_json(cluster, follower, key, tmp_path):
    value = bytes(range(256)) * 1024
    cluster.ctl(follower, "put", key, "--stdin", data=value)
    assert cluster.ctl(follower, "get", key).stdout == value
    output = tmp_path / "value.bin"
    cluster.ctl(follower, "get", key, "--output", str(output))
    assert output.read_bytes() == value
    cluster.ctl(follower, "put", key + "/file", "--file", str(output))
    result = json.loads(cluster.ctl(follower, "--json", "get", key + "/file").stdout)
    assert base64.b64decode(result["value"]) == value
    cluster.ctl(follower, "delete", key)
    assert b"404" in cluster.ctl(follower, "get", key, ok=False).stderr


@pytest.mark.parametrize("arguments", [
    ["put", "missing-value"],
    ["put", "key", "value", "--stdin"],
    ["get", "key", "--json", "--output", "unused"],
], ids=["missing-value", "conflicting-input", "conflicting-output"])
def test_cli_invalid_arguments(cluster, leader, arguments):
    cluster.ctl(leader, *arguments, ok=False)


@pytest.mark.external
def test_curl_and_grpcurl(cluster, follower, key, tmp_path):
    for tool in ("curl", "grpcurl"):
        if not shutil.which(tool):
            pytest.skip(f"optional external tool not installed: {tool}")
    value = bytes(range(256))
    source = tmp_path / "payload.bin"
    source.write_bytes(value)
    url = f"http://127.0.0.1:{cluster.http[follower]}" + cluster.key_path(key)
    subprocess.run(["curl", "--fail-with-body", "-sS", "-L", "-X", "PUT", "--data-binary", "@" + str(source), url],
                   check=True, capture_output=True, timeout=10)
    assert cluster.ctl(follower, "get", key).stdout == value
    subprocess.run(["curl", "--fail-with-body", "-sS", "-L", "-X", "DELETE", url],
                   check=True, capture_output=True, timeout=10)
    response = subprocess.run(["curl", "-sS", "-L", "-o", os.devnull, "-w", "%{http_code}", url],
                              check=True, capture_output=True, timeout=10)
    assert response.stdout == b"404"
    for i in range(3):
        address = f"127.0.0.1:{cluster.grpc[i]}"
        state = subprocess.run(["grpcurl", "-plaintext", address, "raft.v1.RaftService/GetStatus"],
                               check=True, capture_output=True, timeout=10)
        assert json.loads(state.stdout)["id"] == f"node{i+1}"
        raw = subprocess.run(["grpcurl", "-plaintext", "-d", '{"data":"cmF3"}', address,
                              "raft.v1.RaftService/Propose"], capture_output=True, timeout=10)
        assert raw.returncode != 0
        assert b"InvalidArgument" in raw.stderr and b"KV REST API" in raw.stderr
