import json
import os
import signal
import time

import pytest

from tests.acceptance import cluster as manager

pytestmark = pytest.mark.management


def test_status_and_endpoint_do_not_expose_control_token(tmp_path, monkeypatch, capsys):
    state = tmp_path / "cluster.json"
    state.write_text(json.dumps({"controller_pid": 1, "control_token": "private-token", "nodes": [
        {"id": "a", "pid": 2, "http": "http://localhost:18001"},
        {"id": "b", "pid": 3, "http": "http://localhost:18002"},
    ]}))
    monkeypatch.setattr(manager, "node_status", lambda node: {
        "role": "leader" if node["id"] == "b" else "follower", "term": 2,
        "commit_index": 10, "applied_index": 10,
    })
    manager.show_status(state)
    output = capsys.readouterr().out
    assert "leader" in output and "18002" in output
    assert "private-token" not in output
    assert manager.leader_endpoint(state) == "http://localhost:18002"


def test_incomplete_state_has_actionable_error(tmp_path):
    state = tmp_path / "cluster.json"
    state.write_text("{}")
    with pytest.raises(RuntimeError, match="cluster-ps"):
        manager.read_state(state)


def test_stale_marker_recovery_checks_controller(tmp_path, monkeypatch):
    state = tmp_path / "cluster.json"
    state.write_text("{}")
    with pytest.raises(RuntimeError, match="in progress"):
        manager.recover_startup_marker(state)
    os.utime(state, (time.time() - 60, time.time() - 60))
    monkeypatch.setattr(manager, "user_processes", lambda: {
        123: (1, f"python -m tests.acceptance.cluster serve --state {state}"),
    })
    with pytest.raises(RuntimeError, match="still starting"):
        manager.recover_startup_marker(state)
    assert state.exists()
    monkeypatch.setattr(manager, "user_processes", lambda: {})
    manager.recover_startup_marker(state)
    assert not state.exists()


def test_legacy_stop_verifies_identity_before_signaling(tmp_path, monkeypatch):
    state = tmp_path / "cluster.json"
    config = str(tmp_path / "node1.yaml")
    info = {"controller_pid": 123, "nodes": [{"pid": 456, "config": config}]}
    signals = []
    monkeypatch.setattr(manager.os, "kill", lambda pid, sig: signals.append((pid, sig)))
    monkeypatch.setattr(manager, "user_processes", lambda: {123: (1, "python unrelated.py")})
    with pytest.raises(RuntimeError, match="cannot verify"):
        manager.stop_legacy(state, info)
    assert not signals

    snapshots = iter([
        {123: (1, f"python scripts/acceptance.py --work-dir {tmp_path}"),
         456: (123, f"/tmp/raft-acceptance-node --config {config}")},
        {},
    ])
    monkeypatch.setattr(manager, "user_processes", lambda: next(snapshots))
    manager.stop_legacy(state, info)
    assert signals == [(123, signal.SIGINT)]
