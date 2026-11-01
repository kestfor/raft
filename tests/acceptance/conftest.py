from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest

from tests.acceptance.cluster import Cluster, RemoteCluster


def pytest_addoption(parser):
    group = parser.getgroup("raft acceptance")
    group.addoption("--cluster-manifest", help="attach to a cluster created by make cluster-up")
    group.addoption("--raftnode", default="bin/raftnode", help="raftnode binary")
    group.addoption("--raftctl", default="bin/raftctl", help="raftctl binary")


@pytest.fixture(scope="session")
def test_cluster(request, tmp_path_factory):
    manifest = request.config.getoption("--cluster-manifest")
    ctl = str(Path(request.config.getoption("--raftctl")).resolve())
    if manifest:
        cluster = RemoteCluster(manifest, ctl)
        cluster.reset()
        yield cluster
        cluster.reset()  # An attached cluster belongs to the caller and stays up.
        return

    node = str(Path(request.config.getoption("--raftnode")).resolve())
    for binary in (node, ctl):
        if not Path(binary).is_file():
            pytest.fail(f"missing {binary}; run make build")
    cluster = Cluster(SimpleNamespace(node=node, ctl=ctl,
                      work_dir=tmp_path_factory.mktemp("raft") / "cluster"))
    try:
        for i in range(3):
            cluster.start(i)
        cluster.wait_leader()
        yield cluster
    finally:
        cluster.close()


@pytest.fixture
def cluster(test_cluster):
    test_cluster.reset()
    try:
        yield test_cluster
    finally:
        # A failure must not leave the next scenario with isolated/stopped peers.
        test_cluster.reset()


@pytest.fixture
def key():
    return "pytest/" + uuid.uuid4().hex


@pytest.fixture
def leader(cluster):
    return cluster.wait_leader()


@pytest.fixture
def follower(leader):
    return next(i for i in range(3) if i != leader)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    cluster = item.funcargs.get("cluster")
    if report.failed and cluster:
        for path in cluster.root.glob("node*.log"):
            item.add_report_section(report.when, path.name, path.read_text(errors="replace")[-12000:])


def pytest_report_header(config):
    return "Raft acceptance: real processes + WAL + TCP partitions; scenarios run sequentially"
