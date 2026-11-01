"""Owned local test cluster and lifecycle CLI. Test scenarios live in test_*.py."""

import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import secrets
import shlex
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from types import SimpleNamespace


class Network:
    def __init__(self):
        self.lock = threading.Lock()
        self.isolated = None
        self.connections = {}

    def register(self, edge, sockets):
        with self.lock:
            if self.isolated in edge:
                return False
            self.connections.setdefault(edge, set()).add(sockets)
            return True

    def remove(self, edge, sockets):
        with self.lock:
            self.connections.get(edge, set()).discard(sockets)

    def partition(self, node):
        with self.lock:
            self.isolated = node
            sockets = [pair for edge, pairs in self.connections.items() if node in edge for pair in pairs]
        for pair in sockets:
            for connection in pair:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass


class Proxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, network, edge, target):
        self.network, self.edge, self.target = network, edge, target
        super().__init__(("127.0.0.1", 0), Forward)


class Forward(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            upstream = socket.create_connection(self.server.target, timeout=1)
            upstream.settimeout(None)
        except OSError:
            return
        pair = (self.request, upstream)
        if not self.server.network.register(self.server.edge, pair):
            upstream.close()
            return

        def pump(source, dest):
            try:
                while data := source.recv(65536):
                    dest.sendall(data)
            except OSError:
                pass
            finally:
                for connection in pair:
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

        worker = threading.Thread(target=pump, args=pair, daemon=True)
        worker.start()
        pump(upstream, self.request)
        worker.join(timeout=2)
        upstream.close()
        self.server.network.remove(self.server.edge, pair)


def free_port(preferred=0):
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", preferred))
        except OSError:
            sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Cluster:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.work_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.grpc = [free_port() for _ in range(3)]
        self.http = [free_port() for _ in range(3)]
        self.network = Network()
        self.proxies = {}
        self.processes = {}
        self.logs = {}
        for source in range(3):
            for target in range(3):
                if source == target:
                    continue
                proxy = Proxy(self.network, (source, target), ("127.0.0.1", self.grpc[target]))
                self.proxies[source, target] = proxy
                threading.Thread(target=proxy.serve_forever, daemon=True).start()
        for i in range(3):
            peers = "\n".join(f"    node{j+1}: 127.0.0.1:{self.proxies[i,j].server_address[1]}" for j in range(3) if j != i)
            urls = "\n".join(f"    node{j+1}: http://127.0.0.1:{self.http[j]}" for j in range(3))
            (self.root / f"node{i+1}.yaml").write_text(
                f"http:\n  listen_address: 127.0.0.1:{self.http[i]}\n  node_urls:\n{urls}\n"
                f"raft:\n  node_id: node{i+1}\n  listen_address: 127.0.0.1:{self.grpc[i]}\n"
                f"  peers:\n{peers}\n  data_dir: {self.root / ('data'+str(i+1))}\n"
                "log:\n  level: info\n  format: json\n")

    def start(self, i):
        self.logs[i] = open(self.root / f"node{i+1}.log", "ab")
        self.processes[i] = subprocess.Popen(
            [self.args.node, "--config", str(self.root / f"node{i+1}.yaml")],
            stdout=self.logs[i], stderr=self.logs[i])
        self.manifest()

    def stop(self, i, crash=False):
        process = self.processes.pop(i)
        process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise AssertionError(f"node{i+1} did not stop")
        self.logs.pop(i).close()
        assert process.returncode == (-signal.SIGKILL if crash else 0), (i, process.returncode)
        self.manifest()

    def manifest(self):
        data = {
            "controller_pid": os.getpid(),
            "nodes": [{"id": f"node{i+1}", "pid": self.processes[i].pid if i in self.processes else None,
                       "http": f"http://127.0.0.1:{self.http[i]}", "grpc": f"127.0.0.1:{self.grpc[i]}",
                       "config": str(self.root / f"node{i+1}.yaml")} for i in range(3)]}
        if hasattr(self, "control"):
            data.update(self.control)
        path = self.root / "cluster.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(data, indent=2))
        temporary.chmod(0o600)
        temporary.replace(path)
        if hasattr(self, "state_path"):
            temporary = self.state_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(data, indent=2))
            temporary.chmod(0o600)
            temporary.replace(self.state_path)

    def request(self, i, method, path, data=None, follow=True, timeout=8):
        origin = f"http://127.0.0.1:{self.http[i]}"
        for _ in range(6):
            parsed = urllib.parse.urlsplit(origin)
            conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=timeout)
            try:
                conn.request(method, path, body=data, headers={"Content-Type": "application/octet-stream"})
                response = conn.getresponse()
                code, headers, body = response.status, dict(response.getheaders()), response.read()
            finally:
                conn.close()
            if code == 307 and follow:
                target = urllib.parse.urlsplit(headers["Location"])
                origin = f"{target.scheme}://{target.netloc}"
                path = urllib.parse.urlunsplit(("", "", target.path, target.query, ""))
                continue
            return code, headers, body
        raise AssertionError("redirect loop")

    def status(self, i):
        code, _, body = self.request(i, "GET", "/v1/status")
        assert code == 200
        return json.loads(body)

    def wait_leader(self, members=None):
        members = self.members() if members is None else members
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            try:
                states = {i: self.status(i) for i in members}
                leaders = [i for i, s in states.items() if s["role"] == "leader" and s["available"]]
                if len(leaders) == 1 and all(s["leader_id"] == f"node{leaders[0]+1}" for s in states.values()):
                    return leaders[0]
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(.05)
        raise AssertionError("no stable leader")

    def catch_up(self):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            states = [self.status(i) for i in self.members()]
            last = max(s["last_log_index"] for s in states)
            if all(s["applied_index"] == last and s["commit_index"] == last for s in states):
                return
            time.sleep(.05)
        raise AssertionError(f"nodes did not converge: {states}")

    def ctl(self, i, *args, data=None, ok=True):
        result = subprocess.run([self.args.ctl, "--endpoint", f"http://127.0.0.1:{self.http[i]}",
                                 "--timeout", "8s", *args], input=data, capture_output=True, timeout=12)
        assert (result.returncode == 0) == ok, (args, result.returncode, result.stderr.decode(errors="replace"))
        return result

    @staticmethod
    def key_path(key):
        return "/v1/kv/" + urllib.parse.quote(key, safe="")

    def put(self, i, key, value):
        code, _, body = self.request(i, "PUT", self.key_path(key), value)
        assert code == 200, (key, code, body)
        return json.loads(body)["index"]

    def get(self, i, key, value):
        code, _, body = self.request(i, "GET", self.key_path(key))
        assert code == 200 and body == value, (key, code, hashlib.sha256(body).hexdigest())

    def members(self):
        return list(self.processes)

    def partition(self, node):
        self.network.partition(node)

    def reset(self):
        self.partition(None)
        for i in range(3):
            if i not in self.members():
                self.start(i)
        self.wait_leader()
        self.catch_up()

    def close(self):
        self.partition(None)
        failures = []
        try:
            for i in list(self.processes):
                try:
                    self.stop(i)
                except Exception as error:
                    failures.append(error)
        finally:
            for proxy in self.proxies.values():
                proxy.shutdown()
                proxy.server_close()
        if failures:
            raise RuntimeError(f"cluster teardown errors: {failures}")


class RemoteCluster(Cluster):
    """Same test API, but lifecycle operations run in the owned daemon."""

    def __init__(self, state, ctl):
        info = json.loads(Path(state).read_text())
        self.root = Path(info["work_dir"])
        self.args = SimpleNamespace(ctl=str(Path(ctl).resolve()), node=info["node_binary"])
        self.http = [urllib.parse.urlsplit(n["http"]).port for n in info["nodes"]]
        self.grpc = [int(n["grpc"].rsplit(":", 1)[1]) for n in info["nodes"]]
        self.control = info

    def command(self, operation, **kwargs):
        request = urllib.request.Request(
            self.control["control_url"],
            data=json.dumps({"operation": operation, **kwargs}).encode(),
            headers={"Authorization": "Bearer " + self.control["control_token"], "Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)

    def start(self, i):
        self.command("start", node=i)

    def stop(self, i, crash=False):
        self.command("stop", node=i, crash=crash)

    def partition(self, node):
        self.command("partition", node=node)

    def members(self):
        return self.command("members")["members"]

    def close(self):
        self.command("shutdown")


def user_processes():
    result = subprocess.run(["ps", "-u", str(os.getuid()), "-o", "pid=,ppid=,command="],
                            capture_output=True, text=True, check=True)
    processes = {}
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 2)
        if len(fields) == 3:
            processes[int(fields[0])] = (int(fields[1]), fields[2])
    return processes


def show_processes():
    found = False
    print(f"{'PID':>7} {'PARENT':>7}  COMMAND")
    for pid, (parent, command) in user_processes().items():
        if pid == os.getpid():
            continue
        program = Path(command.split()[0]).name
        if (program.startswith("raftnode") or program in ("raft-acceptance-node", "raft-kv-node") or
                "tests.acceptance.cluster serve" in command or
                ("scripts/acceptance.py" in command and "--work-dir" in command)):
            print(f"{pid:>7} {parent:>7}  {command}")
            found = True
    if not found:
        print("No Raft processes found for this user.")


def read_state(path):
    if not path.exists():
        raise RuntimeError(f"no cluster state at {path}; run make cluster-up or set CLUSTER_STATE")
    info = json.loads(path.read_text())
    if not info.get("nodes"):
        raise RuntimeError(f"incomplete startup state at {path}; use make cluster-ps to inspect processes")
    return info


def node_status(node):
    with urllib.request.urlopen(node["http"] + "/v1/status", timeout=2) as response:
        return json.load(response)


def show_status(state):
    info = read_state(state)
    print(f"State: {state}\nController PID: {info.get('controller_pid', '?')}")
    print(f"{'NODE':<9} {'PID':>7} {'ROLE':<12} {'TERM':>5} {'COMMIT':>8} {'APPLIED':>8}  HTTP")
    for node in info["nodes"]:
        try:
            status = node_status(node)
            print(f"{node['id']:<9} {str(node.get('pid') or '-'):>7} {status['role']:<12} "
                  f"{status['term']:>5} {status['commit_index']:>8} {status['applied_index']:>8}  {node['http']}")
        except (OSError, ValueError) as error:
            print(f"{node['id']:<9} {str(node.get('pid') or '-'):>7} {'unreachable':<12} {'-':>5} {'-':>8} {'-':>8}  {node['http']} ({error})")


def leader_endpoint(state):
    for node in read_state(state)["nodes"]:
        try:
            if node_status(node)["role"] == "leader":
                return node["http"]
        except (OSError, ValueError):
            continue
    raise RuntimeError("no reachable leader; inspect make cluster-status or set ENDPOINT explicitly")


def stop_legacy(state, info):
    # The early acceptance runner had no control API. Verify its identity and
    # ownership before signaling the controller; never kill arbitrary saved PIDs.
    processes = user_processes()
    pid = info.get("controller_pid")
    live_nodes = [node for node in info["nodes"] if node.get("pid") in processes]
    if pid not in processes and not live_nodes:
        print("Legacy cluster is already stopped.")
        return
    command = processes.get(pid, (None, ""))[1]
    args = shlex.split(command)
    try:
        work_dir = Path(args[args.index("--work-dir") + 1]).resolve()
    except (ValueError, IndexError):
        work_dir = None
    if "scripts/acceptance.py" not in args or work_dir != state.parent:
        raise RuntimeError("cannot verify legacy controller; inspect make cluster-ps before stopping it manually")
    for node in live_nodes:
        parent, node_command = processes[node["pid"]]
        if parent != pid or node["config"] not in node_command:
            raise RuntimeError("legacy node PID no longer matches its controller/config; refusing to signal")
    os.kill(pid, signal.SIGINT)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        current = user_processes()
        if pid not in current and all(node["pid"] not in current for node in live_nodes):
            print("Legacy cluster stopped. WALs and logs retained.")
            return
        time.sleep(.2)
    raise RuntimeError("legacy cluster shutdown did not finish")


def recover_startup_marker(state):
    if not state.exists() or state.read_text().strip() != "{}":
        return
    if time.time() - state.stat().st_mtime < 30:
        raise RuntimeError("cluster startup is in progress; wait before retrying")
    for _, command in user_processes().values():
        if "tests.acceptance.cluster serve" in command and str(state) in command:
            raise RuntimeError("a controller is still starting; inspect make cluster-ps")
    state.unlink()
    print("Removed stale startup marker (no running controller).")


def serve(args):
    cluster = Cluster(args)
    stopped = threading.Event()
    token = secrets.token_urlsafe(32)

    class Control(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            if self.headers.get("Authorization") != "Bearer " + token:
                self.send_error(403)
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 1024:
                    raise ValueError("invalid body length")
                command = json.loads(self.rfile.read(size))
                operation = command["operation"]
                node = command.get("node")
                if node is not None and (type(node) is not int or node not in range(3)):
                    raise ValueError("invalid node")
                result = {}
                if operation == "start" and node is not None:
                    if node in cluster.members():
                        raise ValueError("node already running")
                    cluster.start(node)
                elif operation == "stop" and node is not None:
                    cluster.stop(node, crash=bool(command.get("crash", False)))
                elif operation == "partition":
                    cluster.partition(node)
                elif operation == "members":
                    result = {"members": cluster.members()}
                elif operation == "shutdown":
                    stopped.set()
                else:
                    raise ValueError("unknown operation")
                body = json.dumps(result).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as error:
                self.send_error(500, str(error))

    control = http.server.HTTPServer(("127.0.0.1", 0), Control)
    cluster.control = {"control_url": f"http://127.0.0.1:{control.server_port}/",
                       "control_token": token, "work_dir": str(cluster.root),
                       "node_binary": args.node}
    try:
        for i in range(3):
            cluster.start(i)
        cluster.wait_leader()
        cluster.catch_up()
        threading.Thread(target=control.serve_forever, daemon=True).start()
        cluster.state_path = Path(args.state).resolve()
        cluster.manifest()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stopped.set())
        stopped.wait()
    finally:
        if hasattr(cluster, "state_path"):
            control.shutdown()
        control.server_close()
        try:
            cluster.close()
        finally:
            Path(args.state).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Manage an owned three-node test cluster")
    parser.add_argument("action", choices=["up", "down", "restart", "serve", "status", "ps", "endpoint", "logs"])
    parser.add_argument("--state", default=".artifacts/test-cluster.json")
    parser.add_argument("--node", default="bin/raftnode")
    parser.add_argument("--ctl", default="bin/raftctl")
    parser.add_argument("--work-dir")
    parser.add_argument("--lines", type=int, default=50)
    parser.add_argument("--follow", action="store_true")
    args = parser.parse_args()
    args.node, args.ctl = str(Path(args.node).resolve()), str(Path(args.ctl).resolve())
    state = Path(args.state).resolve()
    args.state = str(state)

    if args.action == "ps":
        show_processes()
    elif args.action == "status":
        show_status(state)
    elif args.action == "endpoint":
        print(leader_endpoint(state))
    elif args.action == "restart":
        if "control_url" not in read_state(state):
            raise RuntimeError("restart requires a managed cluster with a control API")
        cluster = RemoteCluster(state, args.ctl)
        cluster.partition(None)
        for i in cluster.members():
            cluster.stop(i)
        for i in range(3):
            cluster.start(i)
        cluster.wait_leader()
        cluster.catch_up()
        show_status(state)
    elif args.action == "logs":
        info = read_state(state)
        root = Path(info.get("work_dir", state.parent))
        paths = sorted(root.glob("node*.log"))
        if not paths:
            raise RuntimeError(f"no node logs in {root}")
        if args.lines < 0:
            parser.error("--lines must be nonnegative")
        command = ["tail", "-n", str(args.lines)]
        if args.follow:
            command.append("-F")
        subprocess.run([*command, *map(str, paths)], check=True)
    elif args.action == "serve":
        serve(args)
    elif args.action == "down":
        if not state.exists():
            print("No managed test cluster is running.")
            return
        info = read_state(state)
        if "control_url" not in info:
            stop_legacy(state, info)
            return
        RemoteCluster(state, args.ctl).close()
        deadline = time.monotonic() + 20
        while state.exists() and time.monotonic() < deadline:
            time.sleep(.1)
        if state.exists():
            raise RuntimeError("cluster shutdown did not finish")
        print("Test cluster stopped. WALs and logs retained.")
    else:
        for binary in (args.node, args.ctl):
            if not Path(binary).is_file():
                parser.error(f"missing binary {binary}; run make build")
        state.parent.mkdir(parents=True, exist_ok=True)
        recover_startup_marker(state)
        try:
            with state.open("x") as pending:
                pending.write("{}")
            state.chmod(0o600)
        except FileExistsError:
            parser.error(f"state already exists: {state}; stop that cluster first")
        work = state.parent / "clusters" / str(time.time_ns())
        work.parent.mkdir(parents=True, exist_ok=True)
        log_path = work.with_suffix(".log")
        with log_path.open("ab") as log:
            process = subprocess.Popen(
                [sys.executable, "-m", "tests.acceptance.cluster", "serve", "--state", str(state),
                 "--work-dir", str(work), "--node", args.node, "--ctl", args.ctl],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                state.unlink(missing_ok=True)
                raise RuntimeError(f"cluster startup failed; see {log_path}")
            try:
                remote = RemoteCluster(state, args.ctl)
                if len(remote.members()) == 3:
                    print("Test cluster ready:")
                    for i, port in enumerate(remote.http):
                        print(f"  node{i+1}: http://127.0.0.1:{port}")
                    print(f"State: {state}\nLogs/WAL: {remote.root}")
                    return
            except (OSError, ValueError, KeyError):
                pass
            time.sleep(.1)
        process.terminate()
        process.wait(timeout=15)
        state.unlink(missing_ok=True)
        raise RuntimeError(f"cluster startup timed out; see {log_path}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
