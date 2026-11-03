"""Start three native processes, measure workloads and repeated leader failures.

Python standard library only. Uses fresh temporary data; never touches Compose
volumes. Build bin/server, bin/kvctl, bin/benchmark first (or pass --bin-dir).
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import tempfile
import time
import statistics


def address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", default="bin")
    parser.add_argument("--out", default="results")
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if args.n < 1 or args.repeats < 1:
        parser.error("n and repeats must be positive")
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    suffix = ".exe" if os.name == "nt" else ""
    binaries = {name: str(Path(args.bin_dir).resolve() / (name + suffix))
                for name in ("server", "kvctl", "benchmark")}
    clients = [address() for _ in range(3)]
    rafts = [address() for _ in range(3)]
    peers = ",".join(f"node{i+1}={rafts[i]}={clients[i]}" for i in range(3))
    processes = [None] * 3
    logs = []
    recovery = []

    def stop(i):
        process = processes[i]
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    def cli(i, *cmd):
        return subprocess.run([binaries["kvctl"], "--addr", clients[i],
                               "--timeout", "500ms", *cmd], capture_output=True,
                              text=True, timeout=3)

    def leader():
        for i, p in enumerate(processes):
            if p is not None and p.poll() is None:
                result = cli(i, "status")
                if result.returncode == 0 and "state=Leader " in result.stdout:
                    return i
        return None

    def await_leader():
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            i = leader()
            if i is not None:
                return i
            time.sleep(.05)
        raise RuntimeError("leader election timed out")

    with tempfile.TemporaryDirectory(prefix="kv-bench-") as data:
        def start(i):
            log = open(out / f"node{i+1}.log", "a", encoding="utf-8")
            logs.append(log)
            cmd = [binaries["server"], "--id", f"node{i+1}", "--listen", clients[i],
                   "--raft", rafts[i], "--data", str(Path(data) / f"node{i+1}"),
                   "--peers", peers]
            if i == 0:
                cmd.append("--bootstrap")
            processes[i] = subprocess.Popen(cmd, stdout=log, stderr=log)

        try:
            for i in range(3):
                start(i)
            first = await_leader()
            for mode in ("read", "write"):
                for concurrency in (1, 16):
                    subprocess.run([binaries["benchmark"], "--addr", clients[first],
                                    "--mode", mode, "--concurrency", str(concurrency),
                                    "--n", str(args.n), "--out",
                                    str(out / f"{mode}-{concurrency}.json")], check=True)
            seed = cli(await_leader(), "put", "recovery-probe", "acknowledged")
            if seed.returncode:
                raise RuntimeError(seed.stderr)
            for trial in range(args.repeats):
                old = await_leader()
                killed_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
                killed_at = time.monotonic()
                stop(old)
                deadline = killed_at + 25
                while time.monotonic() < deadline:
                    new = leader()
                    if new is not None and new != old:
                        result = cli(new, "get", "recovery-probe")
                        if result.returncode == 0 and result.stdout.strip() == "acknowledged":
                            completed = time.monotonic()
                            recovery.append({"trial": trial + 1, "old_node": old + 1,
                                             "new_node": new + 1, "kill_requested_utc": killed_utc,
                                             "successful_read_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                             "recovery_ms": (completed - killed_at) * 1000})
                            break
                    time.sleep(.05)
                else:
                    raise RuntimeError("recovery timed out")
                start(old)
                # Require participation in a committed write before the next crash:
                # stop the other follower, so the restarted voter is needed for quorum.
                other = next(i for i in range(3) if i not in (old, new))
                stop(other)
                deadline = time.monotonic() + 25
                while time.monotonic() < deadline:
                    result = cli(new, "put", "recovery-ready", str(trial))
                    if result.returncode == 0:
                        break
                    # This orchestrator deliberately repeats an identical idempotent
                    # setup write; these attempts are outside measured workloads.
                    time.sleep(.05)
                else:
                    raise RuntimeError("restarted voter did not rejoin quorum")
                start(other)
        finally:
            for i in range(3):
                stop(i)
            for log in logs:
                log.close()
            (out / "recovery.json").write_text(json.dumps(recovery, indent=2) + "\n")
    metadata = {"platform": platform.platform(), "processor": platform.processor(),
                "logical_cpus": os.cpu_count(), "deployment": "3 native processes on one host; loopback TCP; temporary local storage",
                "method": "closed-loop, one shared gRPC connection; 100 keys; 256-byte values; 100 warmup operations; preload excluded",
                "recovery_method": "kill request to successful verified read through new leader, including CLI startup and 50ms polling; catch-up setup excluded",
                "recovery_median_ms": statistics.median(r["recovery_ms"] for r in recovery)}
    (out / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    subprocess.run([os.sys.executable, str(Path(__file__).with_name("charts.py")), str(out)], check=True)


if __name__ == "__main__":
    main()
