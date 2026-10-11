"""Start GroupMngr, peers and comparisonServer for a local MPComm baseline run."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable


def read_n():
    namespace = {}
    exec((ROOT / "constMP.py").read_text(encoding="utf-8"), namespace)
    return int(namespace["N"])


def wait_for_text(path: Path, text: str, timeout: float):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and text in path.read_text(encoding="utf-8", errors="replace"):
            return True
        time.sleep(0.1)
    return False


def start_process(name: str, args: list[str], outdir: Path, env: dict | None = None, stdin=None):
    log_path = outdir / f"{name}.out"
    log_file = open(log_path, "w", encoding="utf-8")
    process_env = os.environ.copy()
    process_env["PYTHONUNBUFFERED"] = "1"
    process_env["PYTHONIOENCODING"] = "utf-8"
    if env:
        process_env.update(env)
    process = subprocess.Popen(
        args,
        cwd=str(ROOT),
        env=process_env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        stdin=stdin,
        text=True,
    )
    return process, log_file, log_path


def terminate(process: subprocess.Popen, log_file):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    log_file.close()


def run(n_msgs: int, outdir: Path, bind_addrs: list[str] | None = None):
    n_peers = read_n()
    outdir.mkdir(parents=True, exist_ok=True)
    for leftover in ROOT.glob("logfile*.log"):
        leftover.unlink()

    processes = []
    log_files = []
    try:
        group, group_log, group_path = start_process(
            "groupmngr",
            [PYTHON, "-B", "GroupMngr.py"],
            outdir,
        )
        processes.append(group)
        log_files.append(group_log)
        time.sleep(0.5)
        if group.poll() is not None:
            raise RuntimeError(f"GroupMngr exited early with {group.returncode}")

        peer_paths = []
        if bind_addrs is None:
            bind_addrs = [""] * n_peers
        if len(bind_addrs) != n_peers:
            raise ValueError(f"bind_addrs must have {n_peers} entries")

        for index, bind_addr in enumerate(bind_addrs):
            env = {}
            if bind_addr:
                env["MPCOMM_BIND_ADDR"] = bind_addr
            peer, peer_log, peer_path = start_process(
                f"peer{index}",
                [PYTHON, "-B", "peerCommunicatorUDP.py"],
                outdir,
                env=env,
            )
            processes.append(peer)
            log_files.append(peer_log)
            peer_paths.append(peer_path)
            if not wait_for_text(peer_path, "Waiting for signal to start...", 15):
                raise RuntimeError(f"peer{index} did not reach the start wait")

        server, server_log, server_path = start_process(
            "comparison",
            [PYTHON, "-B", "comparisonServer.py"],
            outdir,
            stdin=subprocess.PIPE,
        )
        processes.append(server)
        log_files.append(server_log)
        if server.stdin is None:
            raise RuntimeError("comparisonServer stdin was not opened")
        server.stdin.write(f"{n_msgs}\n0\n")
        server.stdin.flush()
        server.stdin.close()

        deadline = time.monotonic() + 60 + (n_peers * n_msgs * 0.2)
        while time.monotonic() < deadline:
            if server.poll() is not None:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("comparisonServer did not finish within the timeout")

        # Give peers and the group manager a moment to exit after the stop signal.
        time.sleep(1)
        return {
            "n_peers": n_peers,
            "n_msgs": n_msgs,
            "server_exit": server.returncode,
            "group_exit": group.returncode,
            "peer_exits": [proc.returncode for proc in processes[1:-1]],
            "server_log": server_path,
            "group_log": group_path,
            "peer_logs": peer_paths,
        }
    finally:
        for process, log_file in zip(processes, log_files):
            terminate(process, log_file)
        for logfile in ROOT.glob("logfile*.log"):
            shutil.copy2(logfile, outdir / logfile.name)


def main():
    if len(sys.argv) < 3:
        print("Usage: python run_baseline_local.py <n_msgs> <outdir> [bind_addr ...]")
        sys.exit(2)
    n_msgs = int(sys.argv[1])
    outdir = Path(sys.argv[2])
    bind_addrs = sys.argv[3:] or None
    result = run(n_msgs, outdir, bind_addrs)
    print(result)


if __name__ == "__main__":
    main()
