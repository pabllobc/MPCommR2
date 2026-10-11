"""Run a finite local total-order MPComm experiment with loopback aliases."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable
LOG_PATTERNS = (
    "arrival-peer-*.log",
    "delivery-peer-*.log",
    "protocol-peer-*.log",
)


def read_n():
    """Read N from the shared project constants."""
    namespace = {}
    exec((ROOT / "constMP.py").read_text(encoding="utf-8"), namespace)
    return int(namespace["N"])


def wait_for_text(path: Path, text: str, timeout: float):
    """Wait until one child log proves that its control socket is ready."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (
            path.exists()
            and text in path.read_text(encoding="utf-8", errors="replace")
        ):
            return True
        time.sleep(0.1)
    return False


def start_process(name, args, outdir, env=None, stdin=None):
    """Start one unbuffered child and redirect its complete output."""
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


def terminate(process, log_file):
    """Stop a remaining child without leaving experiment ports occupied."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    log_file.close()


def experiment_logs():
    """Yield every ordered-run evidence file in the project root."""
    for pattern in LOG_PATTERNS:
        yield from ROOT.glob(pattern)


def run(n_msgs, outdir, bind_addrs):
    """Start GroupMngr, N ordered peers, and the ordered coordinator."""
    n_peers = read_n()
    if len(bind_addrs) != n_peers:
        raise ValueError(f"Expected {n_peers} loopback addresses")
    outdir.mkdir(parents=True, exist_ok=True)
    for leftover in experiment_logs():
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
        peer_processes = []
        for index, bind_addr in enumerate(bind_addrs):
            peer, peer_log, peer_path = start_process(
                f"peer{index}",
                [PYTHON, "-B", "peerCommunicatorOrdered.py"],
                outdir,
                env={"MPCOMM_BIND_ADDR": bind_addr},
            )
            processes.append(peer)
            peer_processes.append(peer)
            log_files.append(peer_log)
            peer_paths.append(peer_path)
            if not wait_for_text(
                peer_path,
                "Waiting for signal to start...",
                15,
            ):
                raise RuntimeError(f"peer{index} did not become ready")

        server, server_log, server_path = start_process(
            "comparison",
            [PYTHON, "-B", "comparisonServerOrdered.py"],
            outdir,
            stdin=subprocess.PIPE,
        )
        processes.append(server)
        log_files.append(server_log)
        if server.stdin is None:
            raise RuntimeError("Ordered comparison server stdin was not opened")
        server.stdin.write(f"{n_msgs}\n0\n")
        server.stdin.flush()
        server.stdin.close()

        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if server.poll() is not None:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("Ordered experiment exceeded 180 seconds")
        if server.returncode != 0:
            raise RuntimeError(
                f"Ordered comparison server exited with {server.returncode}"
            )

        for child in [*peer_processes, group]:
            child.wait(timeout=10)
            if child.returncode != 0:
                raise RuntimeError(
                    f"Experiment child exited with {child.returncode}"
                )

        return {
            "n_peers": n_peers,
            "n_msgs": n_msgs,
            "server_exit": server.returncode,
            "group_exit": group.returncode,
            "peer_exits": [peer.returncode for peer in peer_processes],
            "server_log": server_path,
            "group_log": group_path,
            "peer_logs": peer_paths,
        }
    finally:
        for process, log_file in zip(processes, log_files):
            terminate(process, log_file)
        for logfile in experiment_logs():
            shutil.copy2(logfile, outdir / logfile.name)


def main():
    """Parse CLI parameters and execute one local ordered round."""
    if len(sys.argv) < 4:
        print(
            "Usage: python run_ordered_local.py "
            "<n_msgs> <outdir> <bind_addr ...>"
        )
        sys.exit(2)
    n_msgs = int(sys.argv[1])
    outdir = Path(sys.argv[2])
    bind_addrs = sys.argv[3:]
    result = run(n_msgs, outdir, bind_addrs)
    print(result)


if __name__ == "__main__":
    main()
