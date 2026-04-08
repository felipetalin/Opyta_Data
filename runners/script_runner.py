from __future__ import annotations

import os
import sys
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


@dataclass
class ScriptRunResult:
    status: str
    stdout: str
    stderr: str
    returncode: int


def run_python_script(
    script_path: str,
    args: Optional[list[str]] = None,
    cwd: Optional[Path] = None,
    extra_env: Optional[dict[str, str]] = None,
    on_output_line: Optional[Callable[[str], None]] = None,
) -> ScriptRunResult:

    args = args or []

    env = os.environ.copy()

    if extra_env:
        env.update(extra_env)

    script_path = str(Path(script_path).resolve())

    cmd = [sys.executable, script_path] + args

    try:
        if on_output_line is None:
            proc = subprocess.run(
                cmd,
                cwd=str(cwd) if cwd else None,
                env=env,
                capture_output=True,
                text=True,
                timeout=600,
            )

            return ScriptRunResult(
                status="success" if proc.returncode == 0 else "error",
                stdout=proc.stdout or "",
                stderr=proc.stderr or "",
                returncode=proc.returncode,
            )

        # Modo streaming: envia cada linha ao callback e acumula stdout.
        start_time = time.monotonic()
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd) if cwd else None,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        stdout_lines: list[str] = []
        while True:
            if proc.stdout is None:
                break

            line = proc.stdout.readline()
            if line:
                stdout_lines.append(line)
                on_output_line(line.rstrip("\n"))

            if proc.poll() is not None:
                # consome o restante após término
                tail = proc.stdout.read() if proc.stdout else ""
                if tail:
                    stdout_lines.append(tail)
                    for extra_line in tail.splitlines():
                        on_output_line(extra_line)
                break

            if time.monotonic() - start_time > 600:
                proc.kill()
                return ScriptRunResult(
                    status="error",
                    stdout="".join(stdout_lines),
                    stderr="Script excedeu o tempo limite de execução.",
                    returncode=-1,
                )

        rc = proc.returncode if proc.returncode is not None else 1
        return ScriptRunResult(
            status="success" if rc == 0 else "error",
            stdout="".join(stdout_lines),
            stderr="",
            returncode=rc,
        )

    except subprocess.TimeoutExpired as e:
        return ScriptRunResult(
            status="error",
            stdout=e.stdout or "",
            stderr="Script excedeu o tempo limite de execução.",
            returncode=-1,
        )