from __future__ import annotations

import os
import sys
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


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
) -> ScriptRunResult:

    args = args or []

    env = os.environ.copy()

    if extra_env:
        env.update(extra_env)

    script_path = str(Path(script_path).resolve())

    cmd = [sys.executable, script_path] + args

    try:
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

    except subprocess.TimeoutExpired as e:
        return ScriptRunResult(
            status="error",
            stdout=e.stdout or "",
            stderr="Script excedeu o tempo limite de execução.",
            returncode=-1,
        )