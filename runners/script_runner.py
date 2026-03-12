import subprocess
import sys
from pathlib import Path
from dataclasses import dataclass
import os


@dataclass
class RunResult:
    status: str
    stdout: str


def run_python_script(script_path: str, args=None, cwd: Path | None = None) -> RunResult:
    args = args or []
    cmd = [sys.executable, script_path] + args

    # força UTF-8 no subprocess
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",     # ✅ força decode utf-8
            errors="replace",     # ✅ nunca quebra por caractere inválido
            cwd=str(cwd) if cwd else None,
            env=env,
        )

        status = "success" if result.returncode == 0 else "error"
        output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
        return RunResult(status=status, stdout=output)

    except Exception as e:
        return RunResult(status="error", stdout=str(e))