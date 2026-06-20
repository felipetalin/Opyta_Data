"""
core/audit/manifest.py
======================

Primitivas de auditoria/lineage portadas de
`Opyta_Data_Analysis/src/opyta_analysis/audit_utils.py` e adaptadas ao app de
ingestão Opyta_Data.

Tudo aqui usa apenas a biblioteca padrão (sem dependências novas) e é puramente
aditivo: nenhuma função altera dados do banco ou comportamento existente. O
objetivo é registrar *o que aconteceu* (qual arquivo, qual hash, em qual commit,
por qual operação) para dar rastreabilidade às etapas de cadastro, importação,
validação, migração e consolidação.
"""
from __future__ import annotations

import getpass
import hashlib
import json
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

# Raiz do repositório Opyta_Data (este arquivo está em core/audit/).
REPO_ROOT = Path(__file__).resolve().parents[2]

# Diretório onde os registros de auditoria são gravados.
AUDIT_DIR = REPO_ROOT / "runtime" / "audit"

UTC = timezone.utc

DEFAULT_DELIVERABLE_PATTERNS: tuple[str, ...] = (
    "*.xlsx",
    "*.xlsm",
    "*.csv",
    "*.json",
)


# ---------------------------------------------------------------------------
# Git context
# ---------------------------------------------------------------------------
def run_git(args: Sequence[str], repo_root: Path | None = None) -> str | None:
    repo_root = repo_root or REPO_ROOT
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _status_path(line: str) -> str:
    path = re.sub(r"^[ MADRCU?!]{1,2}\s+", "", line, count=1)
    if " -> " in path:
        path = path.split(" -> ", 1)[1]
    return path.strip().strip('"')


def git_context(repo_root: Path | None = None) -> dict[str, object]:
    """Branch/commit/estado-sujo do repositório no momento da operação."""
    repo_root = repo_root or REPO_ROOT
    status_short = run_git(["status", "--short"], repo_root) or ""
    status_lines = [line for line in status_short.splitlines() if line.strip()]
    return {
        "branch": run_git(["branch", "--show-current"], repo_root),
        "commit": run_git(["rev-parse", "--short", "HEAD"], repo_root),
        "dirty": bool(status_lines),
        "status_short_count": len(status_lines),
    }


# ---------------------------------------------------------------------------
# Integridade de arquivos
# ---------------------------------------------------------------------------
def sha256_file(path: str | Path) -> str | None:
    path = Path(path)
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def file_record(path: str | Path, base_dir: Path | None = None) -> dict[str, object]:
    raw_path = Path(path)
    resolved_path = (
        raw_path if raw_path.is_absolute() or base_dir is None else base_dir / raw_path
    )
    record: dict[str, object] = {
        "path": str(resolved_path),
        "exists": resolved_path.exists(),
    }
    if not resolved_path.exists():
        return record

    stat = resolved_path.stat()
    record.update(
        {
            "type": "dir" if resolved_path.is_dir() else "file",
            "size_bytes": stat.st_size if resolved_path.is_file() else None,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, UTC)
            .isoformat()
            .replace("+00:00", "Z"),
        }
    )
    if resolved_path.is_file():
        record["sha256"] = sha256_file(resolved_path)
    return record


def build_file_manifest(
    paths: Iterable[str | Path], base_dir: Path | None = None
) -> list[dict[str, object]]:
    seen: set[str] = set()
    manifest: list[dict[str, object]] = []
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        manifest.append(file_record(path, base_dir=base_dir))
    return manifest


def discover_deliverables(
    root: str | Path,
    patterns: Sequence[str] = DEFAULT_DELIVERABLE_PATTERNS,
    *,
    recursive: bool = True,
    max_files: int | None = 500,
) -> list[dict[str, object]]:
    root_path = Path(root)
    if not root_path.exists() or not root_path.is_dir():
        return []

    files: dict[str, Path] = {}
    for pattern in patterns:
        iterator = root_path.rglob(pattern) if recursive else root_path.glob(pattern)
        for path in iterator:
            if path.is_file():
                files[str(path)] = path

    ordered = sorted(files.values(), key=lambda p: str(p).lower())
    if max_files is not None:
        ordered = ordered[:max_files]
    return build_file_manifest(ordered)


# ---------------------------------------------------------------------------
# Registro de operações de ingestão
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _safe_user() -> str:
    try:
        return getpass.getuser()
    except Exception:
        return "desconhecido"


def record_operation(
    operation: str,
    *,
    status: str,
    grupo: str | None = None,
    projeto: str | None = None,
    source_files: Iterable[str | Path] | None = None,
    metrics: dict[str, object] | None = None,
    issues: dict[str, object] | None = None,
    extra: dict[str, object] | None = None,
    audit_dir: Path | None = None,
) -> dict[str, object]:
    """
    Grava um registro JSON de auditoria de uma operação de ingestão.

    Parameters
    ----------
    operation : str
        Tipo da operação: "cadastro", "importacao", "validacao", "migracao",
        "consolidacao".
    status : str
        Resultado: "ok", "bloqueado", "erro", "parcial".
    grupo : str, optional
        Grupo biológico (Ictiofauna, Bentos, ...).
    projeto : str, optional
        Código/nome do projeto.
    source_files : iterable of path, optional
        Arquivos de origem — entram no manifesto com hash sha256.
    metrics : dict, optional
        Contadores (campanhas, pontos, registros, etc.).
    issues : dict, optional
        Resumo de achados (n_blocks, n_warnings, ...).
    extra : dict, optional
        Campos adicionais arbitrários.
    audit_dir : Path, optional
        Diretório de saída (default: runtime/audit/).

    Returns
    -------
    dict
        O registro gravado (também persistido em disco como JSON).
    """
    audit_dir = audit_dir or AUDIT_DIR
    audit_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _now_iso()
    record: dict[str, object] = {
        "id": uuid.uuid4().hex,
        "timestamp": timestamp,
        "operation": operation,
        "status": status,
        "grupo": grupo,
        "projeto": projeto,
        "usuario": _safe_user(),
        "git": git_context(),
        "source_manifest": build_file_manifest(list(source_files)) if source_files else [],
        "metrics": metrics or {},
        "issues": issues or {},
    }
    if extra:
        record["extra"] = extra

    safe_op = re.sub(r"[^a-z0-9]+", "-", operation.lower()).strip("-") or "op"
    fname = f"{timestamp.replace(':', '').replace('-', '').replace('Z', '')}_{safe_op}_{record['id'][:8]}.json"
    out_path = audit_dir / fname
    out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    record["_audit_path"] = str(out_path)
    return record


def load_audit_log(audit_dir: Path | None = None, limit: int | None = None) -> list[dict[str, object]]:
    """Lê os registros de auditoria existentes (mais recentes primeiro)."""
    audit_dir = audit_dir or AUDIT_DIR
    if not audit_dir.exists():
        return []
    files = sorted(audit_dir.glob("*.json"), key=lambda p: p.name, reverse=True)
    if limit is not None:
        files = files[:limit]
    records: list[dict[str, object]] = []
    for path in files:
        try:
            records.append(json.loads(path.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError):
            continue
    return records
