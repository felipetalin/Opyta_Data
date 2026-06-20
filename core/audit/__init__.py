"""
core/audit
==========

Camada de auditoria e rastreabilidade (lineage) portada da evolução do
repositório Opyta_Data_Analysis (`src/opyta_analysis/audit_utils.py`).

Fornece primitivas para registrar, de forma reproduzível e auditável, cada
operação relevante de ingestão (cadastro, importação, validação, migração,
consolidação):

- `git_context()`     — branch/commit/dirty do repositório no momento da operação
- `sha256_file()`     — hash de integridade de um arquivo
- `file_record()`     — metadados (tamanho, mtime, sha256) de um arquivo
- `build_file_manifest()` / `discover_deliverables()` — manifestos de arquivos
- `record_operation()` — grava um registro JSON de auditoria em `runtime/audit/`

Esta é a base para o painel "Qualidade & Auditoria de Dados" (Bloco 5 do
PLANO_MELHORIAS_STREAMLIT.md), até então não construído.
"""
from __future__ import annotations

from .manifest import (
    DEFAULT_DELIVERABLE_PATTERNS,
    build_file_manifest,
    discover_deliverables,
    file_record,
    git_context,
    load_audit_log,
    record_operation,
    sha256_file,
)

__all__ = [
    "DEFAULT_DELIVERABLE_PATTERNS",
    "build_file_manifest",
    "discover_deliverables",
    "file_record",
    "git_context",
    "load_audit_log",
    "record_operation",
    "sha256_file",
]
