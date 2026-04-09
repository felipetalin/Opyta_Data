"""
validators/especies/db_checker.py
Compara os nomes científicos da planilha com o banco de dados.
Classifica cada linha como 'new', 'existing' ou 'conflict'
(já cadastrado com grupo biológico diferente).
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .normalizer import normalized_key
from .report import ValidationIssue, ValidationReport


@dataclass
class SpeciesStatus:
    nome_cientifico: str
    status: str              # "new" | "existing" | "conflict"
    db_grupo: str | None = None   # grupo biológico encontrado no banco


def check_against_db(
    df: pd.DataFrame,
    engine: Engine,
    report: ValidationReport,
) -> list[SpeciesStatus]:
    """
    Carrega os nomes científicos existentes no banco e compara linha a linha
    com o DataFrame (já normalizado).

    Preenche `report.total_new`, `report.total_existing`,
    `report.new_species`, `report.existing_species`.

    Retorna lista de SpeciesStatus, uma por linha de entrada válida.

    Conflitos de grupo biológico (mesmo nome, grupo diferente) geram *warning*.
    """
    if "Nome_Cientifico" not in df.columns:
        return []

    # --- 1. Carregar catálogo atual do banco
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT nome_cientifico, grupo_biologico FROM especies")
            ).fetchall()
    except Exception as exc:
        report.issues.append(
            ValidationIssue(
                code="DB_QUERY_ERROR",
                severity="warning",
                message=f"Não foi possível consultar o banco para comparação: {exc}. "
                        "A validação seguiu sem comparação com banco.",
            )
        )
        return []

    # Índice: chave normalizada → (nome_original, grupo)
    db_index: dict[str, tuple[str, str | None]] = {
        normalized_key(r[0]): (r[0], r[1])
        for r in rows
        if r[0]
    }

    # --- 2. Classificar cada linha
    statuses: list[SpeciesStatus] = []
    valid_count = 0

    for idx, row in df.iterrows():
        nc = row.get("Nome_Cientifico")
        gb = row.get("Grupo_Biologico")

        if pd.isna(nc) or not isinstance(nc, str) or nc.strip() == "":
            continue  # linha inválida — já bloqueada pelas regras

        valid_count += 1
        key = normalized_key(nc)

        if key in db_index:
            _, db_grupo = db_index[key]
            gb_norm = normalized_key(gb) if isinstance(gb, str) else ""
            db_norm = normalized_key(db_grupo) if db_grupo else ""

            if gb_norm and db_norm and gb_norm != db_norm:
                # Conflito de grupo biológico
                statuses.append(
                    SpeciesStatus(
                        nome_cientifico=nc,
                        status="conflict",
                        db_grupo=db_grupo,
                    )
                )
                report.issues.append(
                    ValidationIssue(
                        code="GROUP_CONFLICT",
                        severity="warning",
                        message=(
                            f"Linha {idx + 2}: '{nc}' já está no banco com "
                            f"grupo '{db_grupo}', mas a planilha informa '{gb}'. "
                            "O registro será atualizado com o valor da planilha."
                        ),
                        row=idx + 2,
                        column="Grupo_Biologico",
                    )
                )
            else:
                statuses.append(
                    SpeciesStatus(
                        nome_cientifico=nc,
                        status="existing",
                        db_grupo=db_grupo,
                    )
                )
                report.existing_species.append(nc)
        else:
            statuses.append(
                SpeciesStatus(nome_cientifico=nc, status="new")
            )
            report.new_species.append(nc)

    report.total_valid = valid_count
    report.total_new = len(report.new_species)
    report.total_existing = len(report.existing_species)

    return statuses
