"""
validators/importacao/pipeline.py
Orquestrador: une reader → checkers em sequência.

Uso típico:
    from validators.importacao import validate_importacao_file
    report = validate_importacao_file(uploaded_file, group="Zooplâncton", engine=get_engine())
    if report.can_proceed:
        run_migracao(engine)
"""
from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from .checkers import (
    check_campaign_consistency,
    check_especies_no_banco,
    check_esforco,
    check_pontos_conflitantes_no_banco,
    check_pontos,
    check_resultados_vs_esforco,
    check_referencias_cruzadas,
)
from .reader import read_sheets
from .report import ValidationIssue, ValidationReport
from ..especies.pipeline import validate_especies_file


def _validate_embedded_species_catalog(
    report: ValidationReport,
    engine: Engine | None,
) -> set[str]:
    """Valida Cadastro_Especies/Especies e retorna especies aceitas no arquivo."""
    df_species = getattr(report, "df_cadastro_especies", None)
    if df_species is None or getattr(df_species, "empty", True):
        return set()

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df_species.to_excel(writer, sheet_name="Especies", index=False)
    bio.seek(0)

    species_report = validate_especies_file(bio, engine=engine)
    report.total_cadastro_especies = species_report.total_rows
    report.total_cadastro_especies_novas = species_report.total_new
    report.total_cadastro_especies_existentes = species_report.total_existing

    for issue in species_report.blocks:
        report.issues.append(
            ValidationIssue(
                code=f"CADASTRO_ESPECIES_{issue.code}",
                severity="block",
                message=f"Cadastro_Especies: {issue.message}",
                lines=[issue.row] if issue.row is not None else [],
                sheet="cadastro_especies",
            )
        )

    for issue in species_report.warnings:
        report.issues.append(
            ValidationIssue(
                code=f"CADASTRO_ESPECIES_{issue.code}",
                severity="warning",
                message=f"Cadastro_Especies: {issue.message}",
                lines=[issue.row] if issue.row is not None else [],
                sheet="cadastro_especies",
            )
        )

    report.issues.append(
        ValidationIssue(
            code="CADASTRO_ESPECIES_VALIDADO",
            severity="info",
            message=(
                "Cadastro_Especies validado: "
                f"{species_report.total_rows} linha(s), "
                f"{species_report.total_new} nova(s), "
                f"{species_report.total_existing} ja existente(s)."
            ),
        )
    )

    if not species_report.can_proceed:
        return set()

    df_clean = species_report.cleaned_df
    if df_clean is None or "Nome_Cientifico" not in df_clean.columns:
        return set()
    return {
        str(value).strip()
        for value in df_clean["Nome_Cientifico"].dropna().tolist()
        if str(value).strip()
    }


def validate_importacao_file(
    source: str | Path | BytesIO,
    group: str,
    engine: Engine | None = None,
    allowed_species: set[str] | None = None,
    strict_unknown_species: bool = False,
) -> ValidationReport:
    """
    Executa o pipeline completo de validação para arquivo de importação.

    Parâmetros
    ----------
    source : caminho do arquivo, Path ou BytesIO
    group : nome do grupo biológico (p.ex. "Zooplâncton", "Bentos")
    engine : SQLAlchemy Engine para consultar banco de dados.
             Se None, validações que dependem do banco são puladas.

    Retorno
    -------
    ValidationReport com todos os resultados.
    `report.can_proceed` indica se o arquivo pode ser migrado.
    """
    report = ValidationReport()

    # --- Etapa 1: Leitura estrutural ---
    success = read_sheets(source, group, report)
    if not success:
        return report

    # --- Etapa 2: Validações de dados ---
    embedded_allowed_species = _validate_embedded_species_catalog(report, engine)
    check_campaign_consistency(report.df_pontos, report.df_esforco, report.df_resultados, report)
    check_pontos(report.df_pontos, report)
    check_esforco(report.df_esforco, report)
    check_resultados_vs_esforco(report.df_resultados, report.df_esforco, group, report)
    check_referencias_cruzadas(report.df_resultados, report.df_pontos, report)

    # --- Etapa 3: Validações que dependem do banco ---
    if engine is not None:
        check_pontos_conflitantes_no_banco(report.df_capa, report.df_pontos, engine, report)
        check_especies_no_banco(
            report.df_resultados,
            engine,
            report,
            allowed_species=set(allowed_species or set()) | embedded_allowed_species,
            strict_unknown_species=strict_unknown_species,
        )

    return report
