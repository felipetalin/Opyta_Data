"""
validators/importacao/reader.py
Lê o arquivo Excel de importação e retorna os DataFrames das abas necessárias.
"""
from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd

from .report import ValidationIssue, ValidationReport

# Abas esperadas conforme o grupo
REQUIRED_SHEETS_BY_GROUP: dict[str, list[str]] = {
    "Ictiofauna": ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Ictiofauna"],
    "Bentos": ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Zoobentos"],
    "Fitoplâncton": ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Fitoplancton"],
    "Zooplâncton": ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Zooplancton"],
    "Avifauna": ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Avifauna"],
    "Herpetofauna": ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Herpetofauna"],
    "Mastofauna": ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Mastofauna"],
}


def read_sheets(
    source: str | Path | BytesIO,
    group: str,
    report: ValidationReport,
) -> bool:
    """
    Abre o arquivo Excel e carrega os DataFrames das abas esperadas.

    Retorna True se tudo OK; False se erro estrutural.
    Erros são registrados como *block* no `report`.
    Os DataFrames são guardados em report.df_* (se sem bloqueios).
    """
    # 1. Abrir arquivo
    try:
        xls = pd.ExcelFile(source)
    except Exception as exc:
        report.issues.append(
            ValidationIssue(
                code="FILE_OPEN_ERROR",
                severity="block",
                message=f"Não foi possível abrir o arquivo: {exc}",
            )
        )
        return False

    # 2. Validar abas esperadas
    expected_sheets = REQUIRED_SHEETS_BY_GROUP.get(group)
    if not expected_sheets:
        report.issues.append(
            ValidationIssue(
                code="GROUP_NOT_RECOGNIZED",
                severity="block",
                message=(
                    f"Grupo '{group}' não está no catálogo de grupos reconhecidos. "
                    f"Grupos válidos: {', '.join(REQUIRED_SHEETS_BY_GROUP.keys())}."
                ),
            )
        )
        return False

    available = set(xls.sheet_names)
    missing = set(expected_sheets) - available
    if missing:
        report.issues.append(
            ValidationIssue(
                code="MISSING_SHEETS",
                severity="block",
                message=(
                    f"Abas ausentes: {sorted(missing)}. "
                    f"Esperado: {expected_sheets}."
                ),
            )
        )
        return False

    # 3. Carregar DataFrames
    try:
        df_capa = xls.parse("Capa_Projeto", dtype=str).dropna(how="all")
        df_pontos = xls.parse("Pontos_e_Campanhas", dtype=str).dropna(how="all")
        df_esforco = xls.parse("Metadados_Esforco", dtype=str).dropna(how="all")
        
        # Nome da aba de resultados deve seguir o grupo selecionado.
        result_sheet = expected_sheets[3]
        if result_sheet not in available:
            report.issues.append(
                ValidationIssue(
                    code="RESULTS_SHEET_NOT_FOUND",
                    severity="block",
                    message=(
                        f"Aba de resultados esperada para o grupo '{group}' não encontrada: "
                        f"'{result_sheet}'."
                    ),
                )
            )
            return False

        df_resultados = xls.parse(result_sheet, dtype=str).dropna(how="all")
        species_sheet = "Cadastro_Especies" if "Cadastro_Especies" in available else None
        if species_sheet is None and "Especies" in available:
            species_sheet = "Especies"
        df_cadastro_especies = (
            xls.parse(species_sheet, dtype=str).dropna(how="all")
            if species_sheet
            else None
        )

    except Exception as exc:
        report.issues.append(
            ValidationIssue(
                code="SHEET_PARSE_ERROR",
                severity="block",
                message=f"Erro ao ler abas do Excel: {exc}",
            )
        )
        return False

    # 4. Validar não-vazios
    if df_capa.empty:
        report.issues.append(
            ValidationIssue(
                code="EMPTY_CAPA",
                severity="block",
                message="Aba 'Capa_Projeto' está vazia.",
            )
        )
        return False

    if df_pontos.empty:
        report.issues.append(
            ValidationIssue(
                code="EMPTY_PONTOS",
                severity="block",
                message="Aba 'Pontos_e_Campanhas' está vazia.",
            )
        )
        return False

    if df_resultados.empty:
        report.issues.append(
            ValidationIssue(
                code="EMPTY_RESULTADOS",
                severity="block",
                message=f"Aba '{result_sheet}' está vazia.",
            )
        )
        return False

    # 5. Guardar DataFrames no report
    report.df_capa = df_capa
    report.df_pontos = df_pontos
    report.df_esforco = df_esforco
    report.df_resultados = df_resultados
    report.df_cadastro_especies = df_cadastro_especies

    # 6. Contar registros
    report.total_campanhas = len(df_pontos["Campanha"].unique()) if "Campanha" in df_pontos else 0
    report.total_pontos = len(df_pontos) if "Ponto" in df_pontos else 0
    report.total_registros = len(df_resultados)
    report.total_cadastro_especies = (
        len(df_cadastro_especies) if df_cadastro_especies is not None else 0
    )

    return True
