"""
validators/especies/reader.py
Responsável exclusivamente por abrir o arquivo Excel e retornar
o DataFrame bruto da aba 'Especies', sem qualquer transformação.
"""
from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd

from .report import ValidationIssue, ValidationReport

# Nome exato da aba esperada (case-sensitive)
SHEET_NAME = "Especies"

# Colunas mínimas que devem existir para o processo continuar
REQUIRED_COLUMNS = {"Nome_Cientifico", "Grupo_Biologico"}

# Todas as colunas reconhecidas pelo sistema (superset)
KNOWN_COLUMNS = {
    "Nome_Cientifico",
    "Nome_Popular",
    "Grupo_Biologico",
    "Reino",
    "Filo",
    "Classe",
    "Ordem",
    "Familia",
    "Genero",
    "Autor_e_Ano",
    "Status_Ameaca_Nacional",
    "Status_Ameaca_Global",
    "Origem",
    "Habito_Alimentar",
    "Estrategia_Reprodutiva",
    "Valor_Economico",
    "Observacoes",
    "BMWP_Score",
    # Colunas opcionais de fauna terrestre (migration 002)
    "Status_Estadual",
    "Status_Copam",
    "Cites",
    "Guilda_Alimentar",
    "Dependencia_Florestal",
    "Endemismo",
    "Sensibilidade_Ambiental",
    "Migratorio",
    "Raridade",
}


def read_sheet(
    source: str | Path | BytesIO,
    report: ValidationReport,
) -> pd.DataFrame | None:
    """
    Abre o arquivo Excel e retorna o DataFrame bruto da aba 'Especies'.

    Erros estruturais (arquivo inválido, aba ausente, colunas obrigatórias
    ausentes) são registrados como *block* no `report`.  Colunas extras
    desconhecidas geram *warning*.

    Retorna None se qualquer bloqueio estrutural for detectado.
    """
    # 1. Tentar abrir o arquivo
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
        return None

    # 2. Verificar presença da aba
    if SHEET_NAME not in xls.sheet_names:
        available = ", ".join(xls.sheet_names) or "(nenhuma)"
        report.issues.append(
            ValidationIssue(
                code="SHEET_NOT_FOUND",
                severity="block",
                message=(
                    f"Aba '{SHEET_NAME}' não encontrada. "
                    f"Abas disponíveis: {available}."
                ),
            )
        )
        return None

    # 3. Ler a aba
    try:
        df = xls.parse(SHEET_NAME, dtype=str)
    except Exception as exc:
        report.issues.append(
            ValidationIssue(
                code="SHEET_PARSE_ERROR",
                severity="block",
                message=f"Erro ao ler a aba '{SHEET_NAME}': {exc}",
            )
        )
        return None

    # 4. Remover linhas totalmente em branco
    df = df.dropna(how="all").reset_index(drop=True)

    if df.empty:
        report.issues.append(
            ValidationIssue(
                code="EMPTY_SHEET",
                severity="block",
                message=f"A aba '{SHEET_NAME}' está vazia (sem dados).",
            )
        )
        return None

    # 5. Verificar colunas obrigatórias
    present = set(df.columns)
    missing_required = REQUIRED_COLUMNS - present
    if missing_required:
        report.issues.append(
            ValidationIssue(
                code="MISSING_REQUIRED_COLUMNS",
                severity="block",
                message=(
                    f"Colunas obrigatórias ausentes: "
                    f"{sorted(missing_required)}. "
                    "Verifique o cabeçalho da planilha."
                ),
            )
        )
        return None

    # 6. Avisar sobre colunas completamente desconhecidas
    unknown = present - KNOWN_COLUMNS
    if unknown:
        report.issues.append(
            ValidationIssue(
                code="UNKNOWN_COLUMNS",
                severity="warning",
                message=(
                    f"Colunas não reconhecidas serão ignoradas: "
                    f"{sorted(unknown)}."
                ),
            )
        )

    report.total_rows = len(df)
    return df
