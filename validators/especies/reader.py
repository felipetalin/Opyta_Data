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
    "Status_IUCN",
    "Status_MMA",
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
    "Status_Ameaca_Estadual",  # alias aceito → mapeado para Status_Estadual
    "Status_Copam",
    "Status_COPAM",
    "Cites",
    "CITES",
    "Guilda_Alimentar",
    "Dependencia_Florestal",
    "Endemismo",
    "Sensibilidade_Ambiental",
    "Migratorio",
    "Raridade",
}

# Aliases aceitos: variações de nome que o usuário pode enviar na planilha.
# Cada entrada é renomeada para o nome canônico antes de qualquer validação.
# Isso garante que colunas com grafia alternativa migrem normalmente para o banco.
_COLUMN_ALIASES: dict[str, str] = {
    # BMWP: aceita lowercase e variações sem underscore
    "bmwp_score": "BMWP_Score",
    "bmwp": "BMWP_Score",
    # Status estadual: aceita nome completo com ameaca
    "Status_Ameaca_Estadual": "Status_Estadual",
    "status_ameaca_estadual": "Status_Estadual",
    "Status_IUCN": "Status_Ameaca_Global",
    "status_iucn": "Status_Ameaca_Global",
    "Status_MMA": "Status_Ameaca_Nacional",
    "status_mma": "Status_Ameaca_Nacional",
    "Status_COPAM": "Status_Copam",
    "status_copam": "Status_Copam",
    "CITES": "Cites",
    "cites": "Cites",
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

    # 4b. Normalizar aliases de colunas para nomes canônicos
    alias_map = {
        col: _COLUMN_ALIASES[col]
        for col in df.columns
        if col in _COLUMN_ALIASES and col != _COLUMN_ALIASES[col]
    }
    if alias_map:
        df = df.rename(columns=alias_map)
        for original, canonical in alias_map.items():
            report.issues.append(
                ValidationIssue(
                    code="COLUMN_ALIAS_NORMALIZED",
                    severity="info",
                    message=(
                        f"Coluna '{original}' reconhecida como '{canonical}' "
                        "e será processada normalmente."
                    ),
                )
            )

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
