"""
validators/especies/pipeline.py
Orquestrador: une reader → normalizer → rules → db_checker
em sequência, retornando um ValidationReport completo.

Uso típico:
    from validators.especies import validate_especies_file
    report = validate_especies_file(uploaded_file, engine)
    if report.can_proceed:
        run_cadastro(report.cleaned_df, engine)
"""
from __future__ import annotations

from io import BytesIO
from pathlib import Path

from sqlalchemy.engine import Engine

from .db_checker import check_against_db
from .normalizer import normalize
from .reader import read_sheet
from .report import ValidationReport
from .rules import run_rules


def validate_especies_file(
    source: str | Path | BytesIO,
    engine: Engine | None = None,
) -> ValidationReport:
    """
    Executa o pipeline completo de validação para a planilha de cadastro.

    Parâmetros
    ----------
    source : caminho do arquivo, Path ou BytesIO (upload Streamlit)
    engine : SQLAlchemy Engine para comparação com banco.
             Se None, a etapa de comparação é omitida e apenas validações
             locais são feitas.

    Retorno
    -------
    ValidationReport com todos os resultados.
    `report.can_proceed` indica se a planilha pode ser enviada para cadastro.
    `report.cleaned_df` contém o DataFrame normalizado (se sem bloqueios).
    """
    report = ValidationReport()

    # --- Etapa 1: Leitura estrutural ---
    df_raw = read_sheet(source, report)
    if df_raw is None:
        # Erro estrutural grave: não há como continuar
        return report

    # --- Etapa 2: Normalização / limpeza automática ---
    df_clean = normalize(df_raw, report)

    # --- Etapa 3: Regras de validação ---
    run_rules(df_clean, report)

    # --- Etapa 4: Comparação com banco (se engine disponível) ---
    if engine is not None:
        check_against_db(df_clean, engine, report)
    else:
        # Sem banco: counts ficam 0, new_species vazio
        # Marcar valid_count ao menos com o total de linhas não-bloqueadas
        blocks_by_row = {i.row for i in report.blocks if i.row is not None}
        valid_count = sum(
            1
            for idx in df_clean.index
            if (idx + 2) not in blocks_by_row
        )
        report.total_valid = valid_count

    # --- Persiste DataFrame limpo no report apenas se não há bloqueios ---
    if report.can_proceed:
        report.cleaned_df = df_clean

    return report
