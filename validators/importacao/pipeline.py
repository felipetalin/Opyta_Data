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

from sqlalchemy.engine import Engine

from .checkers import (
    check_especies_no_banco,
    check_esforco,
    check_pontos,
    check_resultados_vs_esforco,
    check_referencias_cruzadas,
)
from .reader import read_sheets
from .report import ValidationReport


def validate_importacao_file(
    source: str | Path | BytesIO,
    group: str,
    engine: Engine | None = None,
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
    check_pontos(report.df_pontos, report)
    check_esforco(report.df_esforco, report)
    check_resultados_vs_esforco(report.df_resultados, report.df_esforco, group, report)
    check_referencias_cruzadas(report.df_resultados, report.df_pontos, report)

    # --- Etapa 3: Validações que dependem do banco ---
    if engine is not None:
        check_especies_no_banco(report.df_resultados, engine, report)

    return report
