"""
validators/importacao/checkers.py
Validações específicas: pontos, espécies, esforço, referências cruzadas.
"""
from __future__ import annotations

import re

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from .report import ValidationIssue, ValidationReport


def check_pontos(df_pontos: pd.DataFrame, report: ValidationReport) -> None:
    """Valida coordenadas e nomes de pontos de coleta."""
    if df_pontos is None or df_pontos.empty:
        return

    # Procura colunas de coordenada
    lat_col = None
    lon_col = None
    for col in df_pontos.columns:
        col_lower = col.lower()
        if "latitude" in col_lower or "lat" in col_lower:
            lat_col = col
        elif "longitude" in col_lower or "lon" in col_lower or "x" in col_lower:
            lon_col = col

    if lat_col and lon_col:
        invalid_coords = []
        for idx, row in df_pontos.iterrows():
            try:
                lat = float(str(row[lat_col]).replace(",", ".").strip())
                lon = float(str(row[lon_col]).replace(",", ".").strip())
                if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                    invalid_coords.append(idx + 2)
            except (ValueError, TypeError):
                invalid_coords.append(idx + 2)

        if invalid_coords:
            report.issues.append(
                ValidationIssue(
                    code="INVALID_COORDINATES",
                    severity="warning",
                    message=(
                        f"Coordenadas inválidas nas linhas: {invalid_coords[:5]}. "
                        f"Latitude deve estar em [-90, 90] e longitude em [-180, 180]."
                    ),
                    lines=invalid_coords[:10],
                )
            )


def check_especies_no_banco(
    df_resultados: pd.DataFrame,
    engine: Engine,
    report: ValidationReport,
) -> None:
    """
    Compara especies nos resultados com o banco de dados.
    Registra quais não foram encontradas (warning, não bloqueia).
    """
    if df_resultados is None or df_resultados.empty:
        return

    # Procura coluna de espécie
    species_col = None
    for col in df_resultados.columns:
        col_lower = col.lower()
        if "especie" in col_lower or "nome_cientifico" in col_lower or "taxa" in col_lower:
            species_col = col
            break

    if not species_col:
        report.issues.append(
            ValidationIssue(
                code="NO_SPECIES_COLUMN",
                severity="warning",
                message="Não foi encontrada coluna de espécies nos resultados.",
            )
        )
        return

    # Carrega especies do banco
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT LOWER(nome_cientifico) FROM especies")
            ).fetchall()
        db_species = {r[0] for r in rows}
    except Exception as exc:
        report.issues.append(
            ValidationIssue(
                code="DB_QUERY_ERROR",
                severity="warning",
                message=f"Erro ao carregar espécies do banco: {exc}. Validação de espécies pulada.",
            )
        )
        return

    # Comparar
    unknown = set()
    for idx, value in df_resultados[species_col].items():
        if pd.isna(value) or not isinstance(value, str) or value.strip() == "":
            continue
        value_norm = value.strip().lower()
        if value_norm not in db_species:
            unknown.add(value.strip())

    if unknown:
        report.total_especies_desconhecidas = len(unknown)
        report.especies_desconhecidas = sorted(unknown)[:50]  # Top 50
        report.issues.append(
            ValidationIssue(
                code="UNKNOWN_SPECIES",
                severity="info",
                message=(
                    f"Total de {len(unknown)} espécie(s) desconhecida(s) nos resultados. "
                    f"Serão cadastradas automaticamente ou necessitarem revisão."
                ),
            )
        )


def check_esforco(df_esforco: pd.DataFrame, report: ValidationReport) -> None:
    """Valida dias de esforço (valores numéricos)."""
    if df_esforco is None or df_esforco.empty:
        return

    # Procura coluna de esforço
    esforco_col = None
    for col in df_esforco.columns:
        col_lower = col.lower()
        if "esforco" in col_lower or "dias" in col_lower or "effort" in col_lower:
            esforco_col = col
            break

    if not esforco_col:
        return

    total_dias = 0.0
    invalid_rows = []

    for idx, value in df_esforco[esforco_col].items():
        if pd.isna(value) or (isinstance(value, str) and value.strip() == ""):
            continue
        try:
            dias = float(str(value).strip().replace(",", "."))
            if dias < 0:
                invalid_rows.append(idx + 2)
            else:
                total_dias += dias
        except (ValueError, TypeError):
            invalid_rows.append(idx + 2)

    report.total_esforco_dias = total_dias

    if invalid_rows:
        report.issues.append(
            ValidationIssue(
                code="INVALID_EFFORT",
                severity="warning",
                message=(
                    f"Valores inválidos de esforço nas linhas: {invalid_rows[:5]}. "
                    f"Esforço deve ser numérico e não-negativo."
                ),
                lines=invalid_rows[:10],
            )
        )


def check_referencias_cruzadas(
    df_resultados: pd.DataFrame,
    df_pontos: pd.DataFrame,
    report: ValidationReport,
) -> None:
    """Valida se pontos referenciados existem em Pontos_e_Campanhas."""
    if df_resultados is None or df_resultados.empty or df_pontos is None:
        return

    # Procura coluna de ponto nos resultados
    ponto_col = None
    for col in df_resultados.columns:
        col_lower = col.lower()
        if "ponto" in col_lower:
            ponto_col = col
            break

    if not ponto_col:
        return

    # Pontos válidos
    if "Ponto" in df_pontos.columns or "ponto" in df_pontos.columns:
        ponto_col_ref = "Ponto" if "Ponto" in df_pontos.columns else "ponto"
        valid_pontos = set(df_pontos[ponto_col_ref].dropna().astype(str).unique())
    else:
        return

    # Verificar
    missing_pontos = []
    for idx, value in df_resultados[ponto_col].items():
        if pd.isna(value):
            continue
        if str(value).strip() not in valid_pontos:
            missing_pontos.append((idx + 2, str(value).strip()))

    if missing_pontos:
        report.issues.append(
            ValidationIssue(
                code="INVALID_POINT_REFERENCE",
                severity="warning",
                message=(
                    f"Pontos referenciados não encontrados em Pontos_e_Campanhas: "
                    f"{[p[1] for p in missing_pontos[:5]]}. "
                    f"Total: {len(missing_pontos)}."
                ),
                lines=[p[0] for p in missing_pontos[:10]],
            )
        )
