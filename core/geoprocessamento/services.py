from __future__ import annotations

import pandas as pd
import streamlit as st

from core.engine import get_engine
from .indicators import calcular_indicadores_por_ponto
from .queries import (
    ModoGeo,
    get_geo_biota,
    get_geo_fisico,
    listar_campanhas,
    listar_grupos_biologicos,
    listar_projetos,
)


def _pick_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    existing = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name.lower() in existing:
            return existing[name.lower()]
    return None


def _padronizar_colunas_biota(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    required_defaults: dict[str, object] = {
        "projeto": "",
        "campanha": "",
        "ponto": "",
        "latitude": None,
        "longitude": None,
        "grupo_biologico": "",
        "nome_cientifico": "",
        "contagem": 0,
        "biomassa": 0,
        "ordem": "",
        "bmwp_score": 0,
    }

    alias_map: dict[str, list[str]] = {
        "ordem": ["ordem", "ordem_taxonomica", "order", "taxon_order"],
        "bmwp_score": ["bmwp_score", "bmwp", "pontuacao_bmwp", "bmwp_pontuacao"],
        "nome_cientifico": ["nome_cientifico", "taxon", "especie", "scientific_name"],
        "grupo_biologico": ["grupo_biologico", "grupo", "grupo_biota"],
        "contagem": ["contagem", "abundancia", "quantidade", "n_individuos"],
        "biomassa": ["biomassa", "peso_total", "massa", "biomass"],
    }

    for target, candidates in alias_map.items():
        if target in work.columns:
            continue
        src = _pick_first_existing_column(work, candidates)
        if src:
            work[target] = work[src]

    for col, default in required_defaults.items():
        if col not in work.columns:
            work[col] = default

    return work


def _corrigir_coordenadas_invertidas(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "latitude" not in work.columns or "longitude" not in work.columns:
        return work

    # Heuristica para dados no Brasil: lat ~ [-35, 10], lon ~ [-75, -30].
    mask_swap = work["latitude"].between(-75, -30) & work["longitude"].between(-35, 10)
    if mask_swap.any():
        lat_old = work.loc[mask_swap, "latitude"].copy()
        work.loc[mask_swap, "latitude"] = work.loc[mask_swap, "longitude"]
        work.loc[mask_swap, "longitude"] = lat_old
    return work


@st.cache_data(show_spinner=False)
def carregar_projetos(modo: ModoGeo) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_projetos(conn, modo)


@st.cache_data(show_spinner=False)
def carregar_campanhas(modo: ModoGeo, projetos: list[str]) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_campanhas(conn, modo, projetos)


@st.cache_data(show_spinner=False)
def carregar_grupos_biologicos(projetos: list[str], campanhas: list[str]) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_grupos_biologicos(conn, projetos, campanhas)


@st.cache_data(show_spinner=False)
def carregar_dados_geo(
    modo: ModoGeo,
    projetos: list[str],
    campanhas: list[str],
    grupos_biologicos: list[str] | None = None,
) -> pd.DataFrame:
    engine = get_engine()

    with engine.connect() as conn:
        if modo == "Fisico":
            df = get_geo_fisico(conn, projetos, campanhas)
        else:
            df = get_geo_biota(conn, projetos, campanhas, grupos_biologicos)

    if df.empty:
        return df

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    df = _corrigir_coordenadas_invertidas(df)
    df = df[df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)].copy()

    if modo == "Biota":
        df = _padronizar_colunas_biota(df)
        return calcular_indicadores_por_ponto(df)

    df["pontos_amostrados"] = 1

    return df


def calcular_resumo_geo(df: pd.DataFrame, modo: ModoGeo) -> dict[str, float]:
    if df.empty:
        base = {
            "projetos": 0,
            "campanhas": 0,
            "pontos": 0,
            "riqueza_media": 0.0,
            "abundancia_total": 0.0,
            "biomassa_total": 0.0,
            "shannon_medio": 0.0,
            "pielou_medio": 0.0,
            "bmwp_total": 0.0,
            "riqueza_ept_total": 0.0,
            "abundancia_ept_total": 0.0,
        }
        return base

    resumo = {
        "projetos": float(df["projeto"].nunique()) if "projeto" in df.columns else 0.0,
        "campanhas": float(df["campanha"].nunique()) if "campanha" in df.columns else 0.0,
        "pontos": float(df["ponto"].nunique()) if "ponto" in df.columns else 0.0,
    }

    if modo == "Biota":
        resumo.update(
            {
                "riqueza_media": float(df["riqueza"].mean()) if "riqueza" in df.columns and not df.empty else 0.0,
                "abundancia_total": float(df["abundancia_total"].sum()) if "abundancia_total" in df.columns else 0.0,
                "biomassa_total": float(df["biomassa_total"].sum()) if "biomassa_total" in df.columns else 0.0,
                "shannon_medio": float(df["shannon"].mean()) if "shannon" in df.columns and not df.empty else 0.0,
                "pielou_medio": float(df["pielou"].mean()) if "pielou" in df.columns and not df.empty else 0.0,
                "bmwp_total": float(df["bmwp_total"].sum()) if "bmwp_total" in df.columns else 0.0,
                "riqueza_ept_total": float(df["riqueza_ept"].sum()) if "riqueza_ept" in df.columns else 0.0,
                "abundancia_ept_total": float(df["abundancia_ept"].sum()) if "abundancia_ept" in df.columns else 0.0,
            }
        )
    else:
        resumo.update(
            {
                "riqueza_media": float(df["ponto"].nunique()) if "ponto" in df.columns else 0.0,
                "abundancia_total": float(df["pontos_amostrados"].sum()) if "pontos_amostrados" in df.columns else 0.0,
                "biomassa_total": 0.0,
                "shannon_medio": 0.0,
                "pielou_medio": 0.0,
                "bmwp_total": 0.0,
                "riqueza_ept_total": 0.0,
                "abundancia_ept_total": 0.0,
            }
        )

    return resumo
