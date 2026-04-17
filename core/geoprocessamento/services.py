from __future__ import annotations

import pandas as pd
import streamlit as st

from core.engine import get_engine
from .indicators import calcular_indicadores_fisicos_por_ponto, calcular_indicadores_por_ponto
from .queries import (
    ModoGeo,
    get_geo_biota,
    get_geo_fisico,
    get_geo_fisico_com_empreendimento,
    listar_campanhas,
    listar_empreendimentos,
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


@st.cache_data(show_spinner=False, ttl=3600)  # Cache por 1 hora
def carregar_projetos(modo: ModoGeo) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_projetos(conn, modo)


@st.cache_data(show_spinner=False, ttl=3600)  # Cache por 1 hora
def carregar_campanhas(modo: ModoGeo, projetos: list[str]) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_campanhas(conn, modo, projetos, [])


@st.cache_data(show_spinner=False, ttl=3600)  # Cache por 1 hora
def carregar_empreendimentos(modo: ModoGeo, projetos: list[str]) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_empreendimentos(conn, modo, projetos)


@st.cache_data(show_spinner=False, ttl=3600)  # Cache por 1 hora
def carregar_campanhas_filtradas(
    modo: ModoGeo,
    projetos: list[str],
    empreendimentos: list[str],
) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_campanhas(conn, modo, projetos, empreendimentos)


@st.cache_data(show_spinner=False, ttl=3600)  # Cache por 1 hora
def carregar_grupos_biologicos(
    projetos: list[str],
    empreendimentos: list[str],
    campanhas: list[str],
) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_grupos_biologicos(conn, projetos, empreendimentos, campanhas)


@st.cache_data(show_spinner=False, ttl=3600)  # Cache por 1 hora (dados geo não mudam constantemente)
def carregar_dados_geo(
    modo: ModoGeo,
    projetos: list[str],
    empreendimentos: list[str],
    campanhas: list[str],
    grupos_biologicos: list[str] | None = None,
) -> pd.DataFrame:
    engine = get_engine()

    with engine.connect() as conn:
        if modo == "Fisico":
            if empreendimentos:
                df = get_geo_fisico_com_empreendimento(conn, projetos, empreendimentos, campanhas)
            else:
                df = get_geo_fisico_com_empreendimento(conn, projetos, [], campanhas)
        else:
            df = get_geo_biota(conn, projetos, empreendimentos, campanhas, grupos_biologicos)

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

    return calcular_indicadores_fisicos_por_ponto(df)


@st.cache_data(hash_funcs={pd.DataFrame: id}, ttl=3600, show_spinner=False)
def calcular_resumo_geo(df: pd.DataFrame, modo: ModoGeo) -> dict[str, float]:
    if df.empty:
        base = {
            "projetos": 0,
            "campanhas": 0,
            "pontos": 0,
            "iqa_medio": 0.0,
            "parametros_nao_conformes": 0.0,
            "parametros_com_limite": 0.0,
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
                "iqa_medio": float(df["iqa"].mean()) if "iqa" in df.columns and not df.empty else 0.0,
                "parametros_nao_conformes": float(df["parametros_nao_conformes"].sum()) if "parametros_nao_conformes" in df.columns else 0.0,
                "parametros_com_limite": float(df["parametros_com_limite"].sum()) if "parametros_com_limite" in df.columns else 0.0,
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


@st.cache_data(hash_funcs={pd.DataFrame: id}, ttl=3600, show_spinner=False)
def preparar_dados_mapa_media_campanhas(df: pd.DataFrame, modo: ModoGeo) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = ["projeto", "ponto", "latitude", "longitude"]
    if modo == "Biota" and "grupo_biologico" in df.columns:
        group_cols.append("grupo_biologico")

    numeric_cols = [
        c
        for c in [
            "pontos_amostrados",
            "numero_taxons",
            "riqueza",
            "abundancia_total",
            "biomassa_total",
            "iqa",
            "parametros_nao_conformes",
            "parametros_avaliados",
            "parametros_com_limite",
            "shannon",
            "pielou",
            "bmwp_total",
            "riqueza_ept",
            "abundancia_ept",
        ]
        if c in df.columns
    ]

    if not numeric_cols:
        return df

    agg_spec: dict[str, str] = {col: "mean" for col in numeric_cols}
    if "campanha" in df.columns:
        agg_spec["campanha"] = "nunique"

    out = df.groupby(group_cols, dropna=False, as_index=False).agg(agg_spec)

    if "campanha" in out.columns:
        out = out.rename(columns={"campanha": "campanhas_agregadas"})
        out["campanha"] = out["campanhas_agregadas"].apply(lambda n: f"Media de {int(n)} campanha(s)")

    return out
