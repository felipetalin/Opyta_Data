from __future__ import annotations

import pandas as pd
import streamlit as st

from core.engine import get_engine
from .queries import ModoGeo, get_geo_biota, get_geo_fisico, listar_campanhas, listar_projetos


@st.cache_data(show_spinner=False)
def carregar_projetos(modo: ModoGeo) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_projetos(conn, modo)


@st.cache_data(show_spinner=False)
def carregar_campanhas(modo: ModoGeo, projeto: str) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        return listar_campanhas(conn, modo, projeto)


@st.cache_data(show_spinner=False)
def carregar_dados_geo(modo: ModoGeo, projeto: str, campanha: str) -> pd.DataFrame:
    engine = get_engine()

    with engine.connect() as conn:
        if modo == "Fisico":
            df = get_geo_fisico(conn, projeto, campanha)
        else:
            df = get_geo_biota(conn, projeto, campanha)

    if df.empty:
        return df

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    df = df[df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)].copy()

    return df


def calcular_resumo_geo(df: pd.DataFrame) -> dict[str, int]:
    if df.empty:
        return {
            "registros": 0,
            "projetos": 0,
            "campanhas": 0,
            "pontos": 0,
        }

    return {
        "registros": int(len(df)),
        "projetos": int(df["projeto"].nunique()) if "projeto" in df.columns else 0,
        "campanhas": int(df["campanha"].nunique()) if "campanha" in df.columns else 0,
        "pontos": int(df["ponto"].nunique()) if "ponto" in df.columns else 0,
    }
