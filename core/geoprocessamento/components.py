from __future__ import annotations

import pandas as pd
import streamlit as st

from .queries import ModoGeo


VALID_MODES: list[ModoGeo] = ["Fisico", "Biota"]


def render_header() -> None:
    st.title("Geoambiental | Biodiversidade e Qualidade da Água")
    st.markdown(
        "📌 **Geoprocessamento:** Visualize qualidade da água e biodiversidade no mapa interativo.\n"
        "\n"
        "**Passos:**\n"
        "1. Escolha o modo (Físico ou Biota)  \n"
        "2. Aplique filtros de projeto e campanha  \n"
        "3. Explore pontos no mapa e detalhes na tabela"
    )


def render_mode_selector() -> ModoGeo:
    modo = st.radio(
        "Modo de visualização",
        options=VALID_MODES,
        index=0,
        horizontal=True,
    )
    return modo


def render_empty_state() -> None:
    st.info("Nenhum dado encontrado para os filtros selecionados. Ajuste projeto/campanha e tente novamente.")


def render_project_filter(projetos: list[str]) -> str:
    return st.selectbox("Projeto", options=[""] + projetos, index=0, key="geo_projeto")


def render_campaign_filter(campanhas: list[str], disabled: bool = False) -> str:
    if disabled:
        st.selectbox(
            "Campanha",
            options=["(selecione um projeto)"],
            index=0,
            disabled=True,
        )
        return ""

    return st.selectbox("Campanha", options=[""] + campanhas, index=0, key="geo_campanha")


def render_cards(resumo: dict[str, int]) -> None:
    st.subheader("Resumo")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Registros", resumo.get("registros", 0))
    c2.metric("Projetos", resumo.get("projetos", 0))
    c3.metric("Campanhas", resumo.get("campanhas", 0))
    c4.metric("Pontos", resumo.get("pontos", 0))


def render_table(df: pd.DataFrame) -> None:
    st.subheader("Tabela")
    if df.empty:
        render_empty_state()
        return

    st.dataframe(df, use_container_width=True, height=420)
