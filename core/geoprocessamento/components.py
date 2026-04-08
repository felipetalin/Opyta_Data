from __future__ import annotations

import pandas as pd
import streamlit as st

from .queries import ModoGeo


VALID_MODES: list[ModoGeo] = ["Fisico", "Biota"]


def render_header() -> None:
    st.title("Geoambiental | Indicadores ecológicos e do meio físico")
    st.markdown(
        "📌 **Geoprocessamento:** Visualize indicadores ecológicos e do meio físico no mapa interativo.\n"
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


def render_project_filter(projetos: list[str]) -> list[str]:
    return st.multiselect("Projeto", options=projetos, key="geo_projetos")


def render_campaign_filter(campanhas: list[str], disabled: bool = False) -> list[str]:
    if disabled:
        st.multiselect(
            "Campanha",
            options=[],
            disabled=True,
        )
        return []

    return st.multiselect("Campanha", options=campanhas, key="geo_campanhas")


def render_biological_group_filter(grupos: list[str], disabled: bool = False) -> list[str]:
    if disabled:
        st.multiselect("Grupo biológico", options=[], disabled=True)
        return []
    return st.multiselect("Grupo biológico", options=grupos, key="geo_grupos_biologicos")


def render_indicator_selector(modo: ModoGeo, df: pd.DataFrame) -> str:
    if modo != "Biota":
        options = [
            "iqa",
            "parametros_nao_conformes",
        ]
        return st.selectbox(
            "Indicador do mapa",
            options=options,
            index=0,
            key="geo_indicador_fisico",
        )

    options = [
        "riqueza",
        "abundancia_total",
        "biomassa_total",
        "shannon",
        "pielou",
        "numero_taxons",
    ]
    if "bmwp_total" in df.columns:
        options.extend(["bmwp_total", "riqueza_ept", "abundancia_ept"])

    return st.selectbox("Indicador do mapa", options=options, index=0, key="geo_indicador")


def render_context_bar(
    modo: ModoGeo,
    projetos: list[str],
    campanhas: list[str],
    grupos_biologicos: list[str],
    indicador: str,
    total_pontos: int,
) -> None:
    st.caption(
        " | ".join(
            [
                f"Modo: {modo}",
                f"Projetos: {len(projetos) if projetos else 'Todos'}",
                f"Campanhas: {len(campanhas) if campanhas else 'Todas'}",
                f"Grupos: {len(grupos_biologicos) if grupos_biologicos else 'Todos'}",
                f"Indicador: {indicador}",
                f"Pontos: {total_pontos}",
            ]
        )
    )


def render_data_quality_warning(df: pd.DataFrame, modo: ModoGeo, indicador: str) -> None:
    if df.empty:
        return

    if modo == "Fisico":
        st.info("Neste estágio, o modo Físico considera indicadores de Água Superficial.")
        total_pontos = int(df["ponto"].nunique()) if "ponto" in df.columns else 0
        if indicador == "iqa" and "parametros_com_limite" in df.columns and total_pontos > 0:
            pontos_com_limite = int((pd.to_numeric(df["parametros_com_limite"], errors="coerce").fillna(0) > 0).sum())
            if pontos_com_limite == 0:
                st.warning("IQA está zerado porque não há parâmetros com limite regulatório disponível nos dados filtrados.")
            elif pontos_com_limite < total_pontos:
                st.info(
                    f"IQA parcial: {pontos_com_limite}/{total_pontos} ponto(s) possuem parâmetros com limite para cálculo."
                )
        return

    if modo != "Biota":
        return

    total_pontos = int(df["ponto"].nunique()) if "ponto" in df.columns else 0
    if total_pontos <= 0:
        return

    if indicador == "bmwp_total" and "bmwp_registros_com_score" in df.columns:
        pontos_com_bmwp = int((pd.to_numeric(df["bmwp_registros_com_score"], errors="coerce").fillna(0) > 0).sum())
        if pontos_com_bmwp == 0:
            st.warning(
                "BMWP está zerado porque não há `bmwp_score` preenchido nos dados filtrados."
            )
        elif pontos_com_bmwp < total_pontos:
            st.info(
                f"BMWP parcial: {pontos_com_bmwp}/{total_pontos} ponto(s) possuem `bmwp_score` preenchido."
            )

    if indicador in {"riqueza_ept", "abundancia_ept"} and "ept_registros_com_ordem" in df.columns:
        pontos_com_ordem = int((pd.to_numeric(df["ept_registros_com_ordem"], errors="coerce").fillna(0) > 0).sum())
        if pontos_com_ordem == 0:
            st.warning(
                "EPT está zerado porque não há `ordem` taxonômica preenchida nos dados filtrados."
            )
        elif pontos_com_ordem < total_pontos:
            st.info(
                f"EPT parcial: {pontos_com_ordem}/{total_pontos} ponto(s) possuem `ordem` preenchida."
            )


def reset_geo_filters() -> None:
    for key in [
        "geo_projetos",
        "geo_campanhas",
        "geo_grupos_biologicos",
        "geo_indicador",
        "geo_indicador_fisico",
    ]:
        if key in st.session_state:
            st.session_state.pop(key)


def render_cards(resumo: dict[str, float], modo: ModoGeo) -> None:
    st.subheader("Indicadores ecológicos" if modo == "Biota" else "Indicadores do meio físico")
    c1, c2, c3, c4 = st.columns(4)
    if modo == "Biota":
        c1.metric("Riqueza média", f"{resumo.get('riqueza_media', 0.0):.2f}")
        c2.metric("Abundância total", f"{resumo.get('abundancia_total', 0.0):,.0f}")
        c3.metric("Biomassa total", f"{resumo.get('biomassa_total', 0.0):,.2f}")
        c4.metric("Shannon médio", f"{resumo.get('shannon_medio', 0.0):.2f}")
    else:
        c1.metric("IQA médio", f"{resumo.get('iqa_medio', 0.0):.1f}")
        c2.metric("Parâmetros não conformes", f"{resumo.get('parametros_nao_conformes', 0.0):,.0f}")
        c3.metric("Parâmetros com limite", f"{resumo.get('parametros_com_limite', 0.0):,.0f}")
        c4.metric("Pontos avaliados", f"{resumo.get('pontos', 0.0):,.0f}")

    c5, c6, c7, c8 = st.columns(4)
    if modo == "Biota":
        c5.metric("Pielou médio", f"{resumo.get('pielou_medio', 0.0):.2f}")
        c6.metric("Projetos", f"{resumo.get('projetos', 0.0):,.0f}")
        c7.metric("Campanhas", f"{resumo.get('campanhas', 0.0):,.0f}")
        c8.metric("Pontos", f"{resumo.get('pontos', 0.0):,.0f}")
    else:
        c5.metric("Projetos", f"{resumo.get('projetos', 0.0):,.0f}")
        c6.metric("Campanhas", f"{resumo.get('campanhas', 0.0):,.0f}")
        c7.metric("Matriz", "Água Superficial")
        c8.metric("Regra atual", "IQA + NC")

    if modo == "Biota" and resumo.get("bmwp_total", 0.0) > 0:
        c9, c10, c11 = st.columns(3)
        c9.metric("BMWP total", f"{resumo.get('bmwp_total', 0.0):,.0f}")
        c10.metric("Riqueza EPT", f"{resumo.get('riqueza_ept_total', 0.0):,.0f}")
        c11.metric("Abundância EPT", f"{resumo.get('abundancia_ept_total', 0.0):,.0f}")


def render_ranking(df: pd.DataFrame, indicador: str, top_n: int = 10) -> None:
    st.subheader("Ranking de pontos")
    if df.empty or indicador not in df.columns:
        st.info("Sem dados suficientes para ranking.")
        return

    ranking = df[["ponto", "campanha", indicador]].copy()
    ranking[indicador] = pd.to_numeric(ranking[indicador], errors="coerce").fillna(0)
    ranking = ranking.sort_values(indicador, ascending=False).head(top_n)
    st.dataframe(ranking, use_container_width=True, height=360)


def render_table(df: pd.DataFrame, indicador: str | None = None) -> None:
    st.subheader("Tabela")
    if df.empty:
        render_empty_state()
        return

    table_df = df.copy()
    if indicador and indicador in table_df.columns:
        table_df[indicador] = pd.to_numeric(table_df[indicador], errors="coerce")
        table_df = table_df.sort_values(indicador, ascending=False)

    st.dataframe(table_df, use_container_width=True, height=420)
