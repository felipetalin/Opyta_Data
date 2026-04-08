from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.sidebar import render_sidebar
from core.geoprocessamento.components import (
    render_biological_group_filter,
    render_campaign_filter,
    render_cards,
    render_context_bar,
    render_data_quality_warning,
    render_empty_state,
    render_header,
    render_indicator_selector,
    render_mode_selector,
    render_project_filter,
    render_ranking,
    reset_geo_filters,
    render_table,
)
from core.geoprocessamento.map_view import render_mapa_geo
from core.geoprocessamento.services import (
    calcular_resumo_geo,
    carregar_campanhas,
    carregar_dados_geo,
    carregar_grupos_biologicos,
    carregar_projetos,
    preparar_dados_mapa_media_campanhas,
)


st.set_page_config(page_title="Geoambiental | Indicadores ecológicos e do meio físico", layout="wide")

if not st.session_state.get("logged_in"):
    st.switch_page("main.py")

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()

render_sidebar()

render_header()

modo = render_mode_selector()

with st.spinner("Carregando filtros..."):
    projetos = carregar_projetos(modo)

col1, col2, col3 = st.columns(3)
with col1:
    projetos_sel = render_project_filter(projetos)

with col2:
    campanhas = carregar_campanhas(modo, projetos_sel) if projetos_sel else []
    campanhas_sel = render_campaign_filter(campanhas, disabled=not bool(projetos_sel))

with col3:
    if modo == "Biota":
        grupos = carregar_grupos_biologicos(projetos_sel, campanhas_sel)
        grupos_sel = render_biological_group_filter(grupos, disabled=False)
    else:
        grupos_sel = render_biological_group_filter([], disabled=True)

with st.spinner("Carregando dados geográficos..."):
    df_geo = carregar_dados_geo(modo, projetos_sel, campanhas_sel, grupos_sel)
    df_geo_mapa = preparar_dados_mapa_media_campanhas(df_geo, modo)

indicador = render_indicator_selector(modo, df_geo)
render_data_quality_warning(df_geo, modo, indicador)

resumo = calcular_resumo_geo(df_geo, modo)

top_left, top_right = st.columns([5, 1])
with top_left:
    render_context_bar(
        modo=modo,
        projetos=projetos_sel,
        campanhas=campanhas_sel,
        grupos_biologicos=grupos_sel,
        indicador=indicador,
        total_pontos=int(df_geo["ponto"].nunique()) if "ponto" in df_geo.columns and not df_geo.empty else 0,
    )
with top_right:
    if st.button("Limpar filtros", use_container_width=True):
        reset_geo_filters()
        st.rerun()

render_cards(resumo, modo)
if df_geo.empty:
    render_empty_state()
else:
    map_col, rank_col = st.columns([3, 1], gap="large")
    with map_col:
        render_mapa_geo(df_geo_mapa, modo, indicador)
    with rank_col:
        render_ranking(df_geo, indicador)
render_table(df_geo, indicador=indicador)
