from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.sidebar import render_sidebar
from core.ui.design_system import (
    render_section_header,
    render_empty_state,
    render_info_box,
)
from core.geoprocessamento.components import (
    render_actions,
    render_biological_group_filter,
    render_campaign_filter,
    render_campaign_comparison,
    render_cards,
    render_context_bar,
    render_data_quality_warning,
    render_empty_state,
    render_enterprise_filter,
    render_header,
    render_indicator_selector,
    render_insights,
    render_mode_selector,
    render_project_filter,
    render_ranking,
    render_filter_bar_title,
    reset_geo_filters,
    render_table,
)
from core.geoprocessamento.map_view import render_mapa_geo
from core.geoprocessamento.services import (
    calcular_resumo_geo,
    carregar_campanhas_filtradas,
    carregar_campanhas,
    carregar_dados_geo,
    carregar_empreendimentos,
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
    empreendimentos = carregar_empreendimentos(modo, [])

render_filter_bar_title()

col1, col2, col3, col4, col5 = st.columns([1.2, 1.2, 1.2, 1.2, 0.7])
with col1:
    projetos_sel = render_project_filter(projetos)

with col2:
    empreendimentos = carregar_empreendimentos(modo, projetos_sel)
    empreendimentos_sel = render_enterprise_filter(empreendimentos, disabled=False)

with col3:
    if projetos_sel or empreendimentos_sel:
        campanhas = carregar_campanhas_filtradas(modo, projetos_sel, empreendimentos_sel)
    else:
        campanhas = carregar_campanhas(modo, [])
    campanhas_sel = render_campaign_filter(campanhas, disabled=False)

with col4:
    if modo == "Biota":
        grupos = carregar_grupos_biologicos(projetos_sel, empreendimentos_sel, campanhas_sel)
        grupos_sel = render_biological_group_filter(grupos, disabled=False)
    else:
        grupos_sel = render_biological_group_filter([], disabled=True)

with col5:
    st.write("")
    st.write("")
    if st.button("Limpar", use_container_width=True):
        reset_geo_filters()
        st.rerun()

with st.spinner("Carregando dados geográficos..."):
    df_geo = carregar_dados_geo(modo, projetos_sel, empreendimentos_sel, campanhas_sel, grupos_sel)
    df_geo_mapa = preparar_dados_mapa_media_campanhas(df_geo, modo)

indicador = render_indicator_selector(modo, df_geo)
render_data_quality_warning(df_geo, modo, indicador)

resumo = calcular_resumo_geo(df_geo, modo)

top_left, top_right = st.columns([5, 1])
with top_left:
    render_context_bar(
        modo=modo,
        projetos=projetos_sel,
        empreendimentos=empreendimentos_sel,
        campanhas=campanhas_sel,
        grupos_biologicos=grupos_sel,
        indicador=indicador,
        total_pontos=int(df_geo["ponto"].nunique()) if "ponto" in df_geo.columns and not df_geo.empty else 0,
    )
with top_right:
    st.metric("Registros", f"{len(df_geo):,}")

render_cards(resumo, modo)
if df_geo.empty:
    render_empty_state()
else:
    render_insights(df_geo, modo)

    map_col, rank_col = st.columns([2.7, 1.3], gap="large")
    with map_col:
        render_mapa_geo(df_geo_mapa, modo, indicador)
    with rank_col:
        render_ranking(df_geo, modo)

    action = render_actions(df_geo, modo)
    if action == "geo_compare":
        render_campaign_comparison(df_geo, modo)
    elif action == "geo_reset":
        reset_geo_filters()
        st.rerun()

render_table(df_geo, indicador=indicador, modo=modo)
