from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.sidebar import render_sidebar
from core.geoprocessamento.components import (
    render_campaign_filter,
    render_cards,
    render_empty_state,
    render_header,
    render_mode_selector,
    render_project_filter,
    render_table,
)
from core.geoprocessamento.map_view import render_mapa_geo
from core.geoprocessamento.services import (
    calcular_resumo_geo,
    carregar_campanhas,
    carregar_dados_geo,
    carregar_projetos,
)


st.set_page_config(page_title="Geoambiental | Biodiversidade e Qualidade da Água", layout="wide")

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

col1, col2 = st.columns(2)
with col1:
    projeto = render_project_filter(projetos)

with col2:
    campanhas = carregar_campanhas(modo, projeto) if projeto else []
    campanha = render_campaign_filter(campanhas, disabled=not bool(projeto))

with st.spinner("Carregando dados geográficos..."):
    df_geo = carregar_dados_geo(modo, projeto, campanha)

resumo = calcular_resumo_geo(df_geo)

render_cards(resumo)
if df_geo.empty:
    render_empty_state()
else:
    render_mapa_geo(df_geo, modo)
render_table(df_geo)
