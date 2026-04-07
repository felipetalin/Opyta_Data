from __future__ import annotations

import streamlit as st
from pathlib import Path

import pandas as pd

from core.engine import get_engine
from core.sidebar import render_sidebar
from app.state import initialize_system_status, mark_stage_completed
from analises.common.base import RunContext
from analises.common.theme import get_theme, ordem_campanhas_padrao
from analises.common.analises import (
    analisar_composicao_taxonomica,
    analisar_riqueza_por_ponto,
    analisar_riqueza_por_filo,
    analisar_diversidade_alfa,
    analisar_bmwp_zoobentos,
)

# ------------------------------------------------
# PAGE CONFIG
# ------------------------------------------------

st.set_page_config(page_title="03 - Análises", layout="wide")

# ------------------------------------------------
# Verificação de login
# ------------------------------------------------

if not st.session_state.get("logged_in"):
    st.switch_page("main.py")

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()

initialize_system_status()

# ------------------------------------------------
# Sidebar
# ------------------------------------------------

render_sidebar()

st.title("03 — Análises")

st.markdown(
    "📌 **Etapa 4: Análises Ecológicas** | Cálculos e métricas\n"
    "\n"
    "**Passos:**\n"
    "1. Selecione Projeto → Grupo → Campanha (opcional)  \n"
    "2. Marque blocos de análise desejados  \n"
    "3. Clique em Executar → 📊 Visualize resultados"
)

# =========================
# QUERIES AUXILIARES
# =========================
@st.cache_data(show_spinner=False)
def listar_projetos() -> list[str]:
    engine = get_engine()
    sql = """
        SELECT DISTINCT nome_projeto
        FROM public.biota_analise_consolidada
        WHERE nome_projeto IS NOT NULL
        ORDER BY 1
    """
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df["nome_projeto"].astype(str).tolist()


@st.cache_data(show_spinner=False)
def listar_campanhas(projeto: str, grupo: str) -> list[str]:
    engine = get_engine()
    sql = """
        SELECT DISTINCT nome_campanha
        FROM public.biota_analise_consolidada
        WHERE nome_projeto = %(projeto)s
          AND grupo_biologico = %(grupo)s
          AND nome_campanha IS NOT NULL
        ORDER BY 1
    """
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"projeto": projeto, "grupo": grupo})
    return df["nome_campanha"].astype(str).tolist()


@st.cache_data(show_spinner=False)
def carregar_df_base(projeto: str, grupo: str) -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT *
        FROM public.biota_analise_consolidada
        WHERE nome_projeto = %(projeto)s
          AND grupo_biologico = %(grupo)s
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"projeto": projeto, "grupo": grupo})
    return df


# =========================
# CONFIGURAÇÕES DE ANÁLISE
# =========================

st.markdown(
    "### Fluxo de Análises"
    "\n\n1. Selecione seu projeto e grupo."
    "\n2. Ajuste opções e blocos de análise.&nbsp;"
    "\n3. Clique em Executar e veja os resultados no painel abaixo."
)

config_tab, results_tab = st.tabs(["Configuração", "Resultados"])

with config_tab:
    st.subheader("Configuração da Análise")
    with st.container():
        left, right = st.columns([2, 1], gap="large")

        with left:
            st.markdown("**Dados**")
            busca_proj = st.text_input("Buscar projeto", value="").strip().lower()
            projetos = listar_projetos()
            if busca_proj:
                projetos = [p for p in projetos if busca_proj in p.lower()]

            projeto = st.selectbox("Projeto", options=[""] + projetos, index=0)
            grupo = st.selectbox(
                "Grupo",
                options=["Ictiofauna", "Zoobentos", "Fitoplancton", "Zooplancton", "Meio Fisico"],
                index=0,
            )

            campanhas = []
            campanha = "(todas)"
            if projeto:
                try:
                    campanhas = listar_campanhas(projeto, grupo)
                except Exception:
                    campanhas = []

                campanha = st.selectbox("Campanha", options=["(todas)"] + campanhas, index=0)
            else:
                st.selectbox("Campanha", options=["(preencha projeto)"], index=0, disabled=True)

            st.markdown("---")
            st.markdown("**Opções**")
            tema = st.selectbox("Tema de visualização", options=["cliente_azul", "cliente_verde", "neutro"], index=0)
            pasta_saida = st.text_input("Pasta de saída", value="exports")
            exportar_arquivos = st.checkbox("Exportar arquivos (xlsx/png)", value=False)

            st.markdown("---")
            st.markdown("**Blocos de análise**")
            exec_composicao = st.checkbox("Composição Taxonômica", value=True)
            exec_riqueza_ponto = st.checkbox("Riqueza por Ponto", value=True)
            exec_riqueza_filo = st.checkbox("Riqueza por Filo", value=True)
            exec_diversidade = st.checkbox("Diversidade Alfa", value=True)

            if grupo.lower() == "zoobentos":
                with st.expander("Análises específicas", expanded=False):
                    exec_bmwp = st.checkbox("BMWP (Zoobentos)", value=True)
            else:
                exec_bmwp = False

            st.markdown("---")
            st.caption("Marque apenas os blocos que você precisa. As análises padrão geram um bom conjunto inicial.")

            executar = st.button("🚀 Executar análises", use_container_width=True)

        with right:
            st.markdown("**Status do Projeto**")
            if projeto:
                with st.spinner("Carregando prévia do projeto..."):
                    df_base = carregar_df_base(projeto, grupo)

                if df_base.empty:
                    st.warning("Nenhum dado encontrado para esse Projeto + Grupo.")
                else:
                    total_registros = len(df_base)
                    total_campanhas = df_base["nome_campanha"].nunique() if "nome_campanha" in df_base.columns else 0
                    total_pontos = df_base["nome_ponto"].nunique() if "nome_ponto" in df_base.columns else 0

                    st.metric("Registros", f"{total_registros:,}")
                    st.metric("Campanhas", total_campanhas)
                    st.metric("Pontos", total_pontos)
                    st.metric("Grupo", grupo)

                    with st.expander("Ver dados de entrada", expanded=False):
                        st.dataframe(df_base.head(20), use_container_width=True)
            else:
                st.info("Escolha um projeto para ver o resumo e carregar os dados.")

with results_tab:
    st.subheader("Resultados")
    st.markdown("Os resultados serão exibidos aqui após a execução." )
    if not st.session_state.get("results_executed", False):
        st.info("Vá para a aba Configuração e clique em Executar para gerar as análises.")

# =========================
# EXECUÇÃO MODULAR
# =========================
if projeto and not df_base.empty and executar:
    try:
        with st.spinner("Executando análises..."):
            df_trabalho = df_base.copy()
            if campanha and campanha != "(todas)" and "nome_campanha" in df_trabalho.columns:
                df_trabalho = df_trabalho[
                    df_trabalho["nome_campanha"].astype(str).str.strip() == str(campanha).strip()
                ].copy()

            ordem = ordem_campanhas_padrao(grupo)
            if ordem is None and "nome_campanha" in df_trabalho.columns:
                ordem = sorted(df_trabalho["nome_campanha"].dropna().astype(str).unique().tolist())

            ctx = RunContext(
                projeto=projeto,
                grupo=grupo,
                campanha=campanha,
                ponto=None,
                pasta_saida=Path(pasta_saida),
                tema=tema,
                exportar_arquivos=exportar_arquivos,
                ordem_campanhas=ordem,
            )

            results = []
            if exec_composicao:
                results.append(analisar_composicao_taxonomica(df_trabalho, ctx))
            if exec_riqueza_ponto:
                results.append(analisar_riqueza_por_ponto(df_trabalho, ctx))
            if exec_riqueza_filo:
                results.append(analisar_riqueza_por_filo(df_trabalho, ctx))
            if exec_diversidade:
                results.append(analisar_diversidade_alfa(df_trabalho, ctx))
            if exec_bmwp and grupo.lower() == "zoobentos":
                results.append(analisar_bmwp_zoobentos(df_trabalho, ctx))

        st.session_state.results_executed = True
        st.session_state.analysis_results = results
        if results:
            mark_stage_completed("analises_status")
            st.success("Análises concluídas com sucesso!")
        else:
            st.warning("Nenhum bloco de análise foi selecionado para execução.")
    except Exception as exc:
        st.session_state.results_executed = False
        st.session_state.analysis_results = []
        st.error(f"Erro ao executar análises: {exc}")

if st.session_state.get("results_executed", False):
    with results_tab:
        if not st.session_state.get("analysis_results"):
            st.warning("Nenhum bloco de análise foi executado. Marque ao menos uma opção na aba Configuração.")
        else:
            _ = get_theme(tema)
            for idx, res in enumerate(st.session_state.analysis_results, start=1):
                with st.expander(f"{idx:02d} • {res['title']}", expanded=True):
                    if not res["ok"]:
                        st.error(res["error"] or "Erro desconhecido")
                        continue
                    if res["df"] is not None and not res["df"].empty:
                        st.dataframe(res["df"], use_container_width=True)
                    if res["fig"] is not None:
                        st.plotly_chart(res["fig"], use_container_width=True)
                    if res.get("files"):
                        st.caption("Arquivos exportados:")
                        for f in res["files"]:
                            st.write(f"- {Path(f).name}")

