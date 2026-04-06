from __future__ import annotations

import streamlit as st
from pathlib import Path

import pandas as pd

from core.engine import get_engine
from core.sidebar import render_sidebar
from analises.common.base import RunContext
from analises.common.theme import get_theme, ordem_campanhas_padrao
from analises.common.analises import (
    analisar_composicao_taxonomica,
    analisar_riqueza_por_ponto,
    analisar_riqueza_por_filo,
    analisar_diversidade_alfa,
    analisar_bmwp_zoobentos
)

# ------------------------------------------------
# Verificação de login
# ------------------------------------------------

if not st.session_state.get("logged_in"):
    st.switch_page("main.py")

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()

# ------------------------------------------------
# Sidebar
# ------------------------------------------------

render_sidebar()

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(page_title="03 - Análises", layout="wide")
st.title("03 - Análises")
st.info("Selecione filtros e execute análises ecológicas. Use os controles para ativar/desativar blocos específicos.")

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
# SIDEBAR (FILTROS)
# =========================
st.sidebar.header("Filtros")

busca_proj = st.sidebar.text_input("Buscar projeto", value="").strip().lower()
projetos = listar_projetos()
if busca_proj:
    projetos = [p for p in projetos if busca_proj in p.lower()]

projeto = st.sidebar.selectbox(
    "Projeto (nome_projeto)",
    options=[""] + projetos,
    index=0
)

grupo = st.sidebar.selectbox(
    "Grupo (grupo_biologico)",
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

    campanha = st.sidebar.selectbox(
        "Campanha (nome_campanha)",
        options=["(todas)"] + campanhas,
        index=0
    )
else:
    st.sidebar.selectbox(
        "Campanha (nome_campanha)",
        options=["(preencha projeto)"],
        index=0,
        disabled=True
    )

tema = st.sidebar.selectbox(
    "Tema",
    options=["cliente_azul", "cliente_verde", "neutro"],
    index=0
)

pasta_saida = st.sidebar.text_input(
    "Pasta de saída (exports)",
    value="exports"
)

exportar_arquivos = st.sidebar.toggle(
    "Exportar arquivos (xlsx/png)",
    value=False
)

# =========================
# CONTROLES DE ANÁLISE (MODULAR)
# =========================
st.sidebar.header("Blocos de Análise")

# Análises gerais (todos os grupos)
exec_composicao = st.sidebar.toggle("📋 Composição Taxonômica", value=True)
exec_riqueza_ponto = st.sidebar.toggle("📊 Riqueza por Ponto", value=True)
exec_riqueza_filo = st.sidebar.toggle("🌀 Riqueza por Filo", value=True)
exec_diversidade = st.sidebar.toggle("🧬 Diversidade Alfa", value=True)

# Análises específicas por grupo
exec_bmwp = False
if grupo.lower() == "zoobentos":
    exec_bmwp = st.sidebar.toggle("🐛 BMWP (Zoobentos)", value=True)

st.sidebar.markdown("---")
st.sidebar.caption("Ative apenas os blocos desejados para execução mais rápida.")

# =========================
# CARREGAR DADOS
# =========================
df_base = pd.DataFrame()

if projeto:
    with st.spinner("Carregando dados do banco..."):
        df_base = carregar_df_base(projeto, grupo)

    if df_base.empty:
        st.warning("Nenhum dado encontrado para esse Projeto + Grupo.")
    else:
        st.success(f"{len(df_base):,} registros carregados.")
        with st.expander("Prévia (dados brutos)", expanded=False):
            st.dataframe(df_base.head(50), use_container_width=True)

# =========================
# EXECUÇÃO MODULAR
# =========================
if projeto and not df_base.empty:
    df_trabalho = df_base.copy()

    # Filtro de campanha
    if campanha and campanha != "(todas)" and "nome_campanha" in df_trabalho.columns:
        df_trabalho = df_trabalho[
            df_trabalho["nome_campanha"].astype(str).str.strip() == str(campanha).strip()
        ].copy()

    # Ordem padrão de campanhas
    ordem = ordem_campanhas_padrao(grupo)
    if ordem is None and "nome_campanha" in df_trabalho.columns:
        ordem = sorted(
            df_trabalho["nome_campanha"].dropna().astype(str).unique().tolist()
        )

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

    # =========================
    # BOTÃO DE EXECUÇÃO
    # =========================
    col_exec, col_info = st.columns([1, 2])
    with col_exec:
        executar = st.button("🚀 Executar Análises", use_container_width=True)
    with col_info:
        st.caption("Executa apenas os blocos ativados na sidebar.")

    if executar:
        st.markdown("---")

        results = []

        # =========================
        # ANÁLISES GERAIS
        # =========================

        if exec_composicao:
            with st.spinner("Executando: Composição Taxonômica..."):
                res = analisar_composicao_taxonomica(df_trabalho, ctx)
                results.append(res)

        if exec_riqueza_ponto:
            with st.spinner("Executando: Riqueza por Ponto..."):
                res = analisar_riqueza_por_ponto(df_trabalho, ctx)
                results.append(res)

        if exec_riqueza_filo:
            with st.spinner("Executando: Riqueza por Filo..."):
                res = analisar_riqueza_por_filo(df_trabalho, ctx)
                results.append(res)

        if exec_diversidade:
            with st.spinner("Executando: Diversidade Alfa..."):
                res = analisar_diversidade_alfa(df_trabalho, ctx)
                results.append(res)

        # =========================
        # ANÁLISES ESPECÍFICAS
        # =========================

        if exec_bmwp and grupo.lower() == "zoobentos":
            with st.spinner("Executando: BMWP (Zoobentos)..."):
                res = analisar_bmwp_zoobentos(df_trabalho, ctx)
                results.append(res)

        # =========================
        # RENDERIZAÇÃO DOS RESULTADOS
        # =========================

        if results:
            _ = get_theme(tema)  # mantém compatibilidade

            for i, res in enumerate(results, start=1):
                st.subheader(f"{i:02d}) {res['title']}")

                if not res["ok"]:
                    st.error(res["error"] or "Erro desconhecido")
                    continue

                if res["df"] is not None and not res["df"].empty:
                    st.dataframe(res["df"], use_container_width=True)

                if res["fig"] is not None:
                    st.plotly_chart(res["fig"], use_container_width=True)

                if res.get("files"):
                    st.caption("📁 Arquivos exportados:")
                    for f in res["files"]:
                        st.write(f"- {Path(f).name}")

        else:
            st.info("Nenhum bloco de análise foi executado. Ative os toggles na sidebar.")

else:
    st.info("Selecione um Projeto e Grupo para começar as análises.")