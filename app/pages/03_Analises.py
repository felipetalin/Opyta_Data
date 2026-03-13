from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

from analises.common.base import RunContext
from analises.common.theme import get_theme, ordem_campanhas_padrao
from analises.common.ictio.runner import run as run_ictio


# =========================
# BLOQUEIO DE ACESSO
# =========================
if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()


# =========================
# PAGE CONFIG
# =========================
st.set_page_config(page_title="03 - Análises", layout="wide")
st.title("03 - Análises")
st.info("Preencha o Projeto para carregar os dados.")


# =========================
# DB / ENGINE
# =========================
@st.cache_resource(show_spinner=False)
def _get_engine():
    """
    Ordem de prioridade:
    1) DATABASE_URL em st.secrets
    2) DATABASE_URL em variável de ambiente
    3) DB_USER / DB_PASSWORD / DB_HOST / DB_NAME / DB_PORT
    """
    from dotenv import load_dotenv

    load_dotenv()

    # 1) tenta Streamlit Secrets
    database_url = None
    try:
        database_url = st.secrets.get("DATABASE_URL", None)
    except Exception:
        database_url = None

    # 2) tenta variável de ambiente
    if not database_url:
        database_url = os.getenv("DATABASE_URL")

    if database_url:
        return create_engine(database_url, pool_pre_ping=True)

    # 3) fallback para variáveis separadas
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_port = os.getenv("DB_PORT", "5432")

    faltando = [k for k, v in {
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "DB_HOST": db_host,
        "DB_NAME": db_name,
    }.items() if not v]

    if faltando:
        raise RuntimeError(
            "Variáveis de conexão ausentes. Configure DATABASE_URL "
            "ou DB_USER/DB_PASSWORD/DB_HOST/DB_NAME. "
            f"Faltando: {', '.join(faltando)}"
        )

    url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(url, pool_pre_ping=True)


# =========================
# QUERIES AUXILIARES
# =========================
@st.cache_data(show_spinner=False)
def listar_projetos() -> list[str]:
    engine = _get_engine()
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
    engine = _get_engine()
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
def listar_pontos(projeto: str, grupo: str) -> list[str]:
    engine = _get_engine()
    sql = """
        SELECT DISTINCT nome_ponto
        FROM public.biota_analise_consolidada
        WHERE nome_projeto = %(projeto)s
          AND grupo_biologico = %(grupo)s
          AND nome_ponto IS NOT NULL
        ORDER BY 1
    """
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"projeto": projeto, "grupo": grupo})
    return df["nome_ponto"].astype(str).tolist()


@st.cache_data(show_spinner=False)
def carregar_df_base(projeto: str, grupo: str) -> pd.DataFrame:
    engine = _get_engine()
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

campanhas: list[str] = []
campanha = "(todas)"
pontos: list[str] = []
nome_ponto = "(todos)"

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

    try:
        pontos = listar_pontos(projeto, grupo)
    except Exception:
        pontos = []

    nome_ponto = st.sidebar.selectbox(
        "Ponto (nome_ponto)",
        options=["(todos)"] + pontos,
        index=0
    )
else:
    st.sidebar.selectbox(
        "Campanha (nome_campanha)",
        options=["(preencha projeto)"],
        index=0,
        disabled=True
    )
    st.sidebar.selectbox(
        "Ponto (nome_ponto)",
        options=["(preencha projeto)"],
        index=0,
        disabled=True
    )


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
# EXECUÇÃO
# =========================
st.subheader("Execução")
col_a, col_b = st.columns([1, 2], vertical_alignment="center")

rodar = col_a.button(
    "Rodar análises do grupo",
    disabled=(not bool(projeto) or df_base.empty)
)

col_b.caption("Executa o pacote do grupo. Se um bloco falhar, os demais continuam.")

if rodar:
    df_trabalho = df_base.copy()

    # filtro campanha
    if campanha != "(todas)" and "nome_campanha" in df_trabalho.columns:
        df_trabalho = df_trabalho[
            df_trabalho["nome_campanha"].astype(str).str.strip() == str(campanha).strip()
        ].copy()

    # filtro ponto
    if nome_ponto != "(todos)" and "nome_ponto" in df_trabalho.columns:
        df_trabalho = df_trabalho[
            df_trabalho["nome_ponto"].astype(str).str.strip() == str(nome_ponto).strip()
        ].copy()

    ordem = ordem_campanhas_padrao(grupo)
    if ordem is None and "nome_campanha" in df_trabalho.columns:
        ordem = sorted(df_trabalho["nome_campanha"].dropna().astype(str).unique().tolist())

    ctx = RunContext(
        projeto=projeto,
        grupo=grupo,
        campanha=campanha,
        nome_ponto=None if nome_ponto == "(todos)" else nome_ponto,
        pasta_saida=Path(pasta_saida),
        tema=tema,
        exportar_arquivos=exportar_arquivos,
        ordem_campanhas=ordem,
    )

    st.markdown("---")

    # Roteamento por grupo
    if grupo.lower() in ("ictiofauna", "ictio"):
        results = run_ictio(ctx, df_trabalho)
    else:
        st.warning("Runner desse grupo ainda não foi conectado. (Por enquanto: Ictiofauna)")
        results = []

    if results:
        for i, res in enumerate(results, start=1):
            st.subheader(f"{i:02d}) {res.title}")

            if not res.ok:
                st.error(res.error or "Erro desconhecido")
                continue

            if res.warnings:
                for w in res.warnings:
                    st.warning(w)

            if res.df is not None and not res.df.empty:
                st.dataframe(res.df, use_container_width=True)

            if res.fig is not None:
                st.plotly_chart(res.fig, use_container_width=True)

            if res.files:
                st.caption("Exports:")
                for f in res.files:
                    st.write(str(f))
    else:
        st.info("Nenhum resultado retornado pelo runner.")