# app/pages/03_Analises.py
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

from analises.common.base import RunContext
from analises.common.theme import get_theme, ordem_campanhas_padrao

# runner do grupo (por enquanto você tem ictio)
from analises.common.ictio.runner import run as run_ictio


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
    from dotenv import load_dotenv
    load_dotenv()

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
        raise RuntimeError(f"Variáveis ausentes no .env: {', '.join(faltando)}")

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

projeto = st.sidebar.selectbox("Projeto (nome_projeto)", options=[""] + projetos, index=0)

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
    campanha = st.sidebar.selectbox("Campanha (nome_campanha)", options=["(todas)"] + campanhas, index=0)
else:
    st.sidebar.selectbox("Campanha (nome_campanha)", options=["(preencha projeto)"], index=0, disabled=True)

tema = st.sidebar.selectbox("Tema", options=["cliente_azul", "cliente_verde", "neutro"], index=0)

pasta_saida = st.sidebar.text_input("Pasta de saída (exports)", value=r"G:\Meu Drive\Opyta\Opyta_Data\exports")
exportar_arquivos = st.sidebar.toggle("Exportar arquivos (xlsx/png)", value=False)

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

rodar = col_a.button("Rodar análises do grupo", disabled=not bool(projeto) or df_base.empty)
col_b.caption("Executa o pacote do grupo. Se um bloco falhar, os demais continuam.")

if rodar:
    df_trabalho = df_base.copy()

    # filtro de campanha (se não for todas)
    if campanha and campanha != "(todas)" and "nome_campanha" in df_trabalho.columns:
        df_trabalho = df_trabalho[df_trabalho["nome_campanha"].astype(str).str.strip() == str(campanha).strip()]

    # contexto (usa o padrão de campanhas do theme.py)
    ordem = ordem_campanhas_padrao(grupo) or sorted(df_trabalho["nome_campanha"].dropna().astype(str).unique().tolist())

    ctx = RunContext(
        projeto=projeto,
        grupo=grupo,
        campanha=campanha,          # pode ser "(todas)"
        ponto=None,
        pasta_saida=Path(pasta_saida),
        tema=tema,
        exportar_arquivos=exportar_arquivos,
        ordem_campanhas=ordem,
    )

    st.markdown("---")

    # roteamento: por enquanto só ICTIO (você cria outros depois)
    if grupo.lower() in ("ictiofauna", "ictio"):
        results = run_ictio(ctx, df_trabalho)
    else:
        st.warning("Runner desse grupo ainda não foi conectado. (Por enquanto: Ictiofauna)")
        results = []

    # renderiza tudo o que o runner devolver
    if results:
        theme_obj = get_theme(tema)

        for i, res in enumerate(results, start=1):
            st.subheader(f"{i:02d}) {res.title}")

            if res.df is not None and not res.df.empty:
                st.dataframe(res.df, use_container_width=True)

            if res.fig is not None:
                # garante tema nos gráficos (se algum bloco não aplicou)
                try:
                    res.fig.update_traces()
                    res.fig.update_layout()
                except Exception:
                    pass
                st.plotly_chart(res.fig, use_container_width=True)

            if getattr(res, "warnings", None):
                for w in res.warnings:
                    st.warning(w)

            if getattr(res, "files", None):
                if res.files:
                    st.caption("Exports:")
                    for f in res.files:
                        st.write(f"- {f}")

    else:
        st.info("Nenhum resultado retornado pelo runner.")