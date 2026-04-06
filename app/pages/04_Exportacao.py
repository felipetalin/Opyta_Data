from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.engine import get_engine
from core.sidebar import render_sidebar

if not st.session_state.get("logged_in"):
    st.switch_page("main.py")

st.set_page_config(page_title="04 - Exportação", layout="wide")

render_sidebar()

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()

st.title("04 — Exportação")
st.info("Exporte dados do banco para CSV ou Excel. Selecione filtros e baixe o resultado.")

EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


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
def listar_grupos() -> list[str]:
    engine = get_engine()
    sql = """
        SELECT DISTINCT grupo_biologico
        FROM public.biota_analise_consolidada
        WHERE grupo_biologico IS NOT NULL
        ORDER BY 1
    """
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df["grupo_biologico"].astype(str).tolist()


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
def carregar_dados(projeto: str, grupo: str, campanha: str) -> pd.DataFrame:
    engine = get_engine()
    clauses: list[str] = []
    params: dict[str, str] = {}

    if projeto:
        clauses.append("nome_projeto = %(projeto)s")
        params["projeto"] = projeto
    if grupo:
        clauses.append("grupo_biologico = %(grupo)s")
        params["grupo"] = grupo
    if campanha and campanha != "(todas)":
        clauses.append("nome_campanha = %(campanha)s")
        params["campanha"] = campanha

    query = "SELECT * FROM public.biota_analise_consolidada"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY nome_projeto, grupo_biologico, nome_campanha"

    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


def criar_arquivo_csv(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


def criar_arquivo_excel(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="dados")
    return buffer.getvalue()


with st.expander("Filtros de exportação", expanded=True):
    projetos = listar_projetos()
    grupos = listar_grupos()

    projeto = st.selectbox("Projeto", options=[""] + projetos, index=0)
    grupo = st.selectbox("Grupo biológico", options=[""] + grupos, index=0)

    campanhas: list[str] = []
    campanha = ""
    if projeto and grupo:
        try:
            campanhas = listar_campanhas(projeto, grupo)
        except Exception:
            campanhas = []

        campanha = st.selectbox(
            "Campanha",
            options=["(todas)"] + campanhas,
            index=0,
        )
    else:
        st.selectbox("Campanha", options=["(preencha projeto e grupo)"], index=0, disabled=True)

    st.markdown("---")
    st.write(
        "Use os filtros para reduzir o volume exportado. Se nenhum filtro for selecionado, o sistema pode carregar muitos registros."
    )

carregar = st.button("Carregar dados para exportação")

if carregar:
    if not projeto and not grupo:
        st.warning("Selecione ao menos Projeto ou Grupo para evitar consultas muito grandes.")
    else:
        with st.spinner("Consultando dados..."):
            try:
                df = carregar_dados(projeto, grupo, campanha)
            except Exception as exc:
                st.error(f"Erro ao consultar dados: {exc}")
                df = pd.DataFrame()

        if df.empty:
            st.warning("Nenhum registro encontrado com os filtros selecionados.")
        else:
            st.success(f"{len(df):,} registros carregados.")
            st.dataframe(df.head(100), use_container_width=True)

            nome_base = projeto or grupo or "export"
            nome_campanha = campanha if campanha and campanha != "(todas)" else "todas"
            nome_base = nome_base.replace(" ", "_").lower()

            csv_bytes = criar_arquivo_csv(df)
            xlsx_bytes = criar_arquivo_excel(df)

            csv_name = f"export_{nome_base}_{nome_campanha}.csv"
            xlsx_name = f"export_{nome_base}_{nome_campanha}.xlsx"

            st.download_button(
                label="Baixar CSV",
                data=csv_bytes,
                file_name=csv_name,
                mime="text/csv",
            )
            st.download_button(
                label="Baixar Excel",
                data=xlsx_bytes,
                file_name=xlsx_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            Path(EXPORT_DIR / csv_name).write_bytes(csv_bytes)
            Path(EXPORT_DIR / xlsx_name).write_bytes(xlsx_bytes)

            st.info(f"Arquivos também salvos localmente em {EXPORT_DIR.resolve()}")
