from __future__ import annotations

import streamlit as st

# ------------------------------------------------
# Verificação de login (sempre primeiro)
# ------------------------------------------------

if not st.session_state.get("logged_in"):
    st.switch_page("app/main.py")

# ------------------------------------------------
# Sidebar
# ------------------------------------------------

from core.sidebar import render_sidebar

render_sidebar()

# ------------------------------------------------
# Imports do sistema
# ------------------------------------------------

import os
import re
import unicodedata
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from core.engine import get_engine
from runners.registry import ACTIONS, GROUP_TO_ACTION_KEY
from runners.script_runner import run_python_script
from validators.registry import VALIDATORS


# ------------------------------------------------
# Segurança extra
# ------------------------------------------------

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()


# ------------------------------------------------
# Página
# ------------------------------------------------

st.title("01 — Importação")
st.info("DEBUG IMPORTACAO V2")

# Raiz do projeto: .../Opyta_Data
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RUNTIME_ROOT = PROJECT_ROOT / "runtime" / "importacao"
RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)

# ============================================================
# Normalização / Correção segura (automática)
# ============================================================

_HYPHENS = r"[\u2010\u2011\u2012\u2013\u2014\u2212]"


def normalize_text(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return x
    s = str(x)
    s = s.replace("\u00A0", " ")
    s = unicodedata.normalize("NFKC", s)
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(_HYPHENS, "-", s)
    s = re.sub(r"\s*-\s*", "-", s)
    return s


def normalize_df(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    df2 = df.copy()
    changes = 0

    for col in df2.columns:
        if df2[col].dtype == object:
            before = df2[col].copy()
            df2[col] = df2[col].apply(lambda v: normalize_text(v) if pd.notna(v) else v)
            mask = before.astype(str) != df2[col].astype(str)
            changes += int(mask.sum())

    return df2, changes


def write_clean_excel(excel_path: Path, cleaned_path: Path) -> int:
    xls = pd.ExcelFile(excel_path)
    total_changes = 0

    with pd.ExcelWriter(cleaned_path, engine="openpyxl") as writer:
        for sh in xls.sheet_names:
            df = pd.read_excel(xls, sh).dropna(how="all")
            df2, ch = normalize_df(df)
            total_changes += ch
            df2.to_excel(writer, sheet_name=sh, index=False)

    return total_changes


# ============================================================
# Ambiente / Secrets
# ============================================================

def get_runtime_env() -> dict[str, str]:
    env: dict[str, str] = {}

    database_url = None

    try:
        database_url = st.secrets["DATABASE_URL"]
    except Exception:
        database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError(
            "DATABASE_URL não encontrada. Configure esse secret no Streamlit Cloud."
        )

    env["DATABASE_URL"] = str(database_url).strip()

    try:
        supabase_url = st.secrets["SUPABASE_URL"]
        if supabase_url:
            env["SUPABASE_URL"] = str(supabase_url).strip()
    except Exception:
        if os.getenv("SUPABASE_URL"):
            env["SUPABASE_URL"] = os.getenv("SUPABASE_URL", "").strip()

    try:
        supabase_anon_key = st.secrets["SUPABASE_ANON_KEY"]
        if supabase_anon_key:
            env["SUPABASE_ANON_KEY"] = str(supabase_anon_key).strip()
    except Exception:
        if os.getenv("SUPABASE_ANON_KEY"):
            env["SUPABASE_ANON_KEY"] = os.getenv("SUPABASE_ANON_KEY", "").strip()

    return env


# ============================================================
# Resumo 2.1 — pós-migração
# ============================================================

def gerar_resumo_pos_migracao(grupo: str, excel_path: Path):
    """
    Resumo robusto:
    - Projeto: via Codigo_Opyta do Excel
    - Campanhas / pontos: mostra o escopo do Excel
    - Esforços / resultados: conta no banco por projeto + grupo
    """
    xls = pd.ExcelFile(excel_path)

    df_capa = pd.read_excel(xls, "Capa_Projeto")
    df_pontos = pd.read_excel(xls, "Pontos_e_Campanhas").dropna(how="all")

    codigo_raw = str(df_capa.iloc[0].get("Codigo_Opyta", "") or "")
    codigo = codigo_raw.replace("\u00A0", " ").strip()

    campanhas_excel = sorted([
        str(x).replace("\u00A0", " ").strip()
        for x in df_pontos["Campanha"].dropna().unique().tolist()
    ])
    pontos_excel = sorted([
        str(x).replace("\u00A0", " ").strip()
        for x in df_pontos["Ponto"].dropna().unique().tolist()
    ])

    engine = None

    try:
        engine = get_engine()

        with engine.begin() as conn:
            id_projeto = conn.execute(
                text(
                    """
                    SELECT id_projeto
                    FROM projetos
                    WHERE LOWER(TRIM(REPLACE(codigo_interno_opyta, CHR(160), ''))) =
                          LOWER(TRIM(REPLACE(:c, CHR(160), '')))
                    """
                ),
                {"c": codigo},
            ).scalar()

            if not id_projeto:
                st.warning(f"Projeto não encontrado no banco para Código_Opyta = {codigo}")
                return

            rows_camp = conn.execute(
                text(
                    """
                    SELECT DISTINCT ca.nome_campanha
                    FROM campanhas ca
                    JOIN pontos_coleta pc ON pc.id_campanha = ca.id_campanha
                    WHERE pc.id_projeto = :idp
                    ORDER BY ca.nome_campanha
                    """
                ),
                {"idp": id_projeto},
            ).fetchall()
            campanhas_banco = [r[0] for r in rows_camp]

            rows_pontos = conn.execute(
                text(
                    """
                    SELECT DISTINCT nome_ponto
                    FROM pontos_coleta
                    WHERE id_projeto = :idp
                    ORDER BY nome_ponto
                    """
                ),
                {"idp": id_projeto},
            ).fetchall()
            pontos_banco = [r[0] for r in rows_pontos]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Projeto", codigo if codigo else "-")
            c2.metric("Campanhas (Excel)", len(campanhas_excel))
            c3.metric("Pontos (Excel)", len(pontos_excel))
            c4.metric("Grupo", grupo)

            with st.expander("Ver escopo do Excel"):
                st.write("**Campanhas (Excel):**", campanhas_excel)
                st.write("**Pontos (Excel):**", pontos_excel)

            with st.expander("Ver escopo encontrado no banco"):
                st.write("**Campanhas (Banco):**", campanhas_banco)
                st.write("**Pontos (Banco):**", pontos_banco)

            if grupo == "Meio Físico":
                total_res = conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM resultados_analise ra
                        JOIN pontos_coleta pc ON pc.id_ponto_coleta = ra.id_ponto_coleta
                        WHERE pc.id_projeto = :idp
                        """
                    ),
                    {"idp": id_projeto},
                ).scalar()

                a, b = st.columns(2)
                a.metric("Status", "OK")
                b.metric("Resultados (Banco)", int(total_res or 0))
                return

            tabela_map = {
                "Ictiofauna": "resultados_ictiofauna",
                "Bentos": "resultados_zoobentos",
                "Fitoplâncton": "resultados_fitoplancton",
                "Zooplâncton": "resultados_zooplancton",
            }

            grupo_banco_map = {
                "Ictiofauna": "Ictiofauna",
                "Bentos": "Zoobentos",
                "Fitoplâncton": "Fitoplancton",
                "Zooplâncton": "Zooplancton",
            }

            tabela = tabela_map.get(grupo)
            grupo_banco = grupo_banco_map.get(grupo)

            if not tabela or not grupo_banco:
                st.warning(f"Sem mapeamento de banco para o grupo '{grupo}'.")
                return

            total_esforcos = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM esforcos_amostragem e
                    JOIN pontos_coleta pc ON pc.id_ponto_coleta = e.id_ponto_coleta
                    WHERE pc.id_projeto = :idp
                      AND e.grupo_biologico = :g
                    """
                ),
                {"idp": id_projeto, "g": grupo_banco},
            ).scalar()

            total_res = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {tabela} r
                    JOIN esforcos_amostragem e ON e.id_esforco = r.id_esforco
                    JOIN pontos_coleta pc ON pc.id_ponto_coleta = e.id_ponto_coleta
                    WHERE pc.id_projeto = :idp
                      AND e.grupo_biologico = :g
                    """
                ),
                {"idp": id_projeto, "g": grupo_banco},
            ).scalar()

            a, b, c = st.columns(3)
            a.metric("Status", "OK")
            b.metric("Esforços (Banco)", int(total_esforcos or 0))
            c.metric("Resultados (Banco)", int(total_res or 0))

    finally:
        if engine is not None:
            engine.dispose()


# ============================================================
# UI — seleção / upload
# ============================================================

grupo = st.selectbox("Grupo", list(GROUP_TO_ACTION_KEY.keys()))
uploaded = st.file_uploader("Upload do Excel", type=["xlsx"])

action_key = GROUP_TO_ACTION_KEY[grupo]
spec = ACTIONS[action_key]

runtime_dir = RUNTIME_ROOT / action_key.lower()
runtime_dir.mkdir(parents=True, exist_ok=True)

excel_path = None
if uploaded is not None:
    excel_path = runtime_dir / spec.expected_excel_name
    excel_path.write_bytes(uploaded.getbuffer())


# ============================================================
# Validação (1 etapa) = corrige + valida estrutura
# ============================================================

st.subheader("Validação (1 etapa)")

if "validated_ok" not in st.session_state:
    st.session_state["validated_ok"] = False
if "excel_para_migrar" not in st.session_state:
    st.session_state["excel_para_migrar"] = None
if "clean_changes" not in st.session_state:
    st.session_state["clean_changes"] = 0

btn_validate = st.button("Validar (corrige automaticamente)", disabled=(excel_path is None))

if btn_validate:
    cleaned_path = runtime_dir / f"clean_{excel_path.name}"
    total_changes = write_clean_excel(excel_path, cleaned_path)

    st.session_state["clean_changes"] = int(total_changes)
    st.session_state["excel_para_migrar"] = str(cleaned_path)

    st.success("Correção automática concluída ✅")
    st.metric("Correções automáticas aplicadas", st.session_state["clean_changes"])

    xls_clean = pd.ExcelFile(cleaned_path)
    ok, errors = VALIDATORS[grupo].validate(xls_clean)

    if ok:
        st.success("Validação estrutural OK ✅")
        st.session_state["validated_ok"] = True
        st.info("Pronto para migrar: a migração usará o Excel limpo automaticamente.")
    else:
        st.session_state["validated_ok"] = False
        st.error("Validação estrutural falhou ❌")
        for e in errors:
            st.write("-", e)
        st.warning("Corrija os itens acima e valide novamente.")


# ============================================================
# Migração
# ============================================================

st.subheader("Migração")

excel_para_migrar = None
if st.session_state.get("excel_para_migrar"):
    excel_para_migrar = Path(st.session_state["excel_para_migrar"])
elif excel_path is not None:
    excel_para_migrar = excel_path

can_migrate = bool(st.session_state.get("validated_ok", False)) and excel_para_migrar is not None

if st.button("Migrar", disabled=not can_migrate):
    script_abs = (PROJECT_ROOT / spec.script).resolve()

    try:
        extra_env = get_runtime_env()
    except Exception as e:
        st.error(f"Falha ao preparar ambiente da migração: {e}")
        st.stop()

    res = run_python_script(
        script_path=str(script_abs),
        args=[str(excel_para_migrar.resolve())],
        cwd=runtime_dir,
        extra_env=extra_env,
    )

    ok = getattr(res, "status", "") == "success"

    if ok:
        st.success("Migração concluída ✅")
    else:
        st.error("Migração falhou ❌")

    stdout = getattr(res, "stdout", "") or ""
    stderr = getattr(res, "stderr", "") or ""

    if stdout.strip():
        st.code(stdout, language="text")
    if stderr.strip():
        st.code(stderr, language="text")

    st.markdown("---")
    st.subheader("Resumo pós-migração (2.1)")
    try:
        gerar_resumo_pos_migracao(grupo, excel_para_migrar.resolve())
    except Exception as e:
        st.warning(f"Falha ao gerar resumo pós-migração: {e}")