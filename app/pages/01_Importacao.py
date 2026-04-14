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

from core.app_state import initialize_system_status, mark_stage_completed
from core.ui.layout import (
    extract_alert_lines,
    inject_saas_styles,
    render_alert_block,
    render_executive_summary,
    render_stepper,
    render_technical_log,
)
from core.engine import get_engine
from runners.registry import ACTIONS, GROUP_TO_ACTION_KEY
from runners.script_runner import run_python_script
from validators.registry import VALIDATORS
from validators.importacao import render_validation_report, validate_importacao_file


# ------------------------------------------------
# Segurança extra
# ------------------------------------------------

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()

initialize_system_status()
inject_saas_styles()


# ------------------------------------------------
# Página
# ------------------------------------------------

st.title("01 — Importação")

st.markdown(
    "Fluxo guiado com validacao, migracao monitorada e resultado executivo para tomada de decisao."
)

# Raiz do projeto: .../Opyta_Data
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RUNTIME_ROOT = PROJECT_ROOT / "runtime" / "importacao"
RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)


@st.cache_data(show_spinner=False, ttl=90)
def get_importacao_health() -> dict:
    out = {
        "logs_importacao": 0,
        "projetos_importados": 0,
        "ultima_execucao": "-",
        "db_ok": False,
        "erro": None,
    }

    engine = None
    try:
        engine = get_engine()
        with engine.begin() as conn:
            out["logs_importacao"] = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM import_logs
                        WHERE etapa = 'IMPORTACAO'
                        """
                    )
                ).scalar()
                or 0
            )
            out["projetos_importados"] = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(DISTINCT id_projeto)
                        FROM pontos_coleta
                        """
                    )
                ).scalar()
                or 0
            )
            out["ultima_execucao"] = str(
                conn.execute(
                    text(
                        """
                        SELECT MAX(data_execucao)::text
                        FROM import_logs
                        WHERE etapa = 'IMPORTACAO'
                        """
                    )
                ).scalar()
                or "-"
            )
            out["db_ok"] = True
    except Exception as exc:
        out["erro"] = str(exc)
    finally:
        if engine is not None:
            engine.dispose()

    return out


health = get_importacao_health()

st.warning(
    "⚠️ **IMPORTANTE:** Após importar seus dados, você DEVE executar a **Consolidação** para que os dados fiquem disponíveis para análise. "
    "Muitas vezes os usuários se esquecem dessa etapa crítica. Acesse a página **02 - Consolidação** após terminar suas importações!"
)

st.markdown("### Visão operacional")
render_executive_summary(
    "Resumo da etapa",
    [
        {
            "label": "Execuções da etapa",
            "value": health["logs_importacao"],
            "hint": "Histórico registrado no banco",
            "status": "info",
        },
        {
            "label": "Projetos com dados",
            "value": health["projetos_importados"],
            "hint": "Projetos já carregados",
            "status": "ok" if health["db_ok"] else "warn",
        },
        {
            "label": "Última importação",
            "value": health["ultima_execucao"],
            "hint": "Último processamento identificado",
            "status": "info",
        },
    ],
)

st.markdown("### Operação de importação")

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


def normalize_group_key(value: str | None) -> str:
    if not value:
        return ""
    return normalize_text(value).lower()


def get_group_result_mapping() -> tuple[dict[str, str], dict[str, str]]:
    """Fonte única de mapeamento grupo -> tabela e grupo_banco."""
    tabela_map = {
        "ictiofauna": "resultados_ictiofauna",
        "bentos": "resultados_zoobentos",
        "zoobentos": "resultados_zoobentos",
        "fitoplâncton": "resultados_fitoplancton",
        "fitoplancton": "resultados_fitoplancton",
        "zooplâncton": "resultados_zooplancton",
        "zooplancton": "resultados_zooplancton",
        "avifauna": "resultados_avifauna",
        "herpetofauna": "resultados_herpetofauna",
        "mastofauna": "resultados_mastofauna",
    }

    grupo_banco_map = {
        "ictiofauna": "Ictiofauna",
        "bentos": "Zoobentos",
        "zoobentos": "Zoobentos",
        "fitoplâncton": "Fitoplancton",
        "fitoplancton": "Fitoplancton",
        "zooplâncton": "Zooplancton",
        "zooplancton": "Zooplancton",
        "avifauna": "Avifauna",
        "herpetofauna": "Herpetofauna",
        "mastofauna": "Mastofauna",
    }
    return tabela_map, grupo_banco_map


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

def gerar_resumo_pos_migracao(grupo: str, excel_path: Path) -> dict:
    """
    Resumo robusto:
    - Projeto: via Codigo_Opyta do Excel
    - Campanhas / pontos: mostra o escopo do Excel
    - Esforços / resultados: conta no banco por projeto + grupo
    """
    output = {
        "projeto": "-",
        "campanhas_excel": [],
        "pontos_excel": [],
        "campanhas_banco": [],
        "pontos_banco": [],
        "esforcos_banco": 0,
        "resultados_banco": 0,
        "especies_banco": 0,
        "status": "ok",
        "alertas": [],
    }

    xls = pd.ExcelFile(excel_path)

    df_capa = pd.read_excel(xls, "Capa_Projeto")
    df_pontos = pd.read_excel(xls, "Pontos_e_Campanhas").dropna(how="all")

    codigo_raw = str(df_capa.iloc[0].get("Codigo_Opyta", "") or "")
    codigo = codigo_raw.replace("\u00A0", " ").strip()
    output["projeto"] = codigo or "-"

    campanhas_excel = sorted([
        str(x).replace("\u00A0", " ").strip()
        for x in df_pontos["Campanha"].dropna().unique().tolist()
    ])
    output["campanhas_excel"] = campanhas_excel
    pontos_excel = sorted([
        str(x).replace("\u00A0", " ").strip()
        for x in df_pontos["Ponto"].dropna().unique().tolist()
    ])
    output["pontos_excel"] = pontos_excel

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
                output["status"] = "warning"
                output["alertas"].append(
                    f"Projeto nao encontrado no banco para Codigo_Opyta = {codigo}."
                )
                return output

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
            output["campanhas_banco"] = campanhas_banco

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
            output["pontos_banco"] = pontos_banco

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
                output["resultados_banco"] = int(total_res or 0)
                return output

            tabela_map, grupo_banco_map = get_group_result_mapping()

            grupo_norm = normalize_group_key(grupo)
            tabela = tabela_map.get(grupo_norm)
            grupo_banco = grupo_banco_map.get(grupo_norm)

            if not tabela or not grupo_banco:
                output["status"] = "warning"
                output["alertas"].append(f"Sem mapeamento de banco para o grupo '{grupo}'.")
                return output

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
            output["esforcos_banco"] = int(total_esforcos or 0)

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
            output["resultados_banco"] = int(total_res or 0)

            total_especies = conn.execute(
                text(
                    f"""
                    SELECT COUNT(DISTINCT r.id_especie)
                    FROM {tabela} r
                    JOIN esforcos_amostragem e ON e.id_esforco = r.id_esforco
                    JOIN pontos_coleta pc ON pc.id_ponto_coleta = e.id_ponto_coleta
                    WHERE pc.id_projeto = :idp
                      AND e.grupo_biologico = :g
                    """
                ),
                {"idp": id_projeto, "g": grupo_banco},
            ).scalar()
            output["especies_banco"] = int(total_especies or 0)

    finally:
        if engine is not None:
            engine.dispose()

    return output


def parse_inserted_records(stdout: str) -> int:
    if not stdout:
        return 0
    matches = re.findall(r"(\d+)\s+registros", stdout, re.IGNORECASE)
    if not matches:
        return 0
    return max(int(v) for v in matches)


def render_scope_list(title: str, items: list[str], ok: bool = True) -> None:
    icon = "OK" if ok else "!"
    if not items:
        st.write(f"{icon} {title}: sem itens")
        return
    st.write(f"{icon} {title} ({len(items)})")
    for item in items:
        st.write(f"- {item}")


# ============================================================
# UI — seleção / upload
# ============================================================

if "import_result" not in st.session_state:
    st.session_state["import_result"] = None

grupo = st.selectbox("Grupo", list(GROUP_TO_ACTION_KEY.keys()))
uploaded = st.file_uploader("Upload do Excel", type=["xlsx"])


def _uploaded_signature(file_obj) -> str | None:
    if file_obj is None:
        return None
    return f"{file_obj.name}|{file_obj.size}|{grupo}"


current_upload_signature = _uploaded_signature(uploaded)
previous_upload_signature = st.session_state.get("import_upload_signature")

if current_upload_signature != previous_upload_signature:
    st.session_state["validated_ok"] = False
    st.session_state["excel_para_migrar"] = None
    st.session_state["clean_changes"] = 0
    st.session_state["import_result"] = None
    st.session_state["import_upload_signature"] = current_upload_signature

action_key = GROUP_TO_ACTION_KEY[grupo]
spec = ACTIONS[action_key]

runtime_dir = RUNTIME_ROOT / action_key.lower()
runtime_dir.mkdir(parents=True, exist_ok=True)

excel_path = None
if uploaded is not None:
    excel_path = runtime_dir / spec.expected_excel_name
    excel_path.write_bytes(uploaded.getbuffer())

validated_ok = bool(st.session_state.get("validated_ok", False))

if uploaded is None:
    step_index = 0
elif uploaded is not None and not validated_ok:
    step_index = 1
elif validated_ok and st.session_state.get("import_result") is None:
    step_index = 2
else:
    step_index = 4 if st.session_state["import_result"]["status"] == "success" else 3

has_step_error = bool(
    st.session_state.get("import_result")
    and st.session_state["import_result"].get("status") != "success"
)

st.markdown("### Etapas")
render_stepper(["Upload", "Validacao", "Migracao", "Resultado"], current_step=step_index, has_error=has_step_error)


# ============================================================
# Validação (1 etapa) = corrige + valida estrutura
# ============================================================

st.subheader("Validacao")

if "validated_ok" not in st.session_state:
    st.session_state["validated_ok"] = False
if "excel_para_migrar" not in st.session_state:
    st.session_state["excel_para_migrar"] = None
if "clean_changes" not in st.session_state:
    st.session_state["clean_changes"] = 0

btn_validate = st.button("Validar planilha", disabled=(excel_path is None), key="import_validate")

if btn_validate:
    try:
        with st.spinner("Validando arquivo..."):
            # Etapa 1: Limpeza automática de texto
            cleaned_path = runtime_dir / f"clean_{excel_path.name}"
            total_changes = write_clean_excel(excel_path, cleaned_path)

            st.session_state["clean_changes"] = int(total_changes)
            st.session_state["excel_para_migrar"] = str(cleaned_path)
            st.success("Limpeza automática concluída")
            st.metric("Correções de texto aplicadas", st.session_state["clean_changes"])

            # Etapa 2: Validação estrutural (abas, colunas)
            xls_clean = pd.ExcelFile(cleaned_path)
            ok_estrutural, errors_estrutural = VALIDATORS[grupo].validate(xls_clean)

        if not ok_estrutural:
            st.session_state["validated_ok"] = False
            st.error("Validação estrutural falhou:")
            for e in errors_estrutural:
                st.write("-", e)
            st.warning("Corrija os itens acima e valide novamente.")
        else:
            st.success("Estrutura validada ✅")

            # Etapa 3: Validação de dados (coordenadas, espécies, esforço, refs cruzadas)
            from io import BytesIO
            engine = None
            try:
                engine = get_engine()
                report = validate_importacao_file(
                    BytesIO(Path(cleaned_path).read_bytes()),
                    group=grupo,
                    engine=engine,
                )
            finally:
                if engine is not None:
                    engine.dispose()

            st.session_state["import_data_report"] = report

            render_validation_report(report)

            if report.can_proceed:
                mark_stage_completed("importacao_status")
                st.success("✅ Arquivo pronto para migração!")
                st.session_state["validated_ok"] = True
            else:
                st.session_state["validated_ok"] = False
                st.warning("Há bloqueios que impedem a migração. Corrija-os e valide novamente.")

    except Exception as exc:
        st.session_state["validated_ok"] = False
        st.error(f"Erro ao validar arquivo: {exc}")

# Mostrar relatório de validação se disponível
import_data_report = st.session_state.get("import_data_report")
if import_data_report is not None and st.session_state.get("validated_ok") is None:
    st.markdown("---")
    st.markdown("### Último relatório de validação")
    render_validation_report(import_data_report)

# ============================================================
# Migração
# ============================================================

st.subheader("Migracao")

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

    try:
        progress_box = st.empty()
        status_box = st.status("Executando migracao...", expanded=True)
        progress = progress_box.progress(8, text="Preparando migracao")

        line_count = [0]

        def _on_output_line(line: str):
            line_count[0] += 1
            current = min(90, 10 + line_count[0] * 2)
            progress.progress(current, text="Migrando registros no banco")
            if line.strip():
                status_box.write(line.strip())

        with st.spinner("Migrando dados..."):
            res = run_python_script(
                script_path=str(script_abs),
                args=[str(excel_para_migrar.resolve())],
                cwd=runtime_dir,
                extra_env=extra_env,
                on_output_line=_on_output_line,
            )

        progress.progress(100 if res.status == "success" else 92, text="Concluido" if res.status == "success" else "Finalizado com erro")
        status_box.update(
            label="Migracao concluida com sucesso" if res.status == "success" else "Migracao finalizada com erro",
            state="complete" if res.status == "success" else "error",
        )

        ok = getattr(res, "status", "") == "success"

        if ok:
            mark_stage_completed("importacao_status")
            st.success("Migração concluída com sucesso!")
        else:
            st.error("A migração terminou com erro.")

        stdout = getattr(res, "stdout", "") or ""
        stderr = getattr(res, "stderr", "") or ""

        resumo = {"status": "warning", "alertas": ["Resumo indisponivel"], "projeto": "-", "campanhas_excel": [], "pontos_excel": [], "campanhas_banco": [], "pontos_banco": [], "esforcos_banco": 0, "resultados_banco": 0, "especies_banco": 0}
        try:
            resumo = gerar_resumo_pos_migracao(grupo, excel_para_migrar.resolve())
        except Exception as e:
            resumo["alertas"] = [f"Falha ao gerar resumo pos-migracao: {e}"]

        st.session_state["import_result"] = {
            "status": res.status,
            "stdout": stdout,
            "stderr": stderr,
            "resumo": resumo,
            "grupo": grupo,
            "registros_inseridos": parse_inserted_records(stdout),
        }
    except Exception as exc:
        st.error(f"Erro ao migrar dados: {exc}")


import_result = st.session_state.get("import_result")
if import_result:
    st.markdown("---")
    st.subheader("Resultado")

    if import_result["status"] == "success":
        st.success("Importacao concluida com sucesso!")
        st.warning("PROXIMO PASSO OBRIGATORIO: Execute a Consolidacao para seus dados ficarem disponiveis para analise.")
        if st.button("Ir para Consolidacao", use_container_width=True, key="import_button_consolidacao"):
            st.switch_page("pages/02_Consolidacao.py")
        st.markdown("---")

    resumo = import_result["resumo"]
    erros = 0 if import_result["status"] == "success" else 1
    registros = import_result.get("registros_inseridos") or resumo.get("resultados_banco") or 0

    render_executive_summary(
        "Resumo executivo",
        [
            {"label": "Projeto", "value": resumo.get("projeto", "-"), "hint": "Codigo Opyta", "status": "info"},
            {"label": "Campanhas", "value": len(resumo.get("campanhas_excel", [])), "hint": "Escopo do arquivo", "status": "info"},
            {"label": "Pontos processados", "value": len(resumo.get("pontos_excel", [])), "hint": "Escopo do arquivo", "status": "info"},
            {"label": "Registros inseridos", "value": registros, "hint": "Resultado da migracao", "status": "ok" if import_result["status"] == "success" else "warn"},
            {"label": "Erros", "value": erros, "hint": "Execucao atual", "status": "error" if erros else "ok"},
        ],
    )

    st.markdown("### Escopo processado")
    c1, c2 = st.columns(2)
    with c1:
        render_scope_list("Campanhas (Excel)", resumo.get("campanhas_excel", []), ok=True)
        render_scope_list("Pontos (Excel)", resumo.get("pontos_excel", []), ok=True)
    with c2:
        render_scope_list("Campanhas (Banco)", resumo.get("campanhas_banco", []), ok=True)
        render_scope_list("Pontos (Banco)", resumo.get("pontos_banco", []), ok=True)

    st.markdown("### Alertas")
    alerts = []
    alerts.extend(resumo.get("alertas", []))
    alerts.extend(extract_alert_lines(import_result["stdout"], import_result["stderr"]))
    render_alert_block(alerts, title="Inconsistencias e avisos")

    st.markdown("### Log tecnico")
    render_technical_log(import_result["stdout"], import_result["stderr"], title="Ver log tecnico")

    st.markdown("### Acoes")
    action = render_action_buttons(
        [
            {"label": "Ver dados importados", "key": "import_view_data", "primary": True},
            {"label": "Ir para consolidacao", "key": "import_go_consolidacao"},
            {"label": "Nova importacao", "key": "import_reset"},
        ]
    )

    if action == "import_view_data":
        try:
            st.switch_page("app/pages/03_Analises.py")
        except Exception:
            st.info("Nao foi possivel abrir a pagina de analises automaticamente.")
    elif action == "import_go_consolidacao":
        try:
            st.switch_page("app/pages/02_Consolidacao.py")
        except Exception:
            st.info("Nao foi possivel abrir a pagina de consolidacao automaticamente.")
    elif action == "import_reset":
        st.session_state["validated_ok"] = False
        st.session_state["excel_para_migrar"] = None
        st.session_state["clean_changes"] = 0
        st.session_state["import_result"] = None
        st.rerun()
