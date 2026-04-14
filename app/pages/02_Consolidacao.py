from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.app_state import initialize_system_status, mark_stage_completed
from core.ui.layout import (
    extract_alert_lines,
    inject_saas_styles,
    render_alert_block,
    render_executive_summary,
    render_technical_log,
)
from core.engine import get_engine
from core.sidebar import render_sidebar
from core.supabase_client import get_supabase
from runners.script_runner import run_python_script
from runners.registry import ACTIONS


if not st.session_state.get("logged_in"):
    st.switch_page("main.py")

render_sidebar()

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faca login para acessar esta pagina.")
    st.stop()

initialize_system_status()
inject_saas_styles()

st.title("02 - Consolidacao")

st.markdown(
    "Consolidacao orientada a valor analitico: resultado claro, insights rapidos e rastreabilidade tecnica."
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

supabase = get_supabase()
RUNTIME_DIR = Path("runtime/consolidacao")
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def parse_consolidacao_stdout(stdout: str) -> dict:
    data = {
        "processados": 0,
        "consolidados": 0,
        "descartados": 0,
        "alerts": [],
    }
    if not stdout:
        return data

    match = re.search(r"(\d+)\s+registros\s+consolidados", stdout, re.IGNORECASE)
    if match:
        data["consolidados"] = int(match.group(1))
        data["processados"] = int(match.group(1))

    for line in stdout.splitlines():
        line_clean = line.strip()
        if not line_clean:
            continue
        low = line_clean.lower()
        if "aviso" in low or "warning" in low or "erro" in low:
            data["alerts"].append(line_clean)

    return data


def _get_source_total_count(conn) -> int:
    source_union = conn.execute(
        text(
            """
            SELECT
                (SELECT COUNT(*) FROM resultados_ictiofauna)
              + (SELECT COUNT(*) FROM resultados_zooplancton)
              + (SELECT COUNT(*) FROM resultados_zoobentos)
              + (SELECT COUNT(*) FROM resultados_fitoplancton)
              + (SELECT COUNT(*) FROM resultados_avifauna)
              + (SELECT COUNT(*) FROM resultados_herpetofauna)
              + (SELECT COUNT(*) FROM resultados_mastofauna)
            """
        )
    ).scalar()
    return int(source_union or 0)










def get_consolidacao_insights() -> dict:
    engine = None
    out = {
        "consolidados": 0,
        "grupos": 0,
        "campanhas": 0,
        "pontos": 0,
        "especies": 0,
        "fonte_total": 0,
        "preview": pd.DataFrame(),
        "error": None,
    }
    try:
        engine = get_engine()
        with engine.begin() as conn:
            out["consolidados"] = int(
                conn.execute(text("SELECT COUNT(*) FROM biota_analise_consolidada")).scalar() or 0
            )
            out["grupos"] = int(
                conn.execute(text("SELECT COUNT(DISTINCT grupo_biologico) FROM biota_analise_consolidada")).scalar() or 0
            )
            out["campanhas"] = int(
                conn.execute(text("SELECT COUNT(DISTINCT nome_campanha) FROM biota_analise_consolidada")).scalar() or 0
            )
            out["pontos"] = int(
                conn.execute(text("SELECT COUNT(DISTINCT nome_ponto) FROM biota_analise_consolidada")).scalar() or 0
            )
            out["especies"] = int(
                conn.execute(text("SELECT COUNT(DISTINCT nome_cientifico) FROM biota_analise_consolidada")).scalar() or 0
            )
            out["fonte_total"] = _get_source_total_count(conn)
            out["preview"] = pd.read_sql(
                text(
                    """
                    SELECT nome_projeto, nome_campanha, nome_ponto, grupo_biologico, nome_cientifico, contagem
                    FROM biota_analise_consolidada
                    ORDER BY nome_projeto, nome_campanha, nome_ponto
                    LIMIT 25
                    """
                ),
                conn,
            )
    except Exception as exc:
        out["error"] = str(exc)
    finally:
        if engine is not None:
            engine.dispose()
    return out


def write_log(grupo: str, status: str, mensagem: str):
    payload = {
        "grupo": grupo,
        "projeto": None,
        "campanha": None,
        "usuario": None,
        "data_execucao": datetime.now().isoformat(),
        "status": status,
        "mensagem": (mensagem or "")[:5000],
    }
    try:
        supabase.table("import_logs").insert(payload).execute()
    except Exception as exc:
        st.warning(f"Falha ao gravar log no banco (import_logs): {exc}")


spec = ACTIONS["CONSOLIDAR"]

if "consolidacao_result" not in st.session_state:
    st.session_state["consolidacao_result"] = None

st.warning("⚠️ A **Consolidação é obrigatória** para que seus dados importados fiquem disponíveis para análise. Este processo é **definitivo** — não há volta atrás.")
st.info("Executa o script de consolidacao (processar_dados.py) que organiza dados importados em base analitica unica.")

if "consolidacao_result" not in st.session_state:
    st.session_state["consolidacao_result"] = None


if st.button("Rodar Consolidacao Agora", use_container_width=True, key="rodar_consolidacao"):
    script_abs = PROJECT_ROOT / spec.script
    
    try:
        simulation_ok = True
        status_box = st.status("Consolidando dados...", expanded=True)
        progress = st.progress(0, text="Iniciando consolidacao")
        line_count = [0]

        def _on_output_line(line: str):
            line_count[0] += 1
            pct = min(95, int((line_count[0] / 50) * 95))
            progress.progress(pct, text="Executando consolidacao")
            if line.strip():
                status_box.write(line.strip())

        with st.spinner("Consolidando dados..."):
            res = run_python_script(str(script_abs), cwd=RUNTIME_DIR, on_output_line=_on_output_line)

        progress.progress(100, text="Concluido")
        status_box.update(
            label="Consolidacao concluida com sucesso ✅" if res.status == "success" else "Consolidacao finalizada com erro ❌",
            state="complete" if res.status == "success" else "error",
        )

        write_log(spec.key, res.status, res.stdout or "")

        parsed = parse_consolidacao_stdout(res.stdout or "")
        insights = get_consolidacao_insights()
        st.session_state["consolidacao_result"] = {
            "status": res.status,
            "stdout": res.stdout or "",
            "stderr": res.stderr or "",
            "parsed": parsed,
            "insights": insights,
        }

        if res.status == "success":
            mark_stage_completed("consolidacao_status")
        
    except Exception as exc:
        st.error(f"Erro ao consolidar dados: {exc}")


consolidacao_result = st.session_state.get("consolidacao_result")
if consolidacao_result:
    parsed = consolidacao_result["parsed"]
    insights = consolidacao_result["insights"]
    consolidado = int(insights.get("consolidados", 0) or parsed.get("consolidados", 0) or 0)
    processados = int(parsed.get("processados", 0) or consolidado)
    descartados = max(int(insights.get("fonte_total", 0)) - consolidado, 0)

    if consolidacao_result["status"] == "success":
        st.success("✅ Consolidacao concluida com sucesso!")
    else:
        st.error("❌ A consolidacao terminou com erro.")

    render_executive_summary(
        "",
        [
            {"label": "Total processados", "value": processados, "hint": "Origem consolidada", "status": "info"},
            {"label": "Registros consolidados", "value": consolidado, "hint": "Base analitica", "status": "ok"},
            {"label": "Registros descartados", "value": descartados, "hint": "Nao encaixaram", "status": "warn" if descartados else "ok"},
            {"label": "Grupos processados", "value": insights.get("grupos", 0), "hint": "Diversidade consolidada", "status": "info"},
        ],
    )

    st.markdown("### Insights rapidos")
    col1, col2, col3 = st.columns(3)
    col1.metric("Campanhas", insights.get("campanhas", 0))
    col2.metric("Pontos", insights.get("pontos", 0))
    col3.metric("Especies", insights.get("especies", 0))

    st.markdown("### Alertas")
    alertas = []
    alertas.extend(parsed.get("alerts", []))
    alertas.extend(extract_alert_lines(consolidacao_result["stdout"], consolidacao_result["stderr"]))
    if insights.get("error"):
        alertas.append(f"Falha ao carregar insights da base consolidada: {insights['error']}")
    if descartados > 0:
        alertas.append(f"{descartados} registros nao entraram na base consolidada final.")
    
    if alertas:
        render_alert_block(alertas, title="Riscos e inconsistencias")

    st.markdown("### Preview da base consolidada")
    preview = insights.get("preview")
    if isinstance(preview, pd.DataFrame) and not preview.empty:
        st.dataframe(preview, use_container_width=True)
    else:
        st.info("Sem dados para preview.")

    st.markdown("### Log tecnico")
    render_technical_log(consolidacao_result["stdout"], consolidacao_result["stderr"], title="Ver log tecnico")


    st.markdown("### Proximas etapas")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("➜ Ir para Analises", use_container_width=True, key="cons_go_analises"):
            st.switch_page("pages/03_Analises.py")
    with col2:
        if st.button("🔄 Nova consolidacao", use_container_width=True, key="cons_reset"):
            st.session_state["consolidacao_result"] = None
            st.rerun()

    csv_data = ""
    if isinstance(preview, pd.DataFrame) and not preview.empty:
        csv_data = preview.to_csv(index=False)

    
    st.download_button(
        "📥 Exportar preview",
        data=csv_data.encode("utf-8"),
        file_name="preview_biota_consolidada.csv",
        mime="text/csv",
        key="download_consolidacao_preview",
    )
    st.markdown("### Alertas")

