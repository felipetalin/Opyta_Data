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
    render_action_buttons,
    render_alert_block,
    render_executive_summary,
    render_stepper,
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


def run_safe_simulation() -> dict:
    engine = None
    out = {
        "ok": False,
        "source_total": 0,
        "target_before": 0,
        "estimated_discarded": 0,
        "message": "",
        "error": None,
    }
    try:
        engine = get_engine()
        with engine.begin() as conn:
            source_total = _get_source_total_count(conn)
            target_before = int(
                conn.execute(text("SELECT COUNT(*) FROM biota_analise_consolidada")).scalar() or 0
            )
            out["source_total"] = source_total
            out["target_before"] = target_before
            out["estimated_discarded"] = max(source_total - target_before, 0)
            out["ok"] = True
            out["message"] = "Simulacao concluida sem gravacao no banco."
    except Exception as exc:
        out["error"] = str(exc)
        out["message"] = "Falha ao simular consolidacao."
    finally:
        if engine is not None:
            engine.dispose()
    return out


def create_logical_backup() -> dict:
    engine = None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = f"biota_analise_consolidada_bkp_{ts}"
    out = {"ok": False, "table_name": table_name, "rows": 0, "error": None}
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(f'CREATE TABLE "{table_name}" AS TABLE biota_analise_consolidada'))
            rows = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
            out["rows"] = int(rows or 0)
            out["ok"] = True
    except Exception as exc:
        out["error"] = str(exc)
    finally:
        if engine is not None:
            engine.dispose()
    return out


def list_logical_backups() -> list[dict]:
    engine = None
    backups: list[dict] = []
    try:
        engine = get_engine()
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = 'public'
                      AND tablename LIKE 'biota_analise_consolidada_bkp_%'
                    ORDER BY tablename DESC
                    """
                )
            ).fetchall()

            for row in rows:
                table_name = row[0]
                count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
                match = re.search(r"(\d{8}_\d{6})$", table_name)
                backups.append(
                    {
                        "table_name": table_name,
                        "rows": int(count or 0),
                        "created_at": match.group(1) if match else "desconhecido",
                    }
                )
    except Exception:
        return []
    finally:
        if engine is not None:
            engine.dispose()
    return backups


def restore_from_backup(table_name: str) -> dict:
    engine = None
    out = {
        "ok": False,
        "source_backup": table_name,
        "safety_backup": None,
        "restored_rows": 0,
        "error": None,
    }

    if not re.match(r"^biota_analise_consolidada_bkp_\d{8}_\d{6}$", table_name or ""):
        out["error"] = "Nome de backup invalido."
        return out

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safety_backup = f"biota_analise_consolidada_pre_restore_{ts}"
    out["safety_backup"] = safety_backup

    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(f'CREATE TABLE "{safety_backup}" AS TABLE biota_analise_consolidada'))
            conn.execute(text("TRUNCATE TABLE biota_analise_consolidada"))
            conn.execute(text(f'INSERT INTO biota_analise_consolidada SELECT * FROM "{table_name}"'))
            restored_rows = conn.execute(text("SELECT COUNT(*) FROM biota_analise_consolidada")).scalar()
            out["restored_rows"] = int(restored_rows or 0)
            out["ok"] = True
    except Exception as exc:
        out["error"] = str(exc)
    finally:
        if engine is not None:
            engine.dispose()

    return out


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

st.info("Executa o script existente de consolidacao (processar_dados.py) com feedback de negocio.")

st.markdown("### Modo seguro")
simulate_first = st.checkbox(
    "Executar simulacao antes de gravar (recomendado)",
    value=True,
    help="A simulacao nao grava dados. Mostra impacto estimado para revisao antes da consolidacao real.",
)
confirm_real = st.checkbox(
    "Confirmo que desejo executar a consolidacao real com backup logico previo",
    value=False,
)
confirm_text = st.text_input(
    "Digite CONSOLIDAR para liberar a gravacao real",
    value="",
)

if "consolidacao_result" not in st.session_state:
    st.session_state["consolidacao_result"] = None

if "consolidacao_simulation" not in st.session_state:
    st.session_state["consolidacao_simulation"] = None

if "consolidacao_rollback_result" not in st.session_state:
    st.session_state["consolidacao_rollback_result"] = None

st.markdown("### Etapas")
if st.session_state["consolidacao_result"] is None:
    render_stepper(["Loading", "Validacao", "Consolidacao", "Finalizacao"], current_step=0)
else:
    last_status = st.session_state["consolidacao_result"]["status"]
    render_stepper(
        ["Loading", "Validacao", "Consolidacao", "Finalizacao"],
        current_step=4 if last_status == "success" else 3,
        has_error=(last_status != "success"),
    )

st.markdown("### Rollback seguro")
available_backups = list_logical_backups()
if not available_backups:
    st.info("Nenhum backup logico encontrado ainda para restauracao.")
else:
    backup_options = {f"{b['table_name']} | {b['rows']} linhas": b for b in available_backups}
    selected_backup_label = st.selectbox(
        "Selecione um backup para restaurar",
        options=list(backup_options.keys()),
        key="rollback_backup_select",
    )
    selected_backup = backup_options[selected_backup_label]

    render_executive_summary(
        "Backup selecionado",
        [
            {"label": "Tabela backup", "value": selected_backup["table_name"], "hint": "Origem da restauracao", "status": "info"},
            {"label": "Linhas no backup", "value": selected_backup["rows"], "hint": "Conteudo estimado a restaurar", "status": "ok"},
            {"label": "Carimbo", "value": selected_backup["created_at"], "hint": "Timestamp do snapshot", "status": "info"},
        ],
    )

    rollback_confirm = st.checkbox(
        "Confirmo que desejo restaurar a base consolidada a partir do backup selecionado",
        value=False,
        key="rollback_confirm",
    )
    rollback_text = st.text_input(
        "Digite RESTAURAR para liberar o rollback",
        value="",
        key="rollback_text",
    )

    if st.button("Restaurar backup selecionado", key="rollback_execute"):
        if not rollback_confirm or rollback_text.strip().upper() != "RESTAURAR":
            st.warning("Rollback bloqueado. Confirme explicitamente antes de restaurar.")
        else:
            rollback_result = restore_from_backup(selected_backup["table_name"])
            st.session_state["consolidacao_rollback_result"] = rollback_result
            if rollback_result.get("ok"):
                write_log(
                    spec.key,
                    "rollback",
                    (
                        f"Rollback realizado do backup {rollback_result['source_backup']} | "
                        f"snapshot de seguranca {rollback_result['safety_backup']} | "
                        f"linhas restauradas {rollback_result['restored_rows']}"
                    ),
                )
                st.success(
                    f"Rollback concluido com sucesso usando {rollback_result['source_backup']}. Snapshot de seguranca criado: {rollback_result['safety_backup']}."
                )
            else:
                st.error(f"Rollback falhou: {rollback_result.get('error') or 'erro desconhecido'}")

rollback_result = st.session_state.get("consolidacao_rollback_result")
if rollback_result:
    st.markdown("### Ultimo rollback")
    if rollback_result.get("ok"):
        render_executive_summary(
            "Resultado do rollback",
            [
                {"label": "Backup restaurado", "value": rollback_result.get("source_backup", "-"), "hint": "Origem usada", "status": "ok"},
                {"label": "Linhas restauradas", "value": rollback_result.get("restored_rows", 0), "hint": "Estado atual da consolidada", "status": "info"},
                {"label": "Snapshot de seguranca", "value": rollback_result.get("safety_backup", "-"), "hint": "Permite reverter o rollback", "status": "warn"},
            ],
        )
    else:
        render_alert_block(
            [f"Rollback falhou: {rollback_result.get('error') or 'erro desconhecido'}"],
            title="Falha no rollback",
        )

if st.button("Rodar Consolidacao (Modo Seguro)"):
    script_abs = PROJECT_ROOT / spec.script
    try:
        simulation_ok = True
        if simulate_first:
            simulation = run_safe_simulation()
            st.session_state["consolidacao_simulation"] = simulation
            write_log(spec.key, "simulacao", simulation.get("message") or "")
            simulation_ok = bool(simulation.get("ok"))

            if not simulation_ok:
                st.error(f"Simulacao falhou: {simulation.get('error') or 'erro desconhecido'}")
            else:
                st.success("Simulacao concluida sem gravacao.")

        real_released = confirm_real and confirm_text.strip().upper() == "CONSOLIDAR"
        if not simulation_ok:
            st.warning(
                "Consolidacao real bloqueada: a simulacao falhou. Corrija os alertas e tente novamente."
            )
        elif not real_released:
            st.warning(
                "Consolidacao real bloqueada no modo seguro. Revise a simulacao e confirme explicitamente para gravar."
            )
        else:
            backup = create_logical_backup()
            if not backup.get("ok"):
                st.error(f"Backup logico falhou. Consolidacao cancelada. Detalhe: {backup.get('error')}")
            else:
                st.info(f"Backup criado com sucesso: {backup['table_name']} ({backup['rows']} linhas).")
                write_log(
                    spec.key,
                    "backup",
                    f"Backup logico criado em {backup['table_name']} com {backup['rows']} linhas.",
                )

                status_box = st.status("Consolidando dados...", expanded=True)
                progress = st.progress(8, text="Preparando consolidacao")
                line_count = [0]

                def _on_output_line(line: str):
                    line_count[0] += 1
                    pct = min(92, 10 + line_count[0] * 4)
                    progress.progress(pct, text="Executando consolidacao")
                    if line.strip():
                        status_box.write(line.strip())

                with st.spinner("Consolidando dados..."):
                    res = run_python_script(str(script_abs), cwd=RUNTIME_DIR, on_output_line=_on_output_line)

                progress.progress(100 if res.status == "success" else 94, text="Concluido")
                status_box.update(
                    label="Consolidacao concluida com sucesso" if res.status == "success" else "Consolidacao finalizada com erro",
                    state="complete" if res.status == "success" else "error",
                )

                write_log(spec.key, res.status, f"Backup: {backup['table_name']}\n" + (res.stdout or ""))

                parsed = parse_consolidacao_stdout(res.stdout or "")
                insights = get_consolidacao_insights()
                st.session_state["consolidacao_result"] = {
                    "status": res.status,
                    "stdout": res.stdout or "",
                    "stderr": res.stderr or "",
                    "parsed": parsed,
                    "insights": insights,
                    "backup": backup,
                }

                if res.status == "success":
                    mark_stage_completed("consolidacao_status")
                    st.success("Consolidacao concluida com sucesso!")
                else:
                    st.error("A consolidacao terminou com erro.")
    except Exception as exc:
        st.error(f"Erro ao consolidar dados: {exc}")

simulation_result = st.session_state.get("consolidacao_simulation")
if simulation_result:
    st.markdown("### Simulacao (sem gravacao)")
    if simulation_result.get("ok"):
        render_executive_summary(
            "Impacto estimado",
            [
                {"label": "Registros na origem", "value": simulation_result.get("source_total", 0), "hint": "Soma das tabelas fonte", "status": "info"},
                {"label": "Consolidados atuais", "value": simulation_result.get("target_before", 0), "hint": "Antes da consolidacao real", "status": "ok"},
                {"label": "Descartes estimados", "value": simulation_result.get("estimated_discarded", 0), "hint": "Origem - consolidados atuais", "status": "warn" if simulation_result.get("estimated_discarded", 0) else "ok"},
            ],
        )
    else:
        render_alert_block(
            [f"Falha na simulacao: {simulation_result.get('error') or 'erro desconhecido'}"],
            title="Falha na simulacao",
        )

consolidacao_result = st.session_state.get("consolidacao_result")
if consolidacao_result:
    parsed = consolidacao_result["parsed"]
    insights = consolidacao_result["insights"]
    consolidado = int(insights.get("consolidados", 0) or parsed.get("consolidados", 0) or 0)
    processados = int(parsed.get("processados", 0) or consolidado)
    descartados = max(int(insights.get("fonte_total", 0)) - consolidado, 0)

    render_executive_summary(
        "Resumo executivo",
        [
            {"label": "Total processados", "value": processados, "hint": "Origem consolidada", "status": "info"},
            {"label": "Registros consolidados", "value": consolidado, "hint": "Base analitica", "status": "ok"},
            {"label": "Registros descartados", "value": descartados, "hint": "Fonte - consolidado", "status": "warn" if descartados else "ok"},
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
        alertas.append(f"Foram identificados {descartados} registros que nao entraram na base consolidada final.")
    render_alert_block(alertas, title="Riscos e inconsistencias")

    st.markdown("### Preview da base consolidada")
    preview = insights.get("preview")
    if isinstance(preview, pd.DataFrame) and not preview.empty:
        st.dataframe(preview, use_container_width=True)
    else:
        st.info("Sem dados para preview no momento.")

    st.markdown("### Log tecnico")
    render_technical_log(consolidacao_result["stdout"], consolidacao_result["stderr"], title="Ver log tecnico")

    backup_info = consolidacao_result.get("backup")
    if isinstance(backup_info, dict) and backup_info.get("ok"):
        st.markdown("### Backup de seguranca")
        st.success(f"Backup logico usado nesta execucao: {backup_info['table_name']} ({backup_info['rows']} linhas).")

    st.markdown("### Acoes")
    action = render_action_buttons(
        [
            {"label": "Ir para analises", "key": "cons_go_analises", "primary": True},
            {"label": "Nova consolidacao", "key": "cons_reset"},
        ]
    )

    csv_data = ""
    if isinstance(preview, pd.DataFrame) and not preview.empty:
        csv_data = preview.to_csv(index=False)
    st.download_button(
        "Exportar dados",
        data=csv_data.encode("utf-8"),
        file_name="preview_biota_consolidada.csv",
        mime="text/csv",
        key="download_consolidacao_preview",
    )

    if action == "cons_go_analises":
        try:
            st.switch_page("app/pages/03_Analises.py")
        except Exception:
            st.info("Nao foi possivel abrir a aba de analises automaticamente.")
    elif action == "cons_reset":
        st.session_state["consolidacao_result"] = None
        st.rerun()

