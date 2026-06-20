"""
Página: 05_Qualidade_Dados — Qualidade & Auditoria de Dados

Bloco 5 do PLANO_MELHORIAS_STREAMLIT.md (até então não construído).

Objetivo: dar visibilidade sobre rastreabilidade e qualidade dos dados,
consumindo o log de auditoria de operações (cadastro, importação, validação,
migração, consolidação) gravado por `core.audit.record_operation()`.

Fonte de dados: arquivos JSON em `runtime/audit/` (via `core.audit.load_audit_log`).
Não depende de nenhuma tabela no banco — funciona assim que houver operações
registradas.
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone

import streamlit as st
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.sidebar import render_sidebar
from core.audit import load_audit_log
from core.ui.design_system import (
    render_section_header,
    render_info_box,
    render_metric_card,
    render_empty_state,
)

try:
    import plotly.express as px

    _HAS_PLOTLY = True
except Exception:  # pragma: no cover - fallback se plotly indisponível
    _HAS_PLOTLY = False


# ------------------------------------------------
# Segurança
# ------------------------------------------------

if not st.session_state.get("logged_in"):
    st.switch_page("main.py")

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()

render_sidebar()

st.set_page_config(page_title="Qualidade & Auditoria | Opyta", layout="wide")

render_section_header(
    "Qualidade & Auditoria de Dados",
    icon="🔎",
    subtitle="Rastreabilidade das operações de cadastro, importação, validação, migração e consolidação",
)


# ------------------------------------------------
# Carga do log de auditoria
# ------------------------------------------------

@st.cache_data(ttl=120, show_spinner=False)
def _carregar_auditoria() -> list[dict]:
    return load_audit_log()


def _fmt_ts(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        return dt.astimezone().strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(iso)


registros = _carregar_auditoria()

col_refresh, _ = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 Atualizar", use_container_width=True):
        _carregar_auditoria.clear()
        st.rerun()

if not registros:
    render_empty_state(
        "Nenhuma operação auditada ainda",
        icon="🗂️",
        message=(
            "Os registros aparecem aqui automaticamente assim que as operações "
            "de validação/migração passarem a chamar core.audit.record_operation(). "
            "O log é gravado em runtime/audit/ (JSON, fora do versionamento)."
        ),
    )
    st.stop()


# ------------------------------------------------
# Normaliza para DataFrame
# ------------------------------------------------

def _to_rows(records: list[dict]) -> pd.DataFrame:
    rows = []
    for r in records:
        git = r.get("git") or {}
        metrics = r.get("metrics") or {}
        issues = r.get("issues") or {}
        rows.append(
            {
                "timestamp": r.get("timestamp"),
                "quando": _fmt_ts(r.get("timestamp")),
                "operação": r.get("operation"),
                "status": r.get("status"),
                "grupo": r.get("grupo") or "—",
                "projeto": r.get("projeto") or "—",
                "usuário": r.get("usuario") or "—",
                "branch": git.get("branch") or "—",
                "commit": git.get("commit") or "—",
                "arquivos": len(r.get("source_manifest") or []),
                "registros": metrics.get("registros") or metrics.get("total_registros") or "—",
                "bloqueios": issues.get("n_blocks") or issues.get("blocks") or 0,
                "avisos": issues.get("n_warnings") or issues.get("warnings") or 0,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
    return df


df = _to_rows(registros)

# ------------------------------------------------
# KPIs
# ------------------------------------------------

total = len(df)
n_ok = int((df["status"] == "ok").sum())
n_bloq = int((df["status"] == "bloqueado").sum())
n_erro = int((df["status"] == "erro").sum())
ultima = df.iloc[0]["quando"] if total else "—"

c1, c2, c3, c4 = st.columns(4)
with c1:
    render_metric_card("Operações auditadas", total)
with c2:
    render_metric_card("Concluídas (ok)", n_ok)
with c3:
    render_metric_card("Bloqueadas", n_bloq)
with c4:
    render_metric_card("Com erro", n_erro)

st.caption(f"Última operação registrada: **{ultima}**")

# Alertas
if n_erro:
    render_info_box(f"{n_erro} operação(ões) terminaram com erro — verifique o detalhe abaixo.", "error")
elif n_bloq:
    render_info_box(f"{n_bloq} operação(ões) foram bloqueadas na validação.", "warning")
else:
    render_info_box("Nenhuma operação com erro ou bloqueio registrada.", "success")

st.markdown("<br>", unsafe_allow_html=True)


# ------------------------------------------------
# Distribuição por operação / status
# ------------------------------------------------

col_a, col_b = st.columns(2)

with col_a:
    render_section_header("Operações por tipo", icon="🧩")
    by_op = df.groupby("operação").size().reset_index(name="quantidade")
    if _HAS_PLOTLY and not by_op.empty:
        fig = px.bar(by_op, x="operação", y="quantidade", color="operação", text="quantidade")
        fig.update_layout(showlegend=False, height=300, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(by_op, use_container_width=True, hide_index=True)

with col_b:
    render_section_header("Operações por status", icon="🚦")
    by_status = df.groupby("status").size().reset_index(name="quantidade")
    if _HAS_PLOTLY and not by_status.empty:
        fig = px.pie(by_status, names="status", values="quantidade", hole=0.45)
        fig.update_layout(height=300, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(by_status, use_container_width=True, hide_index=True)


# ------------------------------------------------
# Tabela detalhada (lineage)
# ------------------------------------------------

render_section_header("Histórico de operações (lineage)", icon="📜")

filtro_op = st.multiselect(
    "Filtrar por operação",
    options=sorted(df["operação"].dropna().unique().tolist()),
    default=[],
)
view = df if not filtro_op else df[df["operação"].isin(filtro_op)]

st.dataframe(
    view.drop(columns=["timestamp"]),
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "Cada linha corresponde a um registro JSON em runtime/audit/, com hash sha256 "
    "dos arquivos de origem e o commit do repositório no momento da operação."
)
