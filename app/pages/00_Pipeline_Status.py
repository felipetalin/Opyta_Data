"""
Página: 00_Pipeline_Status - Dashboard de Status Operacional

Objetivo: Visão executiva do pipeline de importação/consolidação.
Mostra: Últimas operações, status, alertas e insights operacionais.
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.sidebar import render_sidebar
from core.engine import get_engine
from core.ui.design_system import (
    render_section_header,
    render_info_box,
    render_metric_card,
)

# ------------------------------------------------
# Segurança
# ------------------------------------------------

if not st.session_state.get("logged_in"):
    st.switch_page("main.py")

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()

render_sidebar()

# ------------------------------------------------
# Página
# ------------------------------------------------

st.set_page_config(page_title="Pipeline Status | Opyta", layout="wide")

render_section_header(
    "Status Operacional do Pipeline",
    icon="⚙️",
    subtitle="Monitoramento de importação, validação, consolidação e migrações"
)

# ------------------------------------------------
# Queries de Status
# ------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)  # Cache 5 min
def get_pipeline_health() -> dict:
    """Retorna saúde geral do pipeline."""
    out = {
        "ultimas_24h": 0,
        "ultimas_7d": 0,
        "status_ok": 0,
        "status_erro": 0,
        "status_pendente": 0,
        "ultimo_exec": None,
        "db_ok": False,
        "erro": None,
    }
    
    engine = None
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Últimas 24h
            res_24h = conn.execute(
                text("""
                    SELECT COUNT(*) as cnt
                    FROM import_logs
                    WHERE data_criacao >= NOW() - INTERVAL '24 hours'
                """)
            )
            out["ultimas_24h"] = int(res_24h.fetchone()[0] if res_24h.fetchone() else 0)
            
            # Últimas 7d
            res_7d = conn.execute(
                text("""
                    SELECT COUNT(*) as cnt
                    FROM import_logs
                    WHERE data_criacao >= NOW() - INTERVAL '7 days'
                """)
            )
            out["ultimas_7d"] = int(res_7d.fetchone()[0] if res_7d.fetchone() else 0)
            
            # Status counts
            res_status = conn.execute(
                text("""
                    SELECT 
                        status,
                        COUNT(*) as cnt
                    FROM import_logs
                    WHERE data_criacao >= NOW() - INTERVAL '7 days'
                    GROUP BY status
                """)
            )
            for status, cnt in res_status.fetchall():
                if status == "success":
                    out["status_ok"] = int(cnt)
                elif status == "error":
                    out["status_erro"] = int(cnt)
                elif status == "pending":
                    out["status_pendente"] = int(cnt)
            
            # Última execução
            res_last = conn.execute(
                text("""
                    SELECT data_criacao
                    FROM import_logs
                    ORDER BY data_criacao DESC
                    LIMIT 1
                """)
            )
            last_row = res_last.fetchone()
            if last_row:
                out["ultimo_exec"] = last_row[0]
            
            out["db_ok"] = True
    except Exception as e:
        out["erro"] = str(e)
    finally:
        if engine:
            engine.dispose()
    
    return out


@st.cache_data(ttl=300, show_spinner=False)
def get_recent_logs(limit: int = 10) -> pd.DataFrame:
    """Retorna últimas operações."""
    engine = None
    try:
        engine = get_engine()
        query = text("""
            SELECT 
                id,
                projeto,
                grupo_biologico,
                etapa,
                status,
                mensagem,
                data_criacao
            FROM import_logs
            ORDER BY data_criacao DESC
            LIMIT :limit
        """)
        df = pd.read_sql(query, engine, params={"limit": limit})
        return df
    except Exception as e:
        st.error(f"Erro ao carregar logs: {e}")
        return pd.DataFrame()
    finally:
        if engine:
            engine.dispose()


@st.cache_data(ttl=300, show_spinner=False)
def get_status_timeline() -> pd.DataFrame:
    """Retorna timeline de status para Plotly."""
    engine = None
    try:
        engine = get_engine()
        query = text("""
            SELECT 
                DATE(data_criacao) as data,
                etapa,
                status,
                COUNT(*) as cnt
            FROM import_logs
            WHERE data_criacao >= NOW() - INTERVAL '30 days'
            GROUP BY DATE(data_criacao), etapa, status
            ORDER BY data
        """)
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar timeline: {e}")
        return pd.DataFrame()
    finally:
        if engine:
            engine.dispose()


# ------------------------------------------------
# Renderização
# ------------------------------------------------

health = get_pipeline_health()

# Resumo de saúde
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    render_metric_card("Últimas 24h", health["ultimas_24h"])
with col2:
    render_metric_card("Últimas 7 dias", health["ultimas_7d"])
with col3:
    render_metric_card("✅ OK", health["status_ok"])
with col4:
    render_metric_card("❌ Erro", health["status_erro"])
with col5:
    if health["ultimo_exec"]:
        tempo_decorrido = datetime.now() - health["ultimo_exec"]
        if tempo_decorrido.total_seconds() < 3600:
            valor = f"{int(tempo_decorrido.total_seconds() // 60)}min"
        elif tempo_decorrido.total_seconds() < 86400:
            valor = f"{int(tempo_decorrido.total_seconds() // 3600)}h"
        else:
            valor = f"{int(tempo_decorrido.days)}d"
    else:
        valor = "—"
    render_metric_card("Última exec", valor)

st.divider()

# Status de conexão com BD
if health["db_ok"]:
    render_info_box("✅ Banco de dados conectado e operacional", box_type="success")
else:
    render_info_box(f"❌ Erro ao conectar banco: {health['erro']}", box_type="error")

st.divider()

# Timeline de operações
render_section_header("Timeline de Operações (Últimas 30 Dias)", icon="📅")

timeline_df = get_status_timeline()
if not timeline_df.empty:
    # Agregar por status
    status_colors = {
        "success": "#10b981",
        "error": "#ef4444",
        "pending": "#f59e0b",
    }
    
    fig = go.Figure()
    
    for status in ["success", "error", "pending"]:
        status_data = timeline_df[timeline_df["status"] == status]
        if not status_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=status_data["data"],
                    y=status_data["cnt"],
                    mode="lines+markers",
                    name=f"Status: {status.upper()}",
                    line=dict(color=status_colors.get(status, "#666"), width=2),
                    hovertemplate="<b>%{x}</b><br>Qtd: %{y}<extra></extra>",
                )
            )
    
    fig.update_layout(
        title="Volume de Operações por Data e Status",
        xaxis_title="Data",
        yaxis_title="Quantidade",
        hovermode="x unified",
        height=400,
        margin=dict(l=50, r=20, t=40, b=50),
    )
    
    st.plotly_chart(fig, use_container_width=True, key="timeline_status")
else:
    st.info("Sem dados de operações nos últimos 30 dias")

st.divider()

# Tabela de últimas operações
render_section_header("Últimas Operações", icon="📋")

recent_logs = get_recent_logs(limit=15)
if not recent_logs.empty:
    # Formatar dados
    recent_logs["data_criacao"] = pd.to_datetime(recent_logs["data_criacao"]).dt.strftime("%d/%m %H:%M")
    
    # Colorir status
    def status_color(status):
        if status == "success":
            return "✅"
        elif status == "error":
            return "❌"
        else:
            return "⏳"
    
    recent_logs["status_icon"] = recent_logs["status"].apply(status_color)
    
    # Exibir tabela
    display_cols = ["status_icon", "data_criacao", "projeto", "grupo_biologico", "etapa", "status"]
    st.dataframe(
        recent_logs[display_cols].rename(columns={
            "status_icon": "Status",
            "data_criacao": "Data/Hora",
            "projeto": "Projeto",
            "grupo_biologico": "Grupo Biológico",
            "etapa": "Etapa",
            "status": "Resultado",
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Status": st.column_config.TextColumn(width="80px"),
            "Data/Hora": st.column_config.TextColumn(width="120px"),
            "Etapa": st.column_config.TextColumn(width="120px"),
            "Resultado": st.column_config.TextColumn(width="100px"),
        },
    )
    
    # Mostrar alertas se houver erros
    errors = recent_logs[recent_logs["status"] == "error"]
    if not errors.empty:
        st.divider()
        render_section_header("⚠️ Alertas - Operações com Erro", icon="🚨")
        for _, row in errors.head(5).iterrows():
            render_info_box(
                f"**{row['projeto']}** - {row['grupo_biologico']}: {row.get('mensagem', 'Sem detalhes')}",
                box_type="error",
            )
else:
    st.info("Sem operações registradas ainda")

# Footer
st.divider()
st.caption("🔄 Dashboard atualizado a cada 5 minutos. Clique em 'Rerun' para força atualização imediata.")
