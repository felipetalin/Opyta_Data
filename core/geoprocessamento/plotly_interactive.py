"""
Componentes Interativos com Plotly para Geoambiental

Objetivo: Adicionar dinâmica e interatividade sem re-executar a página Streamlit.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st


def render_scatter_interativo(
    df: pd.DataFrame,
    modo: str,
    indicador: str,
    title: str | None = None,
) -> None:
    """
    Renderiza scatter plot interativo com marcadores coloridos por indicador.
    
    Args:
        df: DataFrame com colunas latitude, longitude e indicador
        modo: "Biota" ou "Fisico"
        indicador: Coluna para colorir marcadores
        title: Título customizado (opcional)
    
    Exemplo:
        render_scatter_interativo(df_geo, "Biota", "shannon", "Diversidade por Ponto")
    """
    if df.empty:
        st.info("Sem dados para exibir scatter plot")
        return
    
    # Garantir valores numéricos
    df_plot = df.copy()
    df_plot["latitude"] = pd.to_numeric(df_plot["latitude"], errors="coerce")
    df_plot["longitude"] = pd.to_numeric(df_plot["longitude"], errors="coerce")
    df_plot[indicador] = pd.to_numeric(df_plot[indicador], errors="coerce")
    df_plot = df_plot.dropna(subset=["latitude", "longitude", indicador])
    
    if df_plot.empty:
        st.warning(f"Sem dados válidos para indicador '{indicador}'")
        return
    
    # Texto hover detalhado
    hover_text = []
    for _, row in df_plot.iterrows():
        text = (
            f"<b>Projeto:</b> {row.get('projeto', '-')}<br>"
            f"<b>Ponto:</b> {row.get('ponto', '-')}<br>"
            f"<b>Campanha:</b> {row.get('campanha', '-')}<br>"
            f"<b>{indicador}:</b> {row[indicador]:.2f}"
        )
        if modo == "Biota" and "riqueza" in row.index:
            text += f"<br><b>Riqueza:</b> {row.get('riqueza', 0):.1f}"
        if modo == "Biota" and "shannon" in row.index:
            text += f"<br><b>Diversidade:</b> {row.get('shannon', 0):.2f}"
        hover_text.append(text)
    
    fig = go.Figure()
    
    fig.add_trace(
        go.Scattergeo(
            lat=df_plot["latitude"],
            lon=df_plot["longitude"],
            mode="markers",
            marker=dict(
                size=df_plot[indicador].apply(lambda x: 8 + (x - df_plot[indicador].min()) / (df_plot[indicador].max() - df_plot[indicador].min()) * 12 if df_plot[indicador].max() > df_plot[indicador].min() else 8),
                color=df_plot[indicador],
                colorscale="RdYlGn",
                showscale=True,
                colorbar=dict(title=indicador, thickness=15, len=0.7),
                opacity=0.7,
                line=dict(width=1, color="#333"),
            ),
            hovertemplate="<b>Localização</b><br>Lat: %{lat:.4f}<br>Lon: %{lon:.4f}<br><extra></extra>",
            text=hover_text,
            customdata=hover_text,
            hovertemplate="%{customdata}<extra></extra>",
        )
    )
    
    fig.update_layout(
        title=title or f"Distribuição Espacial - {indicador}",
        hovermode="closest",
        geo=dict(
            scope="south america",
            projection_type="mercator",
            showland=True,
            landcolor="rgb(243, 243, 243)",
        ),
        height=500,
        margin=dict(l=0, r=0, t=40, b=0),
    )
    
    st.plotly_chart(fig, use_container_width=True, key=f"scatter_{indicador}")


def render_serie_temporal_por_ponto(
    df: pd.DataFrame,
    modo: str,
    indicador: str,
    ponto: str | None = None,
) -> None:
    """
    Renderiza série temporal de um indicador agrupado por campanha/data.
    Permite seleção de ponto específico ou média geral.
    
    Args:
        df: DataFrame com colunas campanha, ponto, indicador
        modo: "Biota" ou "Fisico"
        indicador: Coluna para plotar
        ponto: Ponto específico (opcional, se None usa todos)
    """
    if df.empty:
        st.info("Sem dados para série temporal")
        return
    
    df_plot = df.copy()
    df_plot[indicador] = pd.to_numeric(df_plot[indicador], errors="coerce")
    df_plot = df_plot.dropna(subset=["campanha", indicador])
    
    if df_plot.empty:
        st.warning(f"Sem dados válidos para série temporal do indicador '{indicador}'")
        return
    
    # Se ponto específico selecionado
    if ponto and ponto != "Todos":
        df_plot = df_plot[df_plot["ponto"] == ponto]
        if df_plot.empty:
            st.warning(f"Sem dados para ponto '{ponto}'")
            return
        title_prefix = f"Ponto: {ponto}"
    else:
        # Média por campanha (todos os pontos)
        df_plot = df_plot.groupby("campanha", as_index=False)[indicador].agg(["mean", "min", "max", "count"])
        df_plot.columns = ["campanha", "media", "minimo", "maximo", "n_pontos"]
        title_prefix = "Média Geral"
    
    fig = go.Figure()
    
    if ponto and ponto != "Todos":
        # Se ponto específico, uma linha só
        fig.add_trace(
            go.Scatter(
                x=df_plot["campanha"],
                y=df_plot[indicador],
                mode="lines+markers",
                name=ponto,
                hovertemplate="<b>%{x}</b><br>" + f"{indicador}: %{{y:.2f}}<extra></extra>",
            )
        )
    else:
        # Se agregado, mostrar média com banda min/max
        fig.add_trace(
            go.Scatter(
                x=df_plot["campanha"],
                y=df_plot["maximo"],
                fill=None,
                mode="lines",
                line_color="rgba(0,100,200,0)",
                showlegend=False,
                name="Max",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_plot["campanha"],
                y=df_plot["minimo"],
                fill="tonexty",
                mode="lines",
                line_color="rgba(0,100,200,0)",
                fillcolor="rgba(0,100,200,0.1)",
                showlegend=False,
                name="Min",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_plot["campanha"],
                y=df_plot["media"],
                mode="lines+markers",
                name="Média",
                hovertemplate="<b>%{x}</b><br>" + f"{indicador}: %{{y:.2f}}<extra></extra>",
            )
        )
    
    fig.update_layout(
        title=f"{title_prefix} - {indicador} ao longo do tempo",
        hovermode="x unified",
        xaxis_title="Campanha",
        yaxis_title=f"{indicador} ({getattr(df.get(f'{indicador}_unit', 'unidade'))}" + (")" if df.get(f"{indicador}_unit") else ""),
        height=400,
        margin=dict(l=50, r=20, t=40, b=50),
    )
    
    st.plotly_chart(fig, use_container_width=True, key=f"temporal_{indicador}_{ponto}")


def render_distribuicao_indicador(
    df: pd.DataFrame,
    modo: str,
    indicador: str,
) -> None:
    """
    Renderiza histograma/box plot da distribuição de um indicador.
    
    Args:
        df: DataFrame com coluna indicador
        modo: "Biota" ou "Fisico"
        indicador: Coluna para analisar
    """
    if df.empty:
        st.info("Sem dados para distribuição")
        return
    
    df_plot = df.copy()
    df_plot[indicador] = pd.to_numeric(df_plot[indicador], errors="coerce")
    df_plot = df_plot.dropna(subset=[indicador])
    
    if df_plot.empty:
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Histograma
        fig_hist = go.Figure()
        fig_hist.add_trace(
            go.Histogram(
                x=df_plot[indicador],
                nbinsx=20,
                name=indicador,
                hovertemplate="<b>Intervalo:</b> %{x}<br><b>Frequência:</b> %{y}<extra></extra>",
            )
        )
        fig_hist.update_layout(
            title=f"Distribuição - {indicador}",
            xaxis_title=indicador,
            yaxis_title="Frequência",
            height=350,
            showlegend=False,
            margin=dict(l=40, r=20, t=40, b=40),
        )
        st.plotly_chart(fig_hist, use_container_width=True, key=f"hist_{indicador}")
    
    with col2:
        # Box plot por grupo (se Biota, por grupo_biologico)
        if modo == "Biota" and "grupo_biologico" in df_plot.columns:
            grupo_col = "grupo_biologico"
        elif "projeto" in df_plot.columns:
            grupo_col = "projeto"
        else:
            grupo_col = None
        
        if grupo_col:
            fig_box = px.box(
                df_plot,
                y=indicador,
                x=grupo_col,
                title=f"Distribuição por {grupo_col}",
                height=350,
            )
            fig_box.update_layout(
                xaxis_title=grupo_col,
                yaxis_title=indicador,
                margin=dict(l=40, r=20, t=40, b=100),
            )
            st.plotly_chart(fig_box, use_container_width=True, key=f"box_{indicador}_{grupo_col}")


def render_comparacao_indicadores(
    df: pd.DataFrame,
    indicador_x: str,
    indicador_y: str,
    modo: str,
) -> None:
    """
    Renderiza scatter plot comparando 2 indicadores.
    
    Args:
        df: DataFrame com ambos indicadores
        indicador_x: Eixo X
        indicador_y: Eixo Y
        modo: "Biota" ou "Fisico"
    """
    if df.empty or indicador_x not in df.columns or indicador_y not in df.columns:
        st.warning(f"Indicadores '{indicador_x}' ou '{indicador_y}' não encontrados")
        return
    
    df_plot = df.copy()
    df_plot[indicador_x] = pd.to_numeric(df_plot[indicador_x], errors="coerce")
    df_plot[indicador_y] = pd.to_numeric(df_plot[indicador_y], errors="coerce")
    df_plot = df_plot.dropna(subset=[indicador_x, indicador_y])
    
    if df_plot.empty:
        return
    
    fig = go.Figure()
    
    # Colorir por projeto para visual interessante
    if "projeto" in df_plot.columns:
        for projeto in df_plot["projeto"].unique():
            mask = df_plot["projeto"] == projeto
            fig.add_trace(
                go.Scatter(
                    x=df_plot[mask][indicador_x],
                    y=df_plot[mask][indicador_y],
                    mode="markers",
                    name=projeto,
                    marker=dict(size=8, opacity=0.7),
                    hovertemplate=f"<b>{projeto}</b><br>{indicador_x}: %{{x:.2f}}<br>{indicador_y}: %{{y:.2f}}<extra></extra>",
                )
            )
    
    fig.update_layout(
        title=f"Comparação: {indicador_x} vs {indicador_y}",
        xaxis_title=indicador_x,
        yaxis_title=indicador_y,
        hovermode="closest",
        height=400,
        margin=dict(l=50, r=20, t=40, b=50),
    )
    
    st.plotly_chart(fig, use_container_width=True, key=f"compare_{indicador_x}_{indicador_y}")
