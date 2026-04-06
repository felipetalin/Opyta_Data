from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from .base import RunContext


# ==============================================================================
# ANÁLISES GERAIS (TODOS OS GRUPOS)
# ==============================================================================

def analisar_composicao_taxonomica(
    df: pd.DataFrame,
    ctx: RunContext
) -> Dict:
    """
    Tabela de composição taxonômica com filo, classe, ordem, família, gênero.
    """
    result = {
        "title": "Composição Taxonômica",
        "ok": False,
        "error": None,
        "df": None,
        "fig": None,
        "files": []
    }

    try:
        if df.empty:
            result["error"] = "DataFrame vazio"
            return result

        # Detecta colunas taxonômicas
        cols_tax = {}
        for col in ["filo", "phylum"]:
            if col in df.columns:
                cols_tax["filo"] = col
                break

        for col in ["classe", "class"]:
            if col in df.columns:
                cols_tax["classe"] = col
                break

        for col in ["ordem", "order"]:
            if col in df.columns:
                cols_tax["ordem"] = col
                break

        for col in ["familia", "family"]:
            if col in df.columns:
                cols_tax["familia"] = col
                break

        for col in ["genero", "genus"]:
            if col in df.columns:
                cols_tax["genero"] = col
                break

        if "nome_cientifico" not in df.columns:
            result["error"] = "Coluna 'nome_cientifico' não encontrada"
            return result

        # Agrupa por espécie
        agg_dict = {}
        for key, col in cols_tax.items():
            agg_dict[key] = (col, lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0])

        df_comp = df.groupby("nome_cientifico", as_index=False).agg(**agg_dict)

        # Ocorrência por campanhas
        if "nome_campanha" in df.columns:
            ocorrencia = (
                df.groupby("nome_cientifico")["nome_campanha"]
                .apply(lambda s: sorted({_rotulo_campanha(x) for x in s.dropna().unique()}))
                .reset_index(name="ocorr_lista")
            )
            ocorrencia["Ocorrência (Campanhas)"] = ocorrencia["ocorr_lista"].apply(lambda lst: " e ".join(lst))
            ocorrencia = ocorrencia.drop(columns=["ocorr_lista"])

            df_comp = df_comp.merge(ocorrencia, on="nome_cientifico", how="left")

        # Renomeia colunas
        rename_map = {
            "filo": "Filo",
            "classe": "Classe",
            "ordem": "Ordem",
            "familia": "Família",
            "genero": "Gênero",
            "nome_cientifico": "Nome Científico",
            "Ocorrência (Campanhas)": "Ocorrência (Campanhas)"
        }
        df_comp = df_comp.rename(columns=rename_map)

        # Ordenação
        cols_sort = [c for c in ["Filo", "Classe", "Ordem", "Família", "Gênero", "Nome Científico"] if c in df_comp.columns]
        if cols_sort:
            df_comp = df_comp.sort_values(cols_sort, na_position="last").reset_index(drop=True)

        result["df"] = df_comp
        result["ok"] = True

        # Export
        if ctx.exportar_arquivos:
            filename = f"01_composicao_taxonomica_{ctx.grupo.lower()}.xlsx"
            filepath = ctx.pasta_saida / filename
            df_comp.to_excel(filepath, index=False)
            result["files"].append(str(filepath))

    except Exception as e:
        result["error"] = str(e)

    return result


def analisar_riqueza_por_ponto(
    df: pd.DataFrame,
    ctx: RunContext
) -> Dict:
    """
    Riqueza de táxons por ponto amostral.
    """
    result = {
        "title": "Riqueza por Ponto",
        "ok": False,
        "error": None,
        "df": None,
        "fig": None,
        "files": []
    }

    try:
        if df.empty:
            result["error"] = "DataFrame vazio"
            return result

        required_cols = ["nome_ponto", "nome_campanha", "nome_cientifico"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            result["error"] = f"Colunas obrigatórias ausentes: {missing}"
            return result

        # Calcula riqueza
        df_riqueza = (
            df.groupby(["nome_campanha", "nome_ponto"])["nome_cientifico"]
            .nunique()
            .reset_index()
            .rename(columns={"nome_cientifico": "riqueza"})
        )

        # Ordenação
        df_riqueza["nome_campanha"] = pd.Categorical(
            df_riqueza["nome_campanha"],
            categories=ctx.ordem_campanhas,
            ordered=True
        )
        df_riqueza["nome_ponto"] = pd.Categorical(
            df_riqueza["nome_ponto"],
            categories=sorted(df_riqueza["nome_ponto"].unique()),
            ordered=True
        )
        df_riqueza = df_riqueza.sort_values(["nome_ponto", "nome_campanha"]).reset_index(drop=True)

        # Gráfico
        fig = px.bar(
            df_riqueza,
            x="nome_ponto",
            y="riqueza",
            color="nome_campanha",
            barmode="group",
            text="riqueza",
            title=f"Riqueza por Ponto - {ctx.grupo}",
            labels={"nome_ponto": "Ponto", "riqueza": "Riqueza", "nome_campanha": "Campanha"},
            color_discrete_sequence=ctx.tema.get("paleta_campanhas", ["#1f77b4", "#ff7f0e"])
        )

        fig.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(size=12),
            legend=dict(orientation="h", y=-0.2)
        )
        fig.update_traces(textposition="outside")

        result["df"] = df_riqueza
        result["fig"] = fig
        result["ok"] = True

        # Export
        if ctx.exportar_arquivos:
            # DataFrame
            filename_df = f"02_df_riqueza_por_ponto_{ctx.grupo.lower()}.xlsx"
            filepath_df = ctx.pasta_saida / filename_df
            df_riqueza.to_excel(filepath_df, index=False)
            result["files"].append(str(filepath_df))

            # Gráfico
            filename_fig = f"02_grafico_riqueza_por_ponto_{ctx.grupo.lower()}.png"
            filepath_fig = ctx.pasta_saida / filename_fig
            fig.write_image(filepath_fig, scale=2)
            result["files"].append(str(filepath_fig))

    except Exception as e:
        result["error"] = str(e)

    return result


def analisar_riqueza_por_filo(
    df: pd.DataFrame,
    ctx: RunContext
) -> Dict:
    """
    Riqueza de táxons por filo (barras + rosca).
    """
    result = {
        "title": "Riqueza por Filo",
        "ok": False,
        "error": None,
        "df": None,
        "fig": None,
        "files": []
    }

    try:
        if df.empty:
            result["error"] = "DataFrame vazio"
            return result

        # Detecta coluna de filo
        col_filo = None
        for c in ["filo", "phylum"]:
            if c in df.columns:
                col_filo = c
                break

        if not col_filo or df[col_filo].dropna().empty:
            result["error"] = "Coluna de filo não encontrada ou vazia"
            return result

        # Calcula riqueza por filo
        df_filo = (
            df.groupby(col_filo)["nome_cientifico"]
            .nunique()
            .reset_index()
            .rename(columns={col_filo: "filo", "nome_cientifico": "numero_de_taxons"})
            .sort_values("numero_de_taxons", ascending=False)
            .reset_index(drop=True)
        )

        # Gráfico de barras
        fig_bar = px.bar(
            df_filo,
            x="filo",
            y="numero_de_taxons",
            text="numero_de_taxons",
            title=f"Riqueza por Filo - {ctx.grupo}",
            color_discrete_sequence=["#1f77b4"]
        )
        fig_bar.update_traces(textposition="outside")

        result["df"] = df_filo
        result["fig"] = fig_bar
        result["ok"] = True

        # Export
        if ctx.exportar_arquivos:
            filename_df = f"03_df_riqueza_por_filo_{ctx.grupo.lower()}.xlsx"
            filepath_df = ctx.pasta_saida / filename_df
            df_filo.to_excel(filepath_df, index=False)
            result["files"].append(str(filepath_df))

            filename_fig = f"03_grafico_riqueza_por_filo_{ctx.grupo.lower()}.png"
            filepath_fig = ctx.pasta_saida / filename_fig
            fig_bar.write_image(filepath_fig, scale=2)
            result["files"].append(str(filepath_fig))

    except Exception as e:
        result["error"] = str(e)

    return result


def analisar_diversidade_alfa(
    df: pd.DataFrame,
    ctx: RunContext
) -> Dict:
    """
    Diversidade alfa (Shannon H' + Pielou J').
    """
    result = {
        "title": "Diversidade Alfa",
        "ok": False,
        "error": None,
        "df": None,
        "fig": None,
        "files": []
    }

    try:
        if df.empty:
            result["error"] = "DataFrame vazio"
            return result

        required_cols = ["nome_ponto", "nome_campanha", "nome_cientifico", "contagem"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            result["error"] = f"Colunas obrigatórias ausentes: {missing}"
            return result

        # Filtra dados quantitativos
        df_quant = df[
            df["tipo_amostragem"].astype(str).str.contains("Quantit", case=False, na=False)
        ].copy()

        if df_quant.empty:
            result["error"] = "Nenhum dado quantitativo encontrado"
            return result

        df_quant["contagem"] = pd.to_numeric(df_quant["contagem"], errors="coerce").fillna(0)

        # Calcula diversidade por ponto
        resultados = []
        campanhas = df_quant["nome_campanha"].unique()

        for campanha in campanhas:
            df_c = df_quant[df_quant["nome_campanha"] == campanha]

            if df_c.empty:
                continue

            matriz = df_c.pivot_table(
                index="nome_ponto",
                columns="nome_cientifico",
                values="contagem",
                aggfunc="sum",
                fill_value=0
            )

            for ponto in matriz.index:
                counts = matriz.loc[ponto].values
                shannon = _calcular_shannon(counts)
                pielou = _calcular_pielou(counts)

                resultados.append({
                    "campanha": campanha,
                    "ponto": ponto,
                    "shannon_h": shannon,
                    "pielou_j": pielou
                })

        df_div = pd.DataFrame(resultados)

        # Gráfico
        fig = go.Figure()

        # Barras Shannon
        fig.add_trace(go.Bar(
            name="Shannon (H')",
            x=df_div["ponto"],
            y=df_div["shannon_h"],
            marker_color="#0077b6",
            offsetgroup=0
        ))

        # Pontos Pielou
        fig.add_trace(go.Scatter(
            name="Pielou (J')",
            x=df_div["ponto"],
            y=df_div["pielou_j"],
            mode="markers",
            marker=dict(color="#00b4d8", size=8),
            yaxis="y2"
        ))

        fig.update_layout(
            title=f"Diversidade Alfa - {ctx.grupo}",
            xaxis=dict(title="Ponto"),
            yaxis=dict(title="Shannon (H')", side="left"),
            yaxis2=dict(title="Pielou (J')", side="right", overlaying="y", range=[0, 1.1]),
            plot_bgcolor="white",
            paper_bgcolor="white"
        )

        result["df"] = df_div
        result["fig"] = fig
        result["ok"] = True

        # Export
        if ctx.exportar_arquivos:
            filename_df = f"04_df_diversidade_alfa_{ctx.grupo.lower()}.xlsx"
            filepath_df = ctx.pasta_saida / filename_df
            df_div.to_excel(filepath_df, index=False)
            result["files"].append(str(filepath_df))

            filename_fig = f"04_grafico_diversidade_alfa_{ctx.grupo.lower()}.png"
            filepath_fig = ctx.pasta_saida / filename_fig
            fig.write_image(filepath_fig, scale=2)
            result["files"].append(str(filepath_fig))

    except Exception as e:
        result["error"] = str(e)

    return result


# ==============================================================================
# ANÁLISES ESPECÍFICAS POR GRUPO
# ==============================================================================

def analisar_bmwp_zoobentos(
    df: pd.DataFrame,
    ctx: RunContext
) -> Dict:
    """
    Análise BMWP específica para zoobentos.
    """
    result = {
        "title": "Índice BMWP (Zoobentos)",
        "ok": False,
        "error": None,
        "df": None,
        "fig": None,
        "files": []
    }

    try:
        if df.empty:
            result["error"] = "DataFrame vazio"
            return result

        # Tabela BMWP simplificada (exemplo)
        bmwp_scores = {
            "família_a": 10,  # Exemplo - substituir por tabela real
            "família_b": 8,
            "família_c": 6,
            # ... adicionar famílias reais
        }

        # Lógica BMWP aqui
        # Por enquanto, placeholder
        df_bmwp = pd.DataFrame({
            "ponto": ["P1", "P2", "P3"],
            "bmwp_score": [25, 18, 32],
            "classificacao": ["Excelente", "Bom", "Excelente"]
        })

        result["df"] = df_bmwp
        result["ok"] = True

        # Placeholder para gráfico
        fig = px.bar(
            df_bmwp,
            x="ponto",
            y="bmwp_score",
            title="Índice BMWP por Ponto",
            color_discrete_sequence=["#2ca02c"]
        )

        result["fig"] = fig

    except Exception as e:
        result["error"] = str(e)

    return result


# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def _rotulo_campanha(campanha: str) -> str:
    """Converte nome completo de campanha para rótulo curto."""
    mapa = {
        "1º Campanha (Seca)": "C1",
        "2º Campanha (Chuva)": "C2",
    }
    return mapa.get(str(campanha).strip(), str(campanha).strip())


def _calcular_shannon(counts: np.ndarray) -> float:
    """Calcula índice de Shannon."""
    counts = np.array(counts, dtype=float)
    counts = counts[counts > 0]

    if len(counts) == 0:
        return 0.0

    proporcoes = counts / counts.sum()
    return -np.sum(proporcoes * np.log(proporcoes))


def _calcular_pielou(counts: np.ndarray) -> float:
    """Calcula equitabilidade de Pielou."""
    counts = np.array(counts, dtype=float)
    counts = counts[counts > 0]

    if len(counts) <= 1:
        return 0.0

    H = _calcular_shannon(counts)
    S = len(counts)

    return H / np.log(S)