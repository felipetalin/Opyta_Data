from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import pandas as pd

from .base import AnalysisResult, RunContext
from .export import export_df_xlsx, export_plotly_png
from .theme import get_theme, ordem_campanhas_padrao


@dataclass(frozen=True)
class RiquezaConfig:
    ordem_campanhas: Optional[Sequence[str]] = None
    col_campanha: str = "nome_campanha"
    col_ponto: str = "nome_ponto"
    col_taxon: str = "nome_cientifico"


def calcular_riqueza_por_ponto(df: pd.DataFrame, cfg: RiquezaConfig) -> pd.DataFrame:
    """
    Retorna DataFrame com:
      nome_campanha | nome_ponto | riqueza
    """
    # DF vazio -> devolve estrutura esperada
    if df is None or df.empty:
        return pd.DataFrame(columns=[cfg.col_campanha, cfg.col_ponto, "riqueza"])

    # valida colunas
    for col in (cfg.col_campanha, cfg.col_ponto, cfg.col_taxon):
        if col not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {col}")

    out = (
        df.groupby([cfg.col_campanha, cfg.col_ponto])[cfg.col_taxon]
        .nunique()
        .reset_index()
        .rename(columns={cfg.col_taxon: "riqueza"})
    )

    # limpeza
    out[cfg.col_campanha] = out[cfg.col_campanha].astype(str).str.strip()
    out[cfg.col_ponto] = out[cfg.col_ponto].astype(str).str.strip()

    # ordem campanhas
    if cfg.ordem_campanhas:
        out[cfg.col_campanha] = pd.Categorical(
            out[cfg.col_campanha],
            categories=list(cfg.ordem_campanhas),
            ordered=True,
        )

    # ordem pontos
    pontos_ordem = sorted(out[cfg.col_ponto].dropna().unique())
    out[cfg.col_ponto] = pd.Categorical(out[cfg.col_ponto], categories=pontos_ordem, ordered=True)

    out = out.sort_values([cfg.col_ponto, cfg.col_campanha]).reset_index(drop=True)
    return out


def run(ctx: RunContext, df: pd.DataFrame) -> AnalysisResult:
    """
    Executa a análise de riqueza por ponto.
    Retorna AnalysisResult com df + fig + exports.
    """
    theme = get_theme(ctx.tema)
    ordem = ordem_campanhas_padrao(ctx.grupo)

    cfg = RiquezaConfig(ordem_campanhas=ordem)
    df_out = calcular_riqueza_por_ponto(df, cfg)

    res = AnalysisResult(
        key="riqueza_por_ponto",
        title="01) Riqueza por ponto",
        df=df_out,
    )

    # figura (plotly)
    try:
        import plotly.express as px

        fig = px.bar(
            df_out,
            x="nome_ponto",
            y="riqueza",
            color="nome_campanha",
            barmode="group",
            text="riqueza",
            title=f"Riqueza por ponto — {ctx.grupo}",
            color_discrete_sequence=theme.paleta_padrao,
        )
        fig.update_traces(textposition="outside", cliponaxis=False)
        fig.update_layout(margin=dict(t=70, b=120, l=60, r=20))
        res.fig = fig

    except Exception as e:
        res.warnings.append(f"Plotly indisponível/erro: {e}")

    # exports
    if ctx.exportar_arquivos:
        try:
            res.files.append(
                export_df_xlsx(
                    df_out,
                    ctx.pasta_saida,
                    f"01_riqueza_por_ponto_{ctx.grupo.lower()}.xlsx",
                )
            )
        except Exception as e:
            res.warnings.append(f"Falha ao exportar XLSX: {e}")

        if res.fig is not None:
            try:
                res.files.append(
                    export_plotly_png(
                        res.fig,
                        ctx.pasta_saida,
                        f"01_riqueza_por_ponto_{ctx.grupo.lower()}.png",
                    )
                )
            except Exception as e:
                res.warnings.append(f"Falha ao exportar PNG (kaleido): {e}")

    return res