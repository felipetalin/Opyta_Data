from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import pandas as pd
import plotly.express as px

from analises.common.base import AnalysisResult, RunContext
from analises.common.export import export_df_xlsx, export_plotly_png
from analises.common.theme import get_theme, ordem_campanhas_padrao


@dataclass(frozen=True)
class AbundanciaConfig:
    ordem_campanhas: Optional[Sequence[str]] = None
    col_campanha: str = "nome_campanha"
    col_ponto: str = "nome_ponto"
    col_contagem: str = "contagem"
    col_tipo_amostragem: str = "tipo_amostragem"
    apenas_quantitativa: bool = True


def calcular_abundancia_por_ponto(df: pd.DataFrame, cfg: AbundanciaConfig) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[cfg.col_campanha, cfg.col_ponto, "abundancia_total"])

    for col in (cfg.col_campanha, cfg.col_ponto, cfg.col_contagem):
        if col not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {col}")

    df2 = df.copy()

    if cfg.apenas_quantitativa and cfg.col_tipo_amostragem in df2.columns:
        df2 = df2[df2[cfg.col_tipo_amostragem].astype(str).str.contains("Quantit", case=False, na=False)].copy()

    if df2.empty:
        return pd.DataFrame(columns=[cfg.col_campanha, cfg.col_ponto, "abundancia_total"])

    df2[cfg.col_contagem] = pd.to_numeric(df2[cfg.col_contagem], errors="coerce").fillna(0)

    out = (
        df2.groupby([cfg.col_campanha, cfg.col_ponto], dropna=False)[cfg.col_contagem]
        .sum()
        .reset_index()
        .rename(columns={cfg.col_contagem: "abundancia_total"})
    )

    out[cfg.col_campanha] = out[cfg.col_campanha].astype(str).str.strip()
    out[cfg.col_ponto] = out[cfg.col_ponto].astype(str).str.strip()

    if cfg.ordem_campanhas:
        out[cfg.col_campanha] = pd.Categorical(out[cfg.col_campanha], categories=list(cfg.ordem_campanhas), ordered=True)

    pontos_ordem = sorted(out[cfg.col_ponto].dropna().unique())
    out[cfg.col_ponto] = pd.Categorical(out[cfg.col_ponto], categories=pontos_ordem, ordered=True)

    out = out.sort_values([cfg.col_ponto, cfg.col_campanha]).reset_index(drop=True)
    return out


def run(ctx: RunContext, df: pd.DataFrame) -> AnalysisResult:
    theme = get_theme(ctx.tema)
    ordem = ordem_campanhas_padrao(ctx.grupo)

    cfg = AbundanciaConfig(ordem_campanhas=ordem)
    df_out = calcular_abundancia_por_ponto(df, cfg)

    res = AnalysisResult(
        key="abundancia_por_ponto",
        title="02) Abundância por ponto",
        df=df_out,
    )

    try:
        fig = px.bar(
            df_out,
            x="nome_ponto",
            y="abundancia_total",
            color="nome_campanha",
            barmode="group",
            text="abundancia_total",
            title=f"Abundância por ponto — {ctx.grupo}",
            color_discrete_sequence=theme.paleta_padrao,
            labels={
                "nome_ponto": "Ponto Amostral",
                "abundancia_total": "Abundância Total (Nº de Indivíduos)",
                "nome_campanha": "Campanha",
            },
        )
        fig.update_traces(textposition="outside", cliponaxis=False)
        fig.update_layout(margin=dict(t=70, b=120, l=70, r=20))
        res.fig = fig
    except Exception as e:
        res.warnings.append(f"Falha ao gerar gráfico (plotly): {e}")

    if ctx.exportar_arquivos:
        try:
            res.files.append(export_df_xlsx(df_out, ctx.pasta_saida, f"02_abundancia_por_ponto_{ctx.grupo.lower()}.xlsx"))
        except Exception as e:
            res.warnings.append(f"Falha ao exportar XLSX: {e}")

        if res.fig is not None:
            try:
                res.files.append(export_plotly_png(res.fig, ctx.pasta_saida, f"02_abundancia_por_ponto_{ctx.grupo.lower()}.png"))
            except Exception as e:
                res.warnings.append(f"Falha ao exportar PNG (kaleido): {e}")

    return res