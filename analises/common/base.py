from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

import pandas as pd


@dataclass(frozen=True)
class RunContext:
    projeto: str
    grupo: str
    campanha: str                 # "(todas)" ou nome exato
    ponto: Optional[str]          # None ou nome_ponto exato
    pasta_saida: Path
    tema: str                     # "cliente_verde", "cliente_azul", "neutro"
    ordem_campanhas: Optional[Sequence[str]] = None
    exportar_arquivos: bool = False
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass
class AnalysisResult:
    key: str
    title: str
    ok: bool = True
    error: Optional[str] = None
    df: Optional[pd.DataFrame] = None
    fig: Any = None
    files: list[Path] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def apply_filters(df: pd.DataFrame, ctx: RunContext) -> pd.DataFrame:
    """Aplica filtros padrão do app: campanha (nome_campanha) e ponto (nome_ponto)."""
    if df is None or df.empty:
        return df

    out = df.copy()

    # campanha
    if ctx.campanha and ctx.campanha != "(todas)" and "nome_campanha" in out.columns:
        out = out[out["nome_campanha"].astype(str).str.strip() == str(ctx.campanha).strip()]

    # ponto (NOME CORRETO = nome_ponto)
    if ctx.ponto and "nome_ponto" in out.columns:
        out = out[out["nome_ponto"].astype(str).str.strip() == str(ctx.ponto).strip()]

    return out


def safe_run(fn: Callable[[RunContext, pd.DataFrame], AnalysisResult], ctx: RunContext, df: pd.DataFrame) -> AnalysisResult:
    """Executa um bloco e garante que um erro não derrube os outros."""
    try:
        return fn(ctx, df)
    except Exception as e:
        title = getattr(fn, "__name__", "analise")
        return AnalysisResult(key=title, title=title, ok=False, error=str(e))