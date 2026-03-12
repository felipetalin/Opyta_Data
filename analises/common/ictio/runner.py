from __future__ import annotations

import pandas as pd

from analises.common.base import AnalysisResult, RunContext, apply_filters, safe_run
from analises.common.riqueza import run as run_riqueza
from analises.common.abundancia import run as run_abundancia


def run(ctx: RunContext, df_base: pd.DataFrame) -> list[AnalysisResult]:
    df = apply_filters(df_base, ctx)

    pipeline = [
        run_riqueza,
        run_abundancia,
    ]

    results: list[AnalysisResult] = []
    for fn in pipeline:
        results.append(safe_run(fn, ctx, df))

    return results