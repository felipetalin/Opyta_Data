from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def export_df_xlsx(df: pd.DataFrame, pasta: Path, nome: str) -> Path:
    _mkdir(pasta)
    path = pasta / nome
    df.to_excel(path, index=False, engine="openpyxl")
    return path


def export_dfs_xlsx(dfs: dict[str, pd.DataFrame], pasta: Path, nome: str) -> Path:
    _mkdir(pasta)
    path = pasta / nome
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for aba, df in dfs.items():
            df.to_excel(writer, sheet_name=aba, index=False)
    return path


def export_plotly_png(fig: Any, pasta: Path, nome: str) -> Path:
    _mkdir(pasta)
    path = pasta / nome
    fig.write_image(str(path), scale=2)  # requer kaleido
    return path