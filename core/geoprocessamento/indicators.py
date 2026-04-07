from __future__ import annotations

import math
import unicodedata

import pandas as pd


GROUP_KEYS = [
    "projeto",
    "campanha",
    "ponto",
    "latitude",
    "longitude",
    "grupo_biologico",
]

EPT_ORDERS = {"ephemeroptera", "plecoptera", "trichoptera"}


def _norm_text(value: object) -> str:
    s = str(value or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _safe_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series([default] * len(df), index=df.index)
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def _shannon_from_group(group: pd.DataFrame) -> float:
    taxon_col = "nome_cientifico" if "nome_cientifico" in group.columns else None
    if not taxon_col:
        return 0.0

    contagem = _safe_series(group, "contagem", default=0.0)
    if contagem.sum() <= 0:
        abund_por_taxon = group.groupby(taxon_col).size().astype(float)
    else:
        abund_por_taxon = group.assign(_contagem=contagem).groupby(taxon_col)["_contagem"].sum()

    total = float(abund_por_taxon.sum())
    if total <= 0:
        return 0.0

    p = abund_por_taxon / total
    p = p[p > 0]
    if p.empty:
        return 0.0
    return float(-(p * p.apply(math.log)).sum())


def _pielou(shannon: float, riqueza: int) -> float:
    if riqueza <= 1:
        return 0.0
    return float(shannon / math.log(riqueza)) if riqueza > 1 else 0.0


def _bmwp_from_group(group: pd.DataFrame) -> float:
    if "bmwp_score" not in group.columns or "nome_cientifico" not in group.columns:
        return 0.0

    tmp = group[["nome_cientifico", "bmwp_score"]].copy()
    tmp["bmwp_score"] = pd.to_numeric(tmp["bmwp_score"], errors="coerce").fillna(0)
    by_taxon = tmp.groupby("nome_cientifico", dropna=True)["bmwp_score"].max()
    return float(by_taxon.sum())


def _ept_metrics(group: pd.DataFrame) -> tuple[int, float]:
    if "ordem" not in group.columns:
        return 0, 0.0

    ordens_norm = group["ordem"].apply(_norm_text)
    mask_ept = ordens_norm.isin(EPT_ORDERS)

    riqueza_ept = int(group.loc[mask_ept, "nome_cientifico"].nunique()) if "nome_cientifico" in group.columns else 0
    abundancia_ept = float(_safe_series(group.loc[mask_ept], "contagem", default=0.0).sum())
    return riqueza_ept, abundancia_ept


def calcular_indicadores_por_ponto(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=GROUP_KEYS + [
            "numero_taxons",
            "riqueza",
            "abundancia_total",
            "biomassa_total",
            "shannon",
            "pielou",
            "bmwp_total",
            "riqueza_ept",
            "abundancia_ept",
        ])

    work = df.copy()
    work["contagem"] = _safe_series(work, "contagem", default=0.0)
    work["biomassa"] = _safe_series(work, "biomassa", default=0.0)

    rows: list[dict] = []
    for keys, group in work.groupby(GROUP_KEYS, dropna=False):
        richness = int(group["nome_cientifico"].nunique()) if "nome_cientifico" in group.columns else 0
        shannon = _shannon_from_group(group)
        pielou = _pielou(shannon, richness)

        grupo_biologico = str(group["grupo_biologico"].iloc[0]).strip().lower() if "grupo_biologico" in group.columns else ""
        is_zoobentos = grupo_biologico in {"zoobentos", "bentos"}

        bmwp_total = _bmwp_from_group(group) if is_zoobentos else 0.0
        riqueza_ept, abundancia_ept = _ept_metrics(group) if is_zoobentos else (0, 0.0)

        row = {
            "projeto": keys[0],
            "campanha": keys[1],
            "ponto": keys[2],
            "latitude": keys[3],
            "longitude": keys[4],
            "grupo_biologico": keys[5],
            "numero_taxons": richness,
            "riqueza": richness,
            "abundancia_total": float(group["contagem"].sum()),
            "biomassa_total": float(group["biomassa"].sum()),
            "shannon": float(shannon),
            "pielou": float(pielou),
            "bmwp_total": float(bmwp_total),
            "riqueza_ept": int(riqueza_ept),
            "abundancia_ept": float(abundancia_ept),
        }
        rows.append(row)

    return pd.DataFrame(rows)