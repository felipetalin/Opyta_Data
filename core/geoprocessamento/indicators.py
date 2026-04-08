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

PHYSICAL_GROUP_KEYS = [
    "projeto",
    "campanha",
    "ponto",
    "latitude",
    "longitude",
    "matriz",
]

EPT_ORDERS = {"ephemeroptera", "plecoptera", "trichoptera"}

IQA_PARAM_ALIASES = {
    "od": {"oxigenio dissolvido (od)", "oxigenio dissolvido", "od"},
    "ph": {"ph"},
    "dbo": {"demanda bioquimica de oxigenio", "dbo (5 dias, 20°c)", "dbo (5 dias, 20c)", "dbo"},
    "nitrato": {"nitrato"},
    "turbidez": {"turbidez"},
    "solidos_dissolvidos": {"solidos dissolvidos totais", "solidos totais dissolvidos"},
    "fosforo_total": {
        "fosforo total",
        "fosforo total (ambientes loticos)",
        "fosforo total (ambientes lenticos)",
    },
    "coliformes": {
        "coliformes termotolerantes",
        "coliformes termotolerantes por tubos multiplos - nmp",
        "escherichia coli",
        "escherichia coli por tubos multiplos (substrato enzimatico) – nmp",
        "escherichia coli por tubos multiplos (substrato enzimatico) - nmp",
    },
}


def _norm_text(value: object) -> str:
    s = str(value or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _safe_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series([default] * len(df), index=df.index)
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def _match_iqa_param(name: object) -> str | None:
    norm_name = _norm_text(name)
    for key, aliases in IQA_PARAM_ALIASES.items():
        if norm_name in aliases:
            return key
    return None


def _score_from_limits(value: float | None, min_limit: float | None, max_limit: float | None) -> float | None:
    if value is None or pd.isna(value):
        return None

    if min_limit is not None and not pd.isna(min_limit) and max_limit is not None and not pd.isna(max_limit):
        if min_limit <= value <= max_limit:
            return 100.0
        if value < min_limit:
            return max(0.0, min(100.0, 100.0 * (value / min_limit))) if min_limit > 0 else 0.0
        return max(0.0, min(100.0, 100.0 * (max_limit / value))) if value > 0 else 0.0

    if min_limit is not None and not pd.isna(min_limit):
        return max(0.0, min(100.0, 100.0 * (value / min_limit))) if min_limit > 0 else 0.0

    if max_limit is not None and not pd.isna(max_limit):
        if value <= 0:
            return 100.0
        return max(0.0, min(100.0, 100.0 * (max_limit / value)))

    return None


def _is_non_compliant(value: float | None, min_limit: float | None, max_limit: float | None) -> bool | None:
    if value is None or pd.isna(value):
        return None
    has_min = min_limit is not None and not pd.isna(min_limit)
    has_max = max_limit is not None and not pd.isna(max_limit)
    if not has_min and not has_max:
        return None
    if has_min and value < float(min_limit):
        return True
    if has_max and value > float(max_limit):
        return True
    return False


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
            "bmwp_registros_com_score",
            "bmwp_registros_sem_score",
            "ept_registros_com_ordem",
            "ept_registros_sem_ordem",
        ])

    work = df.copy()
    work["contagem"] = _safe_series(work, "contagem", default=0.0)
    work["biomassa"] = _safe_series(work, "biomassa", default=0.0)

    rows: list[dict] = []
    for keys, group in work.groupby(GROUP_KEYS, dropna=False):
        richness = int(group["nome_cientifico"].nunique()) if "nome_cientifico" in group.columns else 0
        shannon = _shannon_from_group(group)
        pielou = _pielou(shannon, richness)

        grupo_biologico = _norm_text(group["grupo_biologico"].iloc[0]) if "grupo_biologico" in group.columns else ""
        is_zoobentos = (
            grupo_biologico in {"zoobentos", "bentos", "macrozoobentos", "macroinvertebrados bentonicos"}
            or "bento" in grupo_biologico
        )

        bmwp_total = _bmwp_from_group(group) if is_zoobentos else 0.0
        riqueza_ept, abundancia_ept = _ept_metrics(group) if is_zoobentos else (0, 0.0)

        if is_zoobentos and "bmwp_score" in group.columns:
            bmwp_scores = pd.to_numeric(group["bmwp_score"], errors="coerce")
            bmwp_registros_com_score = int(bmwp_scores.notna().sum())
            bmwp_registros_sem_score = int(bmwp_scores.isna().sum())
        elif is_zoobentos:
            bmwp_registros_com_score = 0
            bmwp_registros_sem_score = int(len(group))
        else:
            bmwp_registros_com_score = 0
            bmwp_registros_sem_score = 0

        if is_zoobentos and "ordem" in group.columns:
            ordens = group["ordem"].apply(_norm_text)
            ept_registros_com_ordem = int((ordens != "").sum())
            ept_registros_sem_ordem = int((ordens == "").sum())
        elif is_zoobentos:
            ept_registros_com_ordem = 0
            ept_registros_sem_ordem = int(len(group))
        else:
            ept_registros_com_ordem = 0
            ept_registros_sem_ordem = 0

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
            "bmwp_registros_com_score": bmwp_registros_com_score,
            "bmwp_registros_sem_score": bmwp_registros_sem_score,
            "ept_registros_com_ordem": ept_registros_com_ordem,
            "ept_registros_sem_ordem": ept_registros_sem_ordem,
        }
        rows.append(row)

    return pd.DataFrame(rows)


def calcular_indicadores_fisicos_por_ponto(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=PHYSICAL_GROUP_KEYS + [
            "iqa",
            "parametros_nao_conformes",
            "parametros_avaliados",
            "parametros_com_limite",
            "pontos_amostrados",
        ])

    work = df.copy()
    required_defaults: dict[str, object] = {
        "projeto": "",
        "campanha": "",
        "ponto": "",
        "latitude": None,
        "longitude": None,
        "matriz": "Água Superficial",
        "nome_parametro": "",
        "valor_medido": None,
        "vmp_357_cl2_min": None,
        "vmp_357_cl2_max": None,
        "vmp_amonia_dinamico": None,
    }
    for col, default in required_defaults.items():
        if col not in work.columns:
            work[col] = default

    work["valor_medido"] = _safe_series(work, "valor_medido", default=0.0)
    work["vmp_357_cl2_min"] = pd.to_numeric(work.get("vmp_357_cl2_min"), errors="coerce")
    work["vmp_357_cl2_max"] = pd.to_numeric(work.get("vmp_357_cl2_max"), errors="coerce")
    work["vmp_amonia_dinamico"] = pd.to_numeric(work.get("vmp_amonia_dinamico"), errors="coerce")

    rows: list[dict] = []
    for keys, group in work.groupby(PHYSICAL_GROUP_KEYS, dropna=False):
        sub_scores: dict[str, float] = {}
        non_compliant_params: set[str] = set()
        params_with_limit: set[str] = set()

        for _, row in group.iterrows():
            param_name = row.get("nome_parametro")
            value = row.get("valor_medido")
            min_limit = row.get("vmp_357_cl2_min")
            max_limit = row.get("vmp_357_cl2_max")

            norm_param = _norm_text(param_name)
            if norm_param == "amonia" and pd.notna(row.get("vmp_amonia_dinamico")):
                max_limit = row.get("vmp_amonia_dinamico")

            compliance = _is_non_compliant(value, min_limit, max_limit)
            if compliance is not None:
                params_with_limit.add(str(param_name))
                if compliance:
                    non_compliant_params.add(str(param_name))

            iqa_key = _match_iqa_param(param_name)
            if iqa_key and iqa_key not in sub_scores:
                score = _score_from_limits(value, min_limit, max_limit)
                if score is not None:
                    sub_scores[iqa_key] = float(score)

        iqa = float(sum(sub_scores.values()) / len(sub_scores)) if sub_scores else 0.0

        rows.append(
            {
                "projeto": keys[0],
                "campanha": keys[1],
                "ponto": keys[2],
                "latitude": keys[3],
                "longitude": keys[4],
                "matriz": keys[5],
                "iqa": iqa,
                "parametros_nao_conformes": int(len(non_compliant_params)),
                "parametros_avaliados": int(group["nome_parametro"].nunique()) if "nome_parametro" in group.columns else 0,
                "parametros_com_limite": int(len(params_with_limit)),
                "pontos_amostrados": 1,
            }
        )

    return pd.DataFrame(rows)