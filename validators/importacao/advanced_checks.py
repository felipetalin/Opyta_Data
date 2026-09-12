"""Regras de Gate A compartilhadas por todos os grupos do Streamlit."""
from __future__ import annotations

import re
import unicodedata

import pandas as pd

from .report import ValidationIssue, ValidationReport


PHYSICAL_GROUPS = {"meio fisico"}


def _norm(value: object) -> str:
    if pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKD", str(value).replace("\u00a0", " "))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip().lower()


def _column(df: pd.DataFrame, *aliases: str) -> str | None:
    if df is None:
        return None
    normalized = {col: _norm(col).replace(" ", "_") for col in df.columns}
    for alias in aliases:
        wanted = _norm(alias).replace(" ", "_")
        match = next((col for col, key in normalized.items() if key == wanted), None)
        if match is not None:
            return match
    return None


def _blank(value: object) -> bool:
    return pd.isna(value) or not str(value).strip()


def _number(value: object) -> float | None:
    if _blank(value):
        return None
    try:
        return float(str(value).strip().replace(".", "").replace(",", ".")) if "," in str(value) else float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _add(report: ValidationReport, code: str, severity: str, message: str, *, lines=None, sheet=None) -> None:
    report.issues.append(ValidationIssue(code=code, severity=severity, message=message, lines=list(lines or []), sheet=sheet))


def check_required_values(report: ValidationReport, group: str) -> None:
    identity = ("Parametro",) if _norm(group) in PHYSICAL_GROUPS else ("Nome_Cientifico", "Especie", "Taxon")
    rules = [
        ("capa", report.df_capa, (("Codigo_Opyta",),)),
        ("pontos", report.df_pontos, (("Campanha",), ("Ponto",))),
        ("resultados", report.df_resultados, (("Campanha",), ("Ponto",), identity)),
    ]
    for sheet, df, fields in rules:
        if df is None or df.empty:
            continue
        for aliases in fields:
            col = _column(df, *aliases)
            if col is None:
                _add(report, "MISSING_REQUIRED_COLUMN", "block", f"Coluna obrigatoria ausente em {sheet}: {aliases[0]}.", sheet=sheet)
                continue
            rows = [int(idx + 2) for idx, value in df[col].items() if _blank(value)]
            if rows:
                _add(report, "MISSING_REQUIRED_VALUE", "block", f"{len(rows)} valor(es) obrigatorios ausentes em {sheet}.{col}.", lines=rows[:50], sheet=sheet)


def check_numeric_values(report: ValidationReport, group: str) -> None:
    candidates = {
        "esforco": ("Esforco", "Esforco_Valor", "Esforco_Amostral"),
        "resultados": (
            "Quantidade", "Numero_de_Individuos", "Abundancia", "Biomassa", "PC_g",
            "Comprimento", "CPUE", "Densidade", "Biovolume", "Volume_Filtrado",
            "Valor_Medido", "BMWP_Score",
        ),
    }
    for sheet, names in candidates.items():
        df = getattr(report, f"df_{sheet}", None)
        if df is None or df.empty:
            continue
        for name in names:
            col = _column(df, name)
            if col is None:
                continue
            invalid, negative = [], []
            for idx, value in df[col].items():
                if _blank(value):
                    continue
                parsed = _number(value)
                if parsed is None:
                    invalid.append(int(idx + 2))
                elif parsed < 0:
                    negative.append(int(idx + 2))
            if invalid or negative:
                rows = (invalid + negative)[:50]
                _add(report, "INVALID_NUMERIC_VALUES", "block", f"{sheet}.{col}: {len(invalid)} valor(es) nao numericos e {len(negative)} negativo(s).", lines=rows, sheet=sheet)


def check_campaign_dates(report: ValidationReport) -> None:
    df = report.df_pontos
    if df is None or df.empty:
        return
    camp_col = _column(df, "Campanha")
    date_col = _column(df, "Data", "Data_Coleta")
    if camp_col is None or date_col is None:
        return
    patterns = (
        re.compile(r"^c\d+[-_](\d{4})[-_](\d{1,2})", re.I),
        re.compile(r"^\d+\D+([a-z]{3})[-_/](\d{2,4})$", re.I),
    )
    months = {"jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6, "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}
    bad = []
    for idx, row in df.iterrows():
        label = _norm(row.get(camp_col))
        parsed = pd.to_datetime(row.get(date_col), errors="coerce")
        expected = None
        match = patterns[0].match(label)
        if match:
            expected = (int(match.group(1)), int(match.group(2)))
        else:
            match = patterns[1].match(label)
            if match and match.group(1) in months:
                year = int(match.group(2)); year = year + 2000 if year < 100 else year
                expected = (year, months[match.group(1)])
        if expected and (pd.isna(parsed) or (parsed.year, parsed.month) != expected):
            bad.append(int(idx + 2))
    if bad:
        _add(report, "CAMPAIGN_DATE_MISMATCH", "block", f"{len(bad)} linha(s) possuem campanha e data com ano/mes divergentes.", lines=bad[:50], sheet="pontos")


def check_duplicates(report: ValidationReport, group: str) -> None:
    df = report.df_resultados
    if df is not None and not df.empty:
        rows = [int(idx + 2) for idx in df.index[df.duplicated(keep=False)]]
        if rows:
            _add(report, "EXACT_RESULT_DUPLICATES", "warning", f"{len(rows)} linha(s) de resultados sao duplicatas exatas; confirme antes de migrar.", lines=rows[:50], sheet="resultados")
    species = report.df_cadastro_especies
    if species is not None and not species.empty:
        col = _column(species, "Nome_Cientifico")
        if col:
            keys = species[col].map(_norm)
            rows = [int(idx + 2) for idx in species.index[keys.ne("") & keys.duplicated(keep=False)]]
            if rows:
                _add(report, "SPECIES_CATALOG_DUPLICATES", "block", f"{len(rows)} linha(s) duplicadas no cadastro de especies.", lines=rows[:50], sheet="cadastro_especies")


def check_effort_details(report: ValidationReport, group: str) -> None:
    if _norm(group) in PHYSICAL_GROUPS or report.df_esforco is None or report.df_esforco.empty:
        return
    results, effort = report.df_resultados, report.df_esforco
    res_method = _column(results, "Metodo_de_Captura", "Metodo_Amostragem", "Metodo")
    eff_method = _column(effort, "Metodo_de_Captura", "Metodo_Amostragem", "Metodo")
    if res_method and eff_method:
        left = {_norm(v) for v in results[res_method].dropna() if _norm(v)}
        right = {_norm(v) for v in effort[eff_method].dropna() if _norm(v)}
        if left != right:
            _add(report, "METHOD_SET_MISMATCH", "warning", f"Metodos divergem entre Resultados ({sorted(left)}) e Metadados_Esforco ({sorted(right)}).")

    res_value = _column(results, "Esforco_Amostral", "Esforco_Valor")
    eff_value = _column(effort, "Esforco", "Esforco_Valor", "Esforco_Amostral")
    rc, rp = _column(results, "Campanha"), _column(results, "Ponto")
    ec, ep = _column(effort, "Campanha"), _column(effort, "Ponto")
    if not all((res_value, eff_value, rc, rp, ec, ep)):
        return
    effort_map = {(_norm(row[ec]), _norm(row[ep])): _number(row[eff_value]) for _, row in effort.iterrows()}
    bad = []
    for idx, row in results.iterrows():
        actual, expected = _number(row[res_value]), effort_map.get((_norm(row[rc]), _norm(row[rp])))
        if actual is not None and expected is not None and abs(actual - expected) > 1e-9:
            bad.append(int(idx + 2))
    if bad:
        _add(report, "EFFORT_VALUE_MISMATCH", "block", f"{len(bad)} resultado(s) possuem esforco diferente dos metadados.", lines=bad[:50], sheet="resultados")


def run_advanced_checks(report: ValidationReport, group: str) -> None:
    check_required_values(report, group)
    check_numeric_values(report, group)
    check_campaign_dates(report)
    check_duplicates(report, group)
    check_effort_details(report, group)
