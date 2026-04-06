from __future__ import annotations

import re
from typing import Any

import pandas as pd

from .base import AnalysisResult, RunContext
from .export import export_dfs_xlsx


def valor_ou_padrao(row: pd.Series, candidatos: list[str], padrao: Any = None) -> Any:
    for col in candidatos:
        if col in row.index and pd.notna(row[col]) and str(row[col]).strip() != "":
            return row[col]
    return padrao


def normalizar_texto_id(valor: Any) -> str | None:
    if pd.isna(valor) or valor is None:
        return None
    txt = str(valor).strip()
    txt = txt.replace("/", "-").replace("\\", "-")
    txt = re.sub(r"\s+", " ", txt)
    return txt if txt else None


def normalizar_taxon_rank(nome_cientifico: Any) -> str | None:
    if pd.isna(nome_cientifico) or nome_cientifico is None:
        return None
    nome = str(nome_cientifico).strip()
    if " sp." in nome.lower() or nome.lower().endswith(" sp"):
        return "Genus"
    if len(nome.split()) >= 2:
        return "Species"
    return None


def montar_event_id(row: pd.Series) -> str | None:
    codigo = normalizar_texto_id(valor_ou_padrao(row, ["id_resultado_pk", "id_resultado"], None))
    campanha = normalizar_texto_id(valor_ou_padrao(row, ["nome_campanha"], "SEM_CAMPANHA"))
    ponto = normalizar_texto_id(valor_ou_padrao(row, ["nome_ponto"], "SEM_PONTO"))
    protocolo = normalizar_texto_id(valor_ou_padrao(row, ["metodo_de_captura"], None))

    partes: list[str] = []
    if codigo:
        partes.append(str(codigo))
    partes.extend([campanha, ponto])
    if protocolo:
        partes.append(protocolo)
    return "-".join(partes)


def montar_occurrence_id(row: pd.Series, idx: int) -> str:
    event_id = row["eventID"]
    nome_cientifico = str(valor_ou_padrao(row, ["nome_cientifico", "scientificName"], "sp.")).strip().replace(" ", "_")
    return f"{event_id}-{nome_cientifico}-{idx+1}"


def montar_sampling_effort(row: pd.Series) -> str | None:
    esforco = valor_ou_padrao(row, ["esforco"], None)
    unidade = valor_ou_padrao(row, ["unidade_esforco"], None)

    esforco_txt = "" if pd.isna(esforco) or esforco is None else str(esforco).strip()
    unidade_txt = "" if pd.isna(unidade) or unidade is None else str(unidade).strip()

    texto = f"{esforco_txt} {unidade_txt}".strip()
    return texto if texto else None


def gerar_darwin_core_dfs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if df is None or df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if "nome_cientifico" not in df.columns:
        raise ValueError("A coluna 'nome_cientifico' não foi encontrada no conjunto de dados.")

    df2 = df.copy()
    df2["eventID"] = df2.apply(montar_event_id, axis=1)
    df2["occurrenceID"] = [montar_occurrence_id(row, idx) for idx, row in df2.iterrows()]

    if "contagem" in df2.columns:
        df2["individualCount"] = pd.to_numeric(df2["contagem"], errors="coerce")
    elif "numero_de_individuos" in df2.columns:
        df2["individualCount"] = pd.to_numeric(df2["numero_de_individuos"], errors="coerce")
    else:
        df2["individualCount"] = None

    df2["taxonRank_DWC"] = df2["nome_cientifico"].apply(normalizar_taxon_rank)

    event_cols = [
        "eventID",
        "samplingProtocol",
        "samplingEffort",
        "sampleSizeValue",
        "sampleSizeUnit",
        "eventDate",
        "eventRemarks",
        "county",
        "municipality",
        "waterBody",
        "locality",
        "decimalLatitude",
        "decimalLongitude",
        "geodeticDatum",
    ]

    eventos = pd.DataFrame()
    eventos["eventID"] = df2["eventID"]
    eventos["samplingProtocol"] = df2["metodo_de_captura"] if "metodo_de_captura" in df2.columns else None
    eventos["samplingEffort"] = df2.apply(montar_sampling_effort, axis=1)
    eventos["sampleSizeValue"] = df2["unidade_esforco"] if "unidade_esforco" in df2.columns else None
    eventos["sampleSizeUnit"] = df2["esforco"] if "esforco" in df2.columns else None
    eventos["eventDate"] = df2["nome_campanha"] if "nome_campanha" in df2.columns else None
    eventos["eventRemarks"] = None
    eventos["county"] = "Desconhecido"
    eventos["municipality"] = "Desconhecido"
    eventos["waterBody"] = df2["bacia_hidrografica"] if "bacia_hidrografica" in df2.columns else None
    eventos["locality"] = "Desconhecido"
    eventos["decimalLatitude"] = df2["latitude"] if "latitude" in df2.columns else None
    eventos["decimalLongitude"] = df2["longitude"] if "longitude" in df2.columns else None
    eventos["geodeticDatum"] = "WGS84"

    df_sampling = eventos.drop_duplicates(subset=["eventID"]).reset_index(drop=True)
    df_sampling = df_sampling[event_cols]

    occ_cols = [
        "eventID",
        "occurrenceID",
        "basisOfRecord",
        "scientificName",
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "taxonRank",
        "identificationQualifier",
        "recordedBy",
        "individualCount",
        "sex",
        "lifeStage",
        "reproductiveCondition",
        "preparations",
        "occurrenceRemarks",
    ]

    df_occ = pd.DataFrame(
        {
            "eventID": df2["eventID"],
            "occurrenceID": df2["occurrenceID"],
            "basisOfRecord": "HumanObservation",
            "scientificName": df2["nome_cientifico"],
            "kingdom": df2["reino"] if "reino" in df2.columns else None,
            "phylum": df2["filo"] if "filo" in df2.columns else None,
            "class": df2["classe"] if "classe" in df2.columns else None,
            "order": df2["ordem"] if "ordem" in df2.columns else None,
            "family": df2["familia"] if "familia" in df2.columns else None,
            "taxonRank": df2["taxonRank_DWC"],
            "identificationQualifier": None,
            "recordedBy": None,
            "individualCount": df2["individualCount"],
            "sex": None,
            "lifeStage": None,
            "reproductiveCondition": None,
            "preparations": None,
            "occurrenceRemarks": None,
        }
    )[occ_cols]

    bio_cols = [
        "eventID",
        "occurrenceID",
        "scientificName",
        "individualCount",
        "Weight",
        "StandardLength",
        "TotalLength",
        "Sex",
        "GonadalStage",
        "GonadWeight",
    ]

    df_bio = pd.DataFrame(
        {
            "eventID": df2["eventID"],
            "occurrenceID": df2["occurrenceID"],
            "scientificName": df2["nome_cientifico"],
            "individualCount": df2["individualCount"],
            "Weight": df2["biomassa"] if "biomassa" in df2.columns else None,
            "StandardLength": df2["medida_1"] if "medida_1" in df2.columns else None,
            "TotalLength": df2["medida_2"] if "medida_2" in df2.columns else None,
            "Sex": None,
            "GonadalStage": None,
            "GonadWeight": None,
        }
    )[bio_cols]

    df_bio = df_bio[df_bio[["Weight", "StandardLength", "TotalLength", "Sex", "GonadalStage", "GonadWeight"]].notna().any(axis=1)].reset_index(drop=True)

    return df_sampling, df_occ, df_bio


def run(ctx: RunContext, df: pd.DataFrame) -> AnalysisResult:
    df_sampling, df_occ, df_bio = gerar_darwin_core_dfs(df)

    res = AnalysisResult(
        key="darwin_core",
        title="03) Darwin Core",
        df=df_sampling,
    )

    if ctx.exportar_arquivos:
        arquivo = f"03_darwin_core_{ctx.grupo.lower()}_{ctx.projeto.lower().replace(' ', '_')}.xlsx"
        arquivo = arquivo.replace("/", "-").replace("\\", "-")
        dfs = {
            "Sampling Events": df_sampling,
            "Associated Occurrences": df_occ,
            "Fish Biometric data": df_bio,
        }
        try:
            res.files.append(export_dfs_xlsx(dfs, ctx.pasta_saida, arquivo))
        except Exception as e:
            res.warnings.append(f"Falha ao exportar Darwin Core: {e}")

    if df_occ is not None and not df_occ.empty:
        res.warnings.append(f"Dados Darwin Core gerados: {len(df_sampling)} eventos, {len(df_occ)} ocorrências, {len(df_bio)} registros biométricos.")

    return res