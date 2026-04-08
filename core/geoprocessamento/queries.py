from __future__ import annotations

from typing import Literal

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

ModoGeo = Literal["Fisico", "Biota"]


def _build_in_clause(prefix: str, values: list[str], params: dict[str, str]) -> str:
    placeholders: list[str] = []
    for idx, value in enumerate(values):
        key = f"{prefix}_{idx}"
        params[key] = value
        placeholders.append(f":{key}")
    return ", ".join(placeholders)


def get_geo_fisico(conn, projetos: list[str] | None = None, campanhas: list[str] | None = None) -> pd.DataFrame:
    projetos = projetos or []
    campanhas = campanhas or []

    params: dict[str, str] = {}
    conditions = [
        "latitude IS NOT NULL",
        "longitude IS NOT NULL",
        "matriz = 'Água Superficial'",
    ]

    if projetos:
        conditions.append(
            f"nome_projeto IN ({_build_in_clause('projeto_fisico', projetos, params)})"
        )
    if campanhas:
        conditions.append(
            f"nome_campanha IN ({_build_in_clause('campanha_fisico', campanhas, params)})"
        )

    query = text(
        f"""
        SELECT
            nome_projeto AS projeto,
            nome_campanha AS campanha,
            nome_ponto AS ponto,
            latitude,
            longitude,
            matriz,
            nome_parametro,
            sinal_limite,
            valor_medido,
            unidade_medida,
            vmp_357_cl2_min,
            vmp_357_cl2_max,
            vmp_amonia_dinamico,
            data_hora_coleta
        FROM public.fisico_analise_consolidada
        WHERE {' AND '.join(conditions)}
        ORDER BY nome_projeto, nome_campanha, nome_ponto, nome_parametro
        """
    )
    try:
        return pd.read_sql(query, conn, params=params)
    except SQLAlchemyError:
        # Fallback para ambientes que ainda nao possuem a tabela consolidada.
        legacy_params: dict[str, str] = {}
        legacy_conditions = ["latitude IS NOT NULL", "longitude IS NOT NULL"]
        if projetos:
            legacy_conditions.append(
                f"projeto IN ({_build_in_clause('projeto_legacy', projetos, legacy_params)})"
            )
        if campanhas:
            legacy_conditions.append(
                f"campanha IN ({_build_in_clause('campanha_legacy', campanhas, legacy_params)})"
            )

        legacy_query = text(
            f"""
            SELECT
                projeto,
                campanha,
                ponto,
                latitude,
                longitude,
                'Água Superficial' AS matriz,
                nome_parametro,
                NULL::character varying AS sinal_limite,
                valor_medido,
                unidade_medida,
                NULL::numeric AS vmp_357_cl2_min,
                NULL::numeric AS vmp_357_cl2_max,
                NULL::numeric AS vmp_amonia_dinamico,
                data_hora_coleta
            FROM public.vw_geo_fisico
            WHERE {' AND '.join(legacy_conditions)}
            ORDER BY projeto, campanha, ponto, nome_parametro
            """
        )
        return pd.read_sql(legacy_query, conn, params=legacy_params)


def get_geo_biota(
    conn,
    projetos: list[str] | None = None,
    campanhas: list[str] | None = None,
    grupos_biologicos: list[str] | None = None,
) -> pd.DataFrame:
    projetos = projetos or []
    campanhas = campanhas or []
    grupos_biologicos = grupos_biologicos or []

    params: dict[str, str] = {}
    conditions = ["latitude IS NOT NULL", "longitude IS NOT NULL"]

    if projetos:
        conditions.append(f"nome_projeto IN ({_build_in_clause('projeto', projetos, params)})")
    if campanhas:
        conditions.append(f"nome_campanha IN ({_build_in_clause('campanha', campanhas, params)})")
    if grupos_biologicos:
        conditions.append(
            f"grupo_biologico IN ({_build_in_clause('grupo_biologico', grupos_biologicos, params)})"
        )

    query = text(
        f"""
        SELECT
            nome_projeto AS projeto,
            nome_campanha AS campanha,
            nome_ponto AS ponto,
            latitude,
            longitude,
            grupo_biologico,
            nome_cientifico,
            contagem,
            biomassa,
            ordem,
            bmwp_score
        FROM public.biota_analise_consolidada
        WHERE {' AND '.join(conditions)}
        ORDER BY nome_projeto, nome_campanha, nome_ponto
        """
    )
    return pd.read_sql(query, conn, params=params)


def listar_projetos(conn, modo: ModoGeo) -> list[str]:
    if modo == "Fisico":
        query = text(
            """
            SELECT DISTINCT nome_projeto AS projeto
            FROM public.fisico_analise_consolidada
            WHERE matriz = 'Água Superficial'
              AND nome_projeto IS NOT NULL
            ORDER BY nome_projeto
            """
        )
    else:
        query = text(
            """
            SELECT DISTINCT projeto
            FROM public.vw_geo_biota
            WHERE projeto IS NOT NULL
            ORDER BY projeto
            """
        )
    df = pd.read_sql(query, conn)
    return df["projeto"].astype(str).tolist() if not df.empty else []


def listar_campanhas(conn, modo: ModoGeo, projetos: list[str] | None = None) -> list[str]:
    projetos = projetos or []

    params: dict[str, str] = {}
    conditions = ["campanha IS NOT NULL"]
    if projetos:
        conditions.append(f"projeto IN ({_build_in_clause('projeto', projetos, params)})")

    if modo == "Fisico":
        fisico_conditions = ["nome_campanha IS NOT NULL", "matriz = 'Água Superficial'"]
        if projetos:
            fisico_conditions.append(
                f"nome_projeto IN ({_build_in_clause('projeto_fisico', projetos, params)})"
            )
        query = text(
            f"""
            SELECT DISTINCT nome_campanha AS campanha
            FROM public.fisico_analise_consolidada
            WHERE {' AND '.join(fisico_conditions)}
            ORDER BY nome_campanha
            """
        )
    else:
        query = text(
            f"""
            SELECT DISTINCT campanha
            FROM public.vw_geo_biota
            WHERE {' AND '.join(conditions)}
            ORDER BY campanha
            """
        )
    df = pd.read_sql(query, conn, params=params)
    return df["campanha"].astype(str).tolist() if not df.empty else []


def listar_grupos_biologicos(conn, projetos: list[str] | None = None, campanhas: list[str] | None = None) -> list[str]:
    projetos = projetos or []
    campanhas = campanhas or []

    params: dict[str, str] = {}
    conditions = ["grupo_biologico IS NOT NULL"]
    if projetos:
        conditions.append(f"projeto IN ({_build_in_clause('projeto', projetos, params)})")
    if campanhas:
        conditions.append(f"campanha IN ({_build_in_clause('campanha', campanhas, params)})")

    query = text(
        f"""
        SELECT DISTINCT grupo_biologico
        FROM public.vw_geo_biota
        WHERE {' AND '.join(conditions)}
        ORDER BY grupo_biologico
        """
    )
    df = pd.read_sql(query, conn, params=params)
    return df["grupo_biologico"].astype(str).tolist() if not df.empty else []
