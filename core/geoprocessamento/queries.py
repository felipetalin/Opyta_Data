from __future__ import annotations

from typing import Literal

import pandas as pd
from sqlalchemy import text

ModoGeo = Literal["Fisico", "Biota"]


def get_geo_fisico(conn, projeto: str = "", campanha: str = "") -> pd.DataFrame:
    query = text(
        """
        SELECT
            projeto,
            campanha,
            ponto,
            latitude,
            longitude
        FROM public.vw_geo_fisico
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND (:projeto = '' OR projeto = :projeto)
          AND (:campanha = '' OR campanha = :campanha)
        ORDER BY projeto, campanha, ponto
        """
    )
    return pd.read_sql(query, conn, params={"projeto": projeto or "", "campanha": campanha or ""})


def get_geo_biota(conn, projeto: str = "", campanha: str = "") -> pd.DataFrame:
    query = text(
        """
        SELECT
            projeto,
            campanha,
            ponto,
            latitude,
            longitude
        FROM public.vw_geo_biota
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND (:projeto = '' OR projeto = :projeto)
          AND (:campanha = '' OR campanha = :campanha)
        ORDER BY projeto, campanha, ponto
        """
    )
    return pd.read_sql(query, conn, params={"projeto": projeto or "", "campanha": campanha or ""})


def listar_projetos(conn, modo: ModoGeo) -> list[str]:
    view_name = "vw_geo_fisico" if modo == "Fisico" else "vw_geo_biota"
    query = text(
        f"""
        SELECT DISTINCT projeto
        FROM public.{view_name}
        WHERE projeto IS NOT NULL
        ORDER BY projeto
        """
    )
    df = pd.read_sql(query, conn)
    return df["projeto"].astype(str).tolist() if not df.empty else []


def listar_campanhas(conn, modo: ModoGeo, projeto: str = "") -> list[str]:
    view_name = "vw_geo_fisico" if modo == "Fisico" else "vw_geo_biota"
    query = text(
        f"""
        SELECT DISTINCT campanha
        FROM public.{view_name}
        WHERE campanha IS NOT NULL
          AND (:projeto = '' OR projeto = :projeto)
        ORDER BY campanha
        """
    )
    df = pd.read_sql(query, conn, params={"projeto": projeto or ""})
    return df["campanha"].astype(str).tolist() if not df.empty else []
