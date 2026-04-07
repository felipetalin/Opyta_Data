from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


def _cleanup_schema_desktop_ini() -> None:
    schema_root = Path(
        "G:/Meu Drive/Opyta/Opyta_Data/.venv/Lib/site-packages/jsonschema_specifications/schemas"
    )
    if not schema_root.exists():
        return

    for desktop_ini in schema_root.rglob("desktop.ini"):
        try:
            desktop_ini.unlink(missing_ok=True)
        except Exception:
            # Melhor esforço: falhas aqui nao devem quebrar a pagina.
            pass


def build_leafmap(df: pd.DataFrame, modo: str):
    _cleanup_schema_desktop_ini()
    import leafmap.foliumap as leafmap

    center_lat = float(df["latitude"].mean())
    center_lon = float(df["longitude"].mean())

    m = leafmap.Map(center=(center_lat, center_lon), zoom=5)
    m.add_basemap("SATELLITE")

    layer_name = "Pontos Biota" if modo == "Biota" else "Pontos Fisico"
    m.add_points_from_xy(
        data=df,
        x="longitude",
        y="latitude",
        popup=["projeto", "campanha", "ponto"],
        layer_name=layer_name,
    )
    return m


def render_mapa_geo(df: pd.DataFrame, modo: str) -> None:
    st.subheader("Mapa")

    if df.empty:
        st.warning("Nenhum ponto georreferenciado encontrado para os filtros selecionados.")
        return

    try:
        m = build_leafmap(df, modo)
        m.to_streamlit(height=620)
    except ModuleNotFoundError:
        st.error("Dependencia leafmap nao encontrada no ambiente. Instale o pacote leafmap para habilitar o mapa.")
    except Exception as exc:
        st.error(f"Erro ao renderizar mapa interativo: {exc}")
