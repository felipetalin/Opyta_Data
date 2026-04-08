from __future__ import annotations

from pathlib import Path

import branca.colormap as cm
import folium
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


def _value_range(df: pd.DataFrame, indicador: str) -> tuple[float, float]:
    values = pd.to_numeric(df[indicador], errors="coerce").fillna(0) if indicador in df.columns else pd.Series([0.0])
    return float(values.min()), float(values.max())


def _radius_for_value(value: float, min_v: float, max_v: float) -> float:
    if max_v <= min_v:
        return 6.0
    return 5.0 + 15.0 * ((value - min_v) / (max_v - min_v))


def build_leafmap(df: pd.DataFrame, modo: str, indicador: str):
    _cleanup_schema_desktop_ini()
    import leafmap.foliumap as leafmap

    center_lat = float(df["latitude"].mean())
    center_lon = float(df["longitude"].mean())

    m = leafmap.Map(center=(center_lat, center_lon), zoom=5)
    m.add_basemap("SATELLITE")

    min_v, max_v = _value_range(df, indicador)
    colormap = cm.LinearColormap(["#dcfce7", "#facc15", "#f97316", "#dc2626"], vmin=min_v, vmax=max_v)
    colormap.caption = f"Escala do indicador: {indicador}"

    layer_name = "Pontos Biota" if modo == "Biota" else "Pontos Fisico"
    feature_group = folium.FeatureGroup(name=layer_name)

    for _, row in df.iterrows():
        value = float(pd.to_numeric(row.get(indicador, 0), errors="coerce") or 0.0)
        radius = _radius_for_value(value, min_v, max_v)
        color = colormap(value)

        tooltip_parts = [
            f"Projeto: {row.get('projeto', '-')}",
            f"Empreendimento: {row.get('empreendimento', row.get('projeto', '-'))}",
            f"Campanha: {row.get('campanha', '-')}",
            f"Ponto: {row.get('ponto', '-')}",
            f"{indicador}: {value:.3f}",
        ]
        if "riqueza" in row.index:
            tooltip_parts.append(f"Riqueza: {float(pd.to_numeric(row.get('riqueza', 0), errors='coerce') or 0):.2f}")
        if "shannon" in row.index:
            tooltip_parts.append(f"Diversidade: {float(pd.to_numeric(row.get('shannon', 0), errors='coerce') or 0):.2f}")
        if "abundancia_total" in row.index:
            tooltip_parts.append(f"Abundancia: {float(pd.to_numeric(row.get('abundancia_total', 0), errors='coerce') or 0):.0f}")
        if "iqa" in row.index:
            tooltip_parts.append(f"IQA: {float(pd.to_numeric(row.get('iqa', 0), errors='coerce') or 0):.1f}")

        popup_html = (
            f"<b>Projeto:</b> {row.get('projeto', '-')}<br>"
            f"<b>Empreendimento:</b> {row.get('empreendimento', row.get('projeto', '-'))}<br>"
            f"<b>Campanha:</b> {row.get('campanha', '-')}<br>"
            f"<b>Ponto:</b> {row.get('ponto', '-')}<br>"
            f"<b>{indicador}:</b> {value:.3f}<br>"
            f"<b>Latitude:</b> {float(row.get('latitude', 0)):.5f}<br>"
            f"<b>Longitude:</b> {float(row.get('longitude', 0)):.5f}"
        )

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=radius,
            color=color,
            weight=1,
            fill=True,
            fill_opacity=0.8,
            tooltip=folium.Tooltip("<br>".join(tooltip_parts), sticky=True),
            popup=folium.Popup(popup_html, max_width=320),
        ).add_to(feature_group)

    feature_group.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    colormap.add_to(m)
    return m


def render_mapa_geo(df: pd.DataFrame, modo: str, indicador: str) -> None:
    st.subheader("Mapa")

    if df.empty:
        st.warning("Nenhum ponto georreferenciado encontrado para os filtros selecionados.")
        return

    try:
        m = build_leafmap(df, modo, indicador)
        m.to_streamlit(height=620)
    except ModuleNotFoundError:
        st.error("Dependencia leafmap nao encontrada no ambiente. Instale o pacote leafmap para habilitar o mapa.")
    except Exception as exc:
        st.error(f"Erro ao renderizar mapa interativo: {exc}")
