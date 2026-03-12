from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class Theme:
    name: str
    cor_principal: str
    cores_campanhas: Dict[str, str]  # mapeia "Seca"/"Chuva" quando possível
    paleta_padrao: List[str]


THEMES = {
    "cliente_verde": Theme(
        name="cliente_verde",
        cor_principal="#2E7D32",
        cores_campanhas={"Seca": "#66BB6A", "Chuva": "#2E7D32"},
        paleta_padrao=["#2E7D32", "#66BB6A", "#1B5E20", "#A5D6A7"],
    ),
    "cliente_azul": Theme(
        name="cliente_azul",
        cor_principal="#1f77b4",
        cores_campanhas={"Seca": "#73bfe2", "Chuva": "#1f77b4"},
        paleta_padrao=["#1f77b4", "#73bfe2", "#0D47A1", "#90CAF9"],
    ),
    "neutro": Theme(
        name="neutro",
        cor_principal="#424242",
        cores_campanhas={"Seca": "#9E9E9E", "Chuva": "#424242"},
        paleta_padrao=["#424242", "#9E9E9E", "#212121", "#BDBDBD"],
    ),
}


def get_theme(tema: str) -> Theme:
    return THEMES.get(tema, THEMES["cliente_azul"])


def ordem_campanhas_padrao(grupo: str) -> list[str] | None:
    # padrão que você já usa (ajustável por grupo depois)
    if grupo.lower() in ("ictiofauna", "ictio"):
        return ["1º Campanha (Seca)", "2º Campanha (Chuva)"]
    return None