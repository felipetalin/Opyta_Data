from dataclasses import dataclass
from typing import List, Tuple, Dict, Protocol
import pandas as pd


class Validator(Protocol):
    def validate(self, xls: pd.ExcelFile) -> Tuple[bool, List[str]]:
        ...


@dataclass(frozen=True)
class RequiredSheetsValidator:
    group: str
    required_sheets: List[str]

    def validate(self, xls: pd.ExcelFile) -> Tuple[bool, List[str]]:
        errors = []
        existing = set(xls.sheet_names)

        missing = [s for s in self.required_sheets if s not in existing]
        if missing:
            errors.append(f"Abas ausentes: {missing}")

        return (len(errors) == 0), errors


# ✅ Validadores por grupo (ajuste fino por grupo)
VALIDATORS: Dict[str, Validator] = {
    # BIOTA (padrão com esforço)
    "Ictiofauna": RequiredSheetsValidator(
        "Ictiofauna",
        ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Ictiofauna"],
    ),
    "Bentos": RequiredSheetsValidator(
        "Bentos",
        ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Zoobentos"],
    ),
    "Fitoplâncton": RequiredSheetsValidator(
        "Fitoplâncton",
        ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Fitoplancton"],
    ),
    "Zooplâncton": RequiredSheetsValidator(
        "Zooplâncton",
        ["Capa_Projeto", "Pontos_e_Campanhas", "Metadados_Esforco", "Resultados_Zooplancton"],
    ),

    # ✅ MEIO FÍSICO (SEM esforço)
    "Meio Físico": RequiredSheetsValidator(
        "Meio Físico",
        ["Capa_Projeto", "Pontos_e_Campanhas", "Resultados_Meio_Fisico"],
    ),
}