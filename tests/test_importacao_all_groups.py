from __future__ import annotations

from io import BytesIO

import pandas as pd
import pytest

from core.modelos_oficiais import IMPORT_TEMPLATE_SPECS
from validators.importacao.pipeline import validate_importacao_file


GROUPS = tuple(IMPORT_TEMPLATE_SPECS)


def workbook(group: str, *, blank_identity: bool = False) -> BytesIO:
    spec = IMPORT_TEMPLATE_SPECS[group]
    result_sheet = str(spec["result_sheet"])
    identity = "Parametro" if group == "Meio Físico" else "Nome_Cientifico"
    result = {header: "" for header in spec["result_headers"]}
    result.update({"Campanha": "C001-2026-01", "Ponto": "PT-01", identity: "" if blank_identity else ("pH" if group == "Meio Físico" else "Astyanax sp.")})
    for name in ("Quantidade", "Densidade", "Valor_Medido"):
        if name in result:
            result[name] = "1"

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame([{"Codigo_Opyta": "TEST001", "Nome_Projeto": "Teste"}]).to_excel(writer, sheet_name="Capa_Projeto", index=False)
        pd.DataFrame([{"Campanha": "C001-2026-01", "Ponto": "PT-01", "Latitude": "-20.0", "Longitude": "-43.0", "Data_Coleta": "2026-01-15"}]).to_excel(writer, sheet_name="Pontos_e_Campanhas", index=False)
        if group != "Meio Físico":
            pd.DataFrame([{"Campanha": "C001-2026-01", "Ponto": "PT-01", "Metodo_Amostragem": "Padrao", "Esforco_Valor": "1"}]).to_excel(writer, sheet_name="Metadados_Esforco", index=False)
        pd.DataFrame([result]).to_excel(writer, sheet_name=result_sheet, index=False)
    output.seek(0)
    return output


@pytest.mark.parametrize("group", GROUPS)
def test_official_streamlit_groups_pass_common_validation(group: str) -> None:
    report = validate_importacao_file(workbook(group), group=group)
    assert report.can_proceed, [(issue.code, issue.message) for issue in report.blocks]


@pytest.mark.parametrize("group", GROUPS)
def test_all_groups_block_blank_result_identity(group: str) -> None:
    report = validate_importacao_file(workbook(group, blank_identity=True), group=group)
    assert "MISSING_REQUIRED_VALUE" in {issue.code for issue in report.blocks}


def test_effort_value_mismatch_is_blocked() -> None:
    source = workbook("Ictiofauna")
    sheets = pd.read_excel(source, sheet_name=None, dtype=str)
    sheets["Metadados_Esforco"]["Esforco_Valor"] = "2"
    sheets["Resultados_Ictiofauna"]["Esforco_Amostral"] = "1"
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    output.seek(0)
    report = validate_importacao_file(output, group="Ictiofauna")
    assert "EFFORT_VALUE_MISMATCH" in {issue.code for issue in report.blocks}
