from __future__ import annotations

from io import BytesIO
from typing import Any

import streamlit as st


def _new_workbook() -> Any:
    from openpyxl import Workbook

    return Workbook()


IMPORT_TEMPLATE_SPECS: dict[str, dict[str, object]] = {
    "Ictiofauna": {"filename": "modelo_oficial_ictiofauna_v1_0.xlsx", "result_sheet": "Resultados_Ictiofauna", "result_headers": ["Campanha", "Ponto", "Nome_Cientifico", "Quantidade", "Biomassa", "Comprimento", "Observacoes"]},
    "Bentos": {"filename": "modelo_oficial_bentos_v1_0.xlsx", "result_sheet": "Resultados_Zoobentos", "result_headers": ["Campanha", "Ponto", "Nome_Cientifico", "Quantidade", "BMWP_Score", "Ordem", "Observacoes"]},
    "Fitoplâncton": {"filename": "modelo_oficial_fitoplancton_v1_0.xlsx", "result_sheet": "Resultados_Fitoplancton", "result_headers": ["Campanha", "Ponto", "Nome_Cientifico", "Densidade", "Biovolume", "Observacoes"]},
    "Zooplâncton": {"filename": "modelo_oficial_zooplancton_v1_0.xlsx", "result_sheet": "Resultados_Zooplancton", "result_headers": ["Campanha", "Ponto", "Nome_Cientifico", "Quantidade", "Volume_Filtrado", "Observacoes"]},
    "Meio Físico": {"filename": "modelo_oficial_meio_fisico_v1_0.xlsx", "result_sheet": "Resultados_Meio_Fisico", "result_headers": ["Campanha", "Ponto", "Parametro", "Valor_Medido", "Unidade_Medida", "Data_Coleta", "Observacoes"]},
    "Avifauna": {"filename": "modelo_oficial_avifauna_v1_0.xlsx", "result_sheet": "Resultados_Avifauna", "result_headers": ["Campanha", "Ponto", "Nome_Cientifico", "Quantidade", "Metodo_Amostragem", "Observacoes"]},
    "Herpetofauna": {"filename": "modelo_oficial_herpetofauna_v1_0.xlsx", "result_sheet": "Resultados_Herpetofauna", "result_headers": ["Campanha", "Ponto", "Nome_Cientifico", "Quantidade", "Metodo_Amostragem", "Observacoes"]},
    "Mastofauna": {"filename": "modelo_oficial_mastofauna_v1_0.xlsx", "result_sheet": "Resultados_Mastofauna", "result_headers": ["Campanha", "Ponto", "Nome_Cientifico", "Quantidade", "Metodo_Amostragem", "Observacoes"]},
}

MASTER_SPECIES_SHEETS: dict[str, list[str]] = {
    "Especies": ["Nome_Cientifico", "Nome_Popular", "Grupo_Biologico", "Reino", "Filo", "Classe", "Ordem", "Familia", "Genero", "Autor_e_Ano", "Status_Ameaca_Nacional", "Status_Ameaca_Global", "Origem", "Habito_Alimentar", "Estrategia_Reprodutiva", "Valor_Economico", "Observacoes", "BMWP_Score", "Status_Estadual", "Status_Copam", "Cites", "Guilda_Alimentar", "Dependencia_Florestal", "Endemismo", "Sensibilidade_Ambiental", "Migratorio", "Raridade"],
    "Bacias_Hidrograficas": ["Nome_Bacia"],
    "Biomas": ["Nome_Bioma"],
    "Endemismo": ["Nome_Cientifico", "Tipo_de_Regiao", "Nome_da_Regiao"],
}

MASTER_PARAMETROS_SHEETS: dict[str, list[str]] = {
    "Aguas_Superficiais": ["Parametro", "Unidade_Medida", "VMP_357_CL1_Min", "VMP_357_CL1_Max", "VMP_357_CL2_Min", "VMP_357_CL2_Max", "VMP_396_Consumo_Humano", "VMP_396_Dessedentacao_Animal", "VMP_396_Irrigacao", "VMP_396_Recreacao", "VMP_454_N1", "VMP_454_N2", "VMP_430_Padrao"],
    "Aguas_Subterraneas": ["Parametro", "Unidade_Medida", "VMP_357_CL1_Min", "VMP_357_CL1_Max", "VMP_357_CL2_Min", "VMP_357_CL2_Max", "VMP_396_Consumo_Humano", "VMP_396_Dessedentacao_Animal", "VMP_396_Irrigacao", "VMP_396_Recreacao", "VMP_454_N1", "VMP_454_N2", "VMP_430_Padrao"],
    "Sedimento": ["Parametro", "Unidade_Medida", "VMP_357_CL1_Min", "VMP_357_CL1_Max", "VMP_357_CL2_Min", "VMP_357_CL2_Max", "VMP_396_Consumo_Humano", "VMP_396_Dessedentacao_Animal", "VMP_396_Irrigacao", "VMP_396_Recreacao", "VMP_454_N1", "VMP_454_N2", "VMP_430_Padrao"],
    "Efluentes": ["Parametro", "Unidade_Medida", "VMP_357_CL1_Min", "VMP_357_CL1_Max", "VMP_357_CL2_Min", "VMP_357_CL2_Max", "VMP_396_Consumo_Humano", "VMP_396_Dessedentacao_Animal", "VMP_396_Irrigacao", "VMP_396_Recreacao", "VMP_454_N1", "VMP_454_N2", "VMP_430_Padrao"],
}


def _apply_header_style(cell) -> None:
    from openpyxl.styles import Font, PatternFill

    cell.font = Font(bold=True, color="FFFFFF")
    cell.fill = PatternFill(fill_type="solid", fgColor="1F4E78")


def _style_sheet_headers(sheet) -> None:
    for cell in sheet[1]:
        _apply_header_style(cell)
    sheet.freeze_panes = "A2"
    for column_cells in sheet.columns:
        max_length = max(len(str(cell.value or "")) for cell in column_cells)
        sheet.column_dimensions[column_cells[0].column_letter].width = min(max_length + 4, 36)


def _append_dictionary_sheet(workbook: Any, rows: list[tuple[str, str, str, str]]) -> None:
    ws = workbook.create_sheet("Dicionario")
    ws.append(["Aba", "Coluna", "Obrigatoria", "Cuidado"])
    for row in rows:
        ws.append(list(row))
    _style_sheet_headers(ws)


def _append_import_readme(workbook: Any, group: str, result_sheet: str) -> None:
    ws = workbook.active
    ws.title = "Leia-me"
    lines = [
        ("A1", "Modelo oficial Opyta - Importação"),
        ("A2", f"Grupo: {group}"),
        ("A4", "Cuidados obrigatórios"),
        ("A5", "1. Não renomeie abas nem colunas."),
        ("A6", "2. Preencha apenas a partir da linha 2; mantenha a linha 1 como cabeçalho."),
        ("A7", "3. Capa_Projeto, Pontos_e_Campanhas e aba de Resultados não podem ficar vazias."),
        ("A8", f"4. A aba de resultados esperada para este grupo é '{result_sheet}'."),
        ("A9", "5. Use Campanha e Ponto exatamente como cadastrados na planilha."),
        ("A10", "6. Coordenadas devem estar em decimal; revise antes do upload."),
        ("A11", "7. A aba Metadados_Esforco deve existir para grupos biológicos."),
    ]
    for cell, value in lines:
        ws[cell] = value
    ws.column_dimensions["A"].width = 100


def _append_base_species_readme(workbook: Any) -> None:
    ws = workbook.active
    ws.title = "Leia-me"
    lines = [
        ("A1", "Modelo oficial Opyta - Cadastro Mestre de Espécies"),
        ("A3", "Cuidados obrigatórios"),
        ("A4", "1. A aba 'Especies' deve existir com os nomes de colunas exatamente como no modelo."),
        ("A5", "2. Colunas mínimas obrigatórias: Nome_Cientifico e Grupo_Biologico."),
        ("A6", "3. As abas Bacias_Hidrograficas, Biomas e Endemismo são opcionais, mas se usadas devem manter os cabeçalhos do modelo."),
        ("A7", "4. Não insira linhas de título extras acima do cabeçalho."),
        ("A8", "5. Status_Ameaca_Estadual pode ser enviado como Status_Estadual; o sistema normaliza esse alias."),
        ("A9", "6. BMWP_Score é opcional e útil principalmente para grupos aquáticos."),
        ("A10", "7. Endemismo aceita apenas Tipo_de_Regiao = 'Bacia Hidrográfica' ou 'Bioma'."),
    ]
    for cell, value in lines:
        ws[cell] = value
    ws.column_dimensions["A"].width = 110


def _append_base_parametros_readme(workbook: Any) -> None:
    ws = workbook.active
    ws.title = "Leia-me"
    lines = [
        ("A1", "Modelo oficial Opyta - Cadastro Mestre de Parâmetros"),
        ("A3", "Cuidados obrigatórios"),
        ("A4", "1. Não renomeie as abas: Aguas_Superficiais, Aguas_Subterraneas, Sedimento e Efluentes."),
        ("A5", "2. A coluna obrigatória em todas as abas é Parametro."),
        ("A6", "3. Valores de VMP podem ficar vazios quando não se aplicarem ao parâmetro."),
        ("A7", "4. Use ponto ou vírgula decimal; o script converte ambos."),
        ("A8", "5. Não use textos como 'N.A.' ou '-' quando puder deixar em branco."),
        ("A9", "6. Unidade_Medida deve ser preenchida sempre que existir unidade técnica definida."),
    ]
    for cell, value in lines:
        ws[cell] = value
    ws.column_dimensions["A"].width = 110


@st.cache_data(show_spinner=False)
def build_group_template_bytes(group: str) -> bytes:
    spec = IMPORT_TEMPLATE_SPECS[group]
    workbook = _new_workbook()
    _append_import_readme(workbook, group, str(spec["result_sheet"]))

    dictionary_rows: list[tuple[str, str, str, str]] = []

    ws_capa = workbook.create_sheet("Capa_Projeto")
    ws_capa.append(["Codigo_Opyta", "Nome_Projeto", "Responsavel", "Versao_Modelo"])
    _style_sheet_headers(ws_capa)
    dictionary_rows.extend([
        ("Capa_Projeto", "Codigo_Opyta", "Sim", "Código do projeto exatamente como usado no sistema."),
        ("Capa_Projeto", "Nome_Projeto", "Não", "Nome amigável do projeto."),
        ("Capa_Projeto", "Responsavel", "Não", "Responsável pelo preenchimento."),
        ("Capa_Projeto", "Versao_Modelo", "Não", "Versão do modelo utilizado."),
    ])

    ws_pontos = workbook.create_sheet("Pontos_e_Campanhas")
    ws_pontos.append(["Campanha", "Ponto", "Latitude", "Longitude", "Data_Coleta"])
    _style_sheet_headers(ws_pontos)
    dictionary_rows.extend([
        ("Pontos_e_Campanhas", "Campanha", "Sim", "Identificador da campanha."),
        ("Pontos_e_Campanhas", "Ponto", "Sim", "Código do ponto de amostragem."),
        ("Pontos_e_Campanhas", "Latitude", "Não", "Latitude decimal."),
        ("Pontos_e_Campanhas", "Longitude", "Não", "Longitude decimal."),
        ("Pontos_e_Campanhas", "Data_Coleta", "Não", "Data principal da coleta."),
    ])

    if group != "Meio Físico":
        ws_esforco = workbook.create_sheet("Metadados_Esforco")
        ws_esforco.append(["Campanha", "Ponto", "Metodo_Amostragem", "Esforco_Valor", "Esforco_Unidade", "Observacoes"])
        _style_sheet_headers(ws_esforco)
        dictionary_rows.extend([
            ("Metadados_Esforco", "Campanha", "Não", "Campanha a que o esforço se refere."),
            ("Metadados_Esforco", "Ponto", "Não", "Ponto a que o esforço se refere."),
            ("Metadados_Esforco", "Metodo_Amostragem", "Não", "Método usado na campanha."),
            ("Metadados_Esforco", "Esforco_Valor", "Não", "Valor do esforço amostral."),
            ("Metadados_Esforco", "Esforco_Unidade", "Não", "Unidade do esforço."),
            ("Metadados_Esforco", "Observacoes", "Não", "Notas complementares."),
        ])

    ws_resultados = workbook.create_sheet(str(spec["result_sheet"]))
    result_headers = list(spec["result_headers"])
    ws_resultados.append(result_headers)
    _style_sheet_headers(ws_resultados)
    for column in result_headers:
        required = "Sim" if column in {"Campanha", "Ponto"} else "Não"
        care = "Preencha exatamente como coletado." if required == "Sim" else "Opcional conforme o grupo e o dado disponível."
        dictionary_rows.append((str(spec["result_sheet"]), column, required, care))

    _append_dictionary_sheet(workbook, dictionary_rows)
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def get_group_template_filename(group: str) -> str:
    return str(IMPORT_TEMPLATE_SPECS[group]["filename"])


@st.cache_data(show_spinner=False)
def build_master_species_template_bytes() -> bytes:
    workbook = _new_workbook()
    _append_base_species_readme(workbook)
    dictionary_rows: list[tuple[str, str, str, str]] = []
    required_species = {"Nome_Cientifico", "Grupo_Biologico"}

    for sheet_name, headers in MASTER_SPECIES_SHEETS.items():
        ws = workbook.create_sheet(sheet_name)
        ws.append(headers)
        _style_sheet_headers(ws)
        for header in headers:
            required = "Sim" if (sheet_name == "Especies" and header in required_species) else "Não"
            if sheet_name == "Endemismo" and header == "Tipo_de_Regiao":
                care = "Use apenas 'Bacia Hidrográfica' ou 'Bioma'."
            elif sheet_name == "Bacias_Hidrograficas":
                care = "Uma bacia por linha, sem duplicidade."
            elif sheet_name == "Biomas":
                care = "Um bioma por linha, sem duplicidade."
            else:
                care = "Obrigatório para o cadastro funcionar." if required == "Sim" else "Preencha somente se tiver o dado confirmado."
            dictionary_rows.append((sheet_name, header, required, care))

    _append_dictionary_sheet(workbook, dictionary_rows)
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def get_master_species_template_filename() -> str:
    return "cadastro_especies_opyta.xlsx"


@st.cache_data(show_spinner=False)
def build_master_parameters_template_bytes() -> bytes:
    workbook = _new_workbook()
    _append_base_parametros_readme(workbook)
    dictionary_rows: list[tuple[str, str, str, str]] = []

    for sheet_name, headers in MASTER_PARAMETROS_SHEETS.items():
        ws = workbook.create_sheet(sheet_name)
        ws.append(headers)
        _style_sheet_headers(ws)
        for header in headers:
            required = "Sim" if header == "Parametro" else "Não"
            if header == "Parametro":
                care = "Nome técnico do parâmetro exatamente como será cadastrado."
            elif header == "Unidade_Medida":
                care = "Ex.: mg/L, uS/cm, NTU."
            else:
                care = "Pode ficar em branco quando não houver limite aplicável."
            dictionary_rows.append((sheet_name, header, required, care))

    _append_dictionary_sheet(workbook, dictionary_rows)
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def get_master_parameters_template_filename() -> str:
    return "cadastro_parametros_opyta.xlsx"