from dataclasses import dataclass

@dataclass(frozen=True)
class ActionSpec:
    key: str
    label: str
    script: str
    expected_excel_name: str | None


ACTIONS = {
    # BASE MESTRE
    "BASE_ESPECIES": ActionSpec(
        key="BASE_ESPECIES",
        label="Cadastrar Espécies",
        script="scripts/cadastrar_especies.py",
        expected_excel_name="cadastro_especies_opyta.xlsx",
    ),
    "BASE_PARAMETROS": ActionSpec(
        key="BASE_PARAMETROS",
        label="Cadastrar Parâmetros",
        script="scripts/cadastrar_parametros.py",
        expected_excel_name="cadastro_parametros_opyta.xlsx",
    ),

    # MIGRAÇÕES (ajuste expected_excel_name conforme cada script procura)
    "MIGRAR_ICTIO": ActionSpec(
        key="MIGRAR_ICTIO",
        label="Migrar Ictiofauna",
        script="scripts/migrar_ictiofauna.py",
        expected_excel_name="projeto_ictio_real.xlsx",
    ),
    "MIGRAR_BENTOS": ActionSpec(
        key="MIGRAR_BENTOS",
        label="Migrar Bentos",
        script="scripts/migrar_bentos.py",
        expected_excel_name="projeto_bentos.xlsx",
    ),
    "MIGRAR_FITOPL": ActionSpec(
        key="MIGRAR_FITOPL",
        label="Migrar Fitoplâncton",
        script="scripts/migrar_fitoplancton.py",
        expected_excel_name="projeto_fitoplancton.xlsx",
    ),
    "MIGRAR_ZOOPL": ActionSpec(
        key="MIGRAR_ZOOPL",
        label="Migrar Zooplâncton",
        script="scripts/migrar_zooplancton.py",
        expected_excel_name="projeto_zooplancton.xlsx",
    ),
    "MIGRAR_FISICO": ActionSpec(
        key="MIGRAR_FISICO",
        label="Migrar Meio Físico",
        script="scripts/migrar_meio_fisico.py",
        expected_excel_name="Resultados_Meio_Fisico.xlsx",
    ),

    # CONSOLIDAÇÃO
    "CONSOLIDAR": ActionSpec(
        key="CONSOLIDAR",
        label="Consolidar",
        script="scripts/processar_dados.py",
        expected_excel_name=None,
    ),
}


GROUP_TO_ACTION_KEY = {
    "Ictiofauna": "MIGRAR_ICTIO",
    "Bentos": "MIGRAR_BENTOS",
    "Fitoplâncton": "MIGRAR_FITOPL",
    "Zooplâncton": "MIGRAR_ZOOPL",
    "Meio Físico": "MIGRAR_FISICO",
}