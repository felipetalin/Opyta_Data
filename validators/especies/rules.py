"""
validators/especies/rules.py
Regras de validação aplicadas sobre o DataFrame já normalizado.
Não acessa o banco de dados (isso é responsabilidade de db_checker).
"""
from __future__ import annotations

import re

import pandas as pd

from .normalizer import normalized_key
from .report import ValidationIssue, ValidationReport

# Colunas obrigatórias com valor não-nulo
_REQUIRED_VALUE_COLS = ["Nome_Cientifico", "Grupo_Biologico"]

# Exceções de regras por grupo biológico
# Alguns grupos têm táxons especiais que não seguem a nomenclatura científica padrão
_EXCEPTIONS_BY_GROUP: dict[str, dict[str, list[str]]] = {
    "Zooplâncton": {
        # Táxons que podem ter gênero "N.A." ou não followem padrão
        "skip_genus_check": {
            "Ciliado ni",
            "Ciliado NI",
            "Cyclopoida (nauplius)",
            "Cyclopoida (copepodito)",
            "Calanoida (nauplius)",
            "Calanoida (copepodito)",
            "Bdelloida",
            "Bdelloida sp.",
        },
        # Táxons que são nomes únicos válidos (não geram warning)
        "single_word_valid": {
            "Bdelloida",
        },
    },
    "Bentos": {
        "skip_genus_check": {
            "Mitilideo sp.",
        },
    },
}


def _is_exception_for_group(
    taxa_name: str,
    group: str,
    exception_type: str,
) -> bool:
    """
    Verifica se um táxon tem exceção de regra para seu grupo.

    exception_type pode ser:
    - "skip_genus_check": pula validação de coerência de gênero
    - "single_word_valid": aceita nomes com uma única palavra
    """
    if group not in _EXCEPTIONS_BY_GROUP:
        return False

    exceptions = _EXCEPTIONS_BY_GROUP[group].get(exception_type, set())
    taxa_norm = normalized_key(taxa_name)

    for exc in exceptions:
        if normalized_key(exc) == taxa_norm:
            return True

    return False


def run_rules(df: pd.DataFrame, report: ValidationReport) -> None:
    """
    Executa todas as regras de validação sobre o DataFrame normalizado.
    Registra problemas diretamente em `report.issues`.
    Não modifica o DataFrame.
    """
    _check_required_values(df, report)
    _check_scientific_name_format(df, report)
    _check_genus_coherence(df, report)
    _check_bmwp_score(df, report)
    _check_intra_sheet_duplicates(df, report)


# ---------------------------------------------------------------------------
# Regra 1: valores obrigatórios presentes
# ---------------------------------------------------------------------------

def _check_required_values(df: pd.DataFrame, report: ValidationReport) -> None:
    for col in _REQUIRED_VALUE_COLS:
        if col not in df.columns:
            continue  # ausência de coluna já foi reportada por reader.py
        empty_rows = df.index[df[col].isna() | (df[col].str.strip() == "")].tolist()
        for idx in empty_rows:
            report.issues.append(
                ValidationIssue(
                    code="MISSING_REQUIRED_VALUE",
                    severity="block",
                    message=f"Coluna '{col}' sem valor na linha {idx + 2}.",
                    row=idx + 2,
                    column=col,
                )
            )


# ---------------------------------------------------------------------------
# Regra 2: formato básico do Nome_Cientifico
# ---------------------------------------------------------------------------

_SINGLE_WORD = re.compile(r"^\S+$")

def _check_scientific_name_format(df: pd.DataFrame, report: ValidationReport) -> None:
    if "Nome_Cientifico" not in df.columns:
        return

    grupo_col = "Grupo_Biologico" if "Grupo_Biologico" in df.columns else None

    for idx, value in df["Nome_Cientifico"].items():
        if pd.isna(value) or not isinstance(value, str) or value.strip() == "":
            continue  # já tratado por _check_required_values

        grupo = df.at[idx, grupo_col] if grupo_col else None
        grupo_str = str(grupo).strip() if pd.notna(grupo) else ""

        # Verificar se é exceção para este grupo
        if _is_exception_for_group(value, grupo_str, "single_word_valid"):
            continue

        # Nome com uma única palavra que não é marcador taxonômico nem família
        if _SINGLE_WORD.match(value.strip()):
            token = value.strip().lower()
            # Família ou ordem geralmente termina em -idae, -inae, -aceae, -ale ...
            is_family_level = bool(re.search(r"(idae|inae|aceae|ales|formes|iformes)$", token))
            if not is_family_level:
                report.issues.append(
                    ValidationIssue(
                        code="SINGLE_WORD_NAME",
                        severity="warning",
                        message=(
                            f"Nome científico com uma única palavra na linha {idx + 2}: "
                            f"'{value}'. Esperado formato 'Gênero epiteto' ou 'Gênero sp.'."
                        ),
                        row=idx + 2,
                        column="Nome_Cientifico",
                    )
                )


# ---------------------------------------------------------------------------
# Regra 3: coerência entre Genero e a primeira palavra de Nome_Cientifico
# ---------------------------------------------------------------------------

def _check_genus_coherence(df: pd.DataFrame, report: ValidationReport) -> None:
    if "Genero" not in df.columns or "Nome_Cientifico" not in df.columns:
        return

    grupo_col = "Grupo_Biologico" if "Grupo_Biologico" in df.columns else None

    for idx, row in df.iterrows():
        genus_val = row.get("Genero")
        name_val = row.get("Nome_Cientifico")
        grupo = row.get(grupo_col) if grupo_col else None
        grupo_str = str(grupo).strip() if pd.notna(grupo) else ""

        if pd.isna(genus_val) or not isinstance(genus_val, str) or genus_val.strip() == "":
            continue
        if pd.isna(name_val) or not isinstance(name_val, str) or name_val.strip() == "":
            continue

        # Verificar se é exceção para este grupo
        if _is_exception_for_group(name_val, grupo_str, "skip_genus_check"):
            continue

        genus_norm = normalized_key(genus_val)
        first_word_norm = normalized_key(name_val.split()[0])

        if genus_norm != first_word_norm:
            report.issues.append(
                ValidationIssue(
                    code="GENUS_MISMATCH",
                    severity="warning",
                    message=(
                        f"Linha {idx + 2}: 'Genero' = '{genus_val}' não coincide "
                        f"com a primeira palavra de 'Nome_Cientifico' = '{name_val}'."
                    ),
                    row=idx + 2,
                    column="Genero",
                )
            )


# ---------------------------------------------------------------------------
# Regra 4: BMWP_Score deve ser numérico quando preenchido
# ---------------------------------------------------------------------------

def _check_bmwp_score(df: pd.DataFrame, report: ValidationReport) -> None:
    if "BMWP_Score" not in df.columns:
        return

    for idx, value in df["BMWP_Score"].items():
        if pd.isna(value) or (isinstance(value, str) and value.strip() == ""):
            continue
        try:
            float(str(value).strip())
        except (ValueError, TypeError):
            report.issues.append(
                ValidationIssue(
                    code="INVALID_BMWP_SCORE",
                    severity="warning",
                    message=(
                        f"Linha {idx + 2}: 'BMWP_Score' = '{value}' não é numérico."
                    ),
                    row=idx + 2,
                    column="BMWP_Score",
                )
            )


# ---------------------------------------------------------------------------
# Regra 5: duplicatas dentro da própria planilha
# ---------------------------------------------------------------------------

def _check_intra_sheet_duplicates(df: pd.DataFrame, report: ValidationReport) -> None:
    if "Nome_Cientifico" not in df.columns:
        return

    keys_seen: dict[str, int] = {}  # chave normalizada → primeiro índice (linha Excel)

    for idx, value in df["Nome_Cientifico"].items():
        if pd.isna(value) or not isinstance(value, str) or value.strip() == "":
            continue

        key = normalized_key(value)
        excel_row = idx + 2

        if key in keys_seen:
            original_row = keys_seen[key]
            # Duplicatas intra-planilha são warnings, não bloqueios
            # O cadastro fará upsert por nome científico, então duplicatas são tratadas
            report.issues.append(
                ValidationIssue(
                    code="INTRA_SHEET_DUPLICATE",
                    severity="warning",
                    message=(
                        f"Linha {excel_row}: '{value}' duplica o registro "
                        f"da linha {original_row}. Será consolidado no cadastro (upsert)."
                    ),
                    row=excel_row,
                    column="Nome_Cientifico",
                )
            )
        else:
            keys_seen[key] = excel_row
