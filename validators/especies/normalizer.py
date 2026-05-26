"""
validators/especies/normalizer.py
Aplica todas as correções automáticas de texto sobre o DataFrame.
Não altera nomes de colunas; opera apenas sobre os *valores* das células.
"""
from __future__ import annotations

import re
import unicodedata

import pandas as pd

from .report import CorrectionDetail, ValidationReport

# ---------------------------------------------------------------------------
# Colunas de texto livre (aplica limpeza completa)
# ---------------------------------------------------------------------------
TEXT_COLUMNS = [
    "Nome_Cientifico",
    "Nome_Popular",
    "Grupo_Biologico",
    "Reino",
    "Filo",
    "Classe",
    "Ordem",
    "Familia",
    "Genero",
    "Autor_e_Ano",
    "Status_Ameaca_Nacional",
    "Status_Ameaca_Global",
    "Origem",
    "Habito_Alimentar",
    "Estrategia_Reprodutiva",
    "Valor_Economico",
    "Cinegetica",
    "Xerimbabo",
    "Observacoes",
    "Status_Estadual",
    "Status_Copam",
    "Cites",
    "Guilda_Alimentar",
    "Dependencia_Florestal",
    "Endemismo",
    "Sensibilidade_Ambiental",
    "Migratorio",
    "Raridade",
]

# Colunas que recebem Title Case
TITLE_CASE_COLUMNS = {
    "Grupo_Biologico",
    "Reino",
    "Filo",
    "Classe",
    "Ordem",
    "Familia",
    "Genero",
    "Origem",
    "Valor_Economico",
    "Cinegetica",
    "Xerimbabo",
    "Status_Estadual",
    "Status_Copam",
    "Cites",
    "Guilda_Alimentar",
    "Dependencia_Florestal",
    "Endemismo",
    "Sensibilidade_Ambiental",
    "Migratorio",
    "Raridade",
}

# Padrão para marcadores de identificação incerta na nomenclatura científica
_SP_PATTERN = re.compile(r"\bsp\b(?!\.)", re.IGNORECASE)
_CF_PATTERN = re.compile(r"\bcf\b(?!\.)", re.IGNORECASE)
_AFF_PATTERN = re.compile(r"\baff\b(?!\.)", re.IGNORECASE)

# Caracteres invisíveis que devem ser removidos
_INVISIBLE_CHARS = re.compile(
    r"[\u00ad\u200b\u200c\u200d\u200e\u200f\u202a-\u202e\u2060\ufeff]"
)


# ---------------------------------------------------------------------------
# Funções de limpeza atômica
# ---------------------------------------------------------------------------

def _clean_unicode(value: str) -> str:
    """Normaliza forma Unicode (NFKC) e remove caracteres invisíveis."""
    value = unicodedata.normalize("NFKC", value)
    value = _INVISIBLE_CHARS.sub("", value)
    return value


def _clean_whitespace(value: str) -> str:
    """Remove espaços nas bordas e colapsa múltiplos espaços internos."""
    value = value.strip()
    value = re.sub(r"[ \t]+", " ", value)
    return value


def _normalize_scientific_markers(value: str) -> str:
    """
    Padroniza marcadores taxonômicos incompletos:
      sp  → sp.
      cf  → cf.
      aff → aff.
    Respeitando os que já têm ponto.
    """
    value = _SP_PATTERN.sub("sp.", value)
    value = _CF_PATTERN.sub("cf.", value)
    value = _AFF_PATTERN.sub("aff.", value)
    return value


def _normalize_scientific_name_case(value: str) -> str:
    """
    Regra de maiúsculas/minúsculas para Nome_Cientifico:
    - Primeira palavra (gênero): capitalizada (Primeira Letra Maiúscula)
    - Demais palavras: minúsculas (já que autores, cf., sp. ficam lowercase)
    - Tokens como 'sp.', 'cf.', 'aff.' preservam a forma pós-normalização.
    """
    tokens = value.split()
    if not tokens:
        return value
    result = [tokens[0].capitalize()]
    for tok in tokens[1:]:
        result.append(tok.lower())
    return " ".join(result)


# ---------------------------------------------------------------------------
# Normalizador principal
# ---------------------------------------------------------------------------

def normalize(
    df: pd.DataFrame,
    report: ValidationReport,
) -> pd.DataFrame:
    """
    Aplica todas as correções automáticas. Retorna um DataFrame novo
    com os valores limpos. Registra cada alteração em `report.corrections`.
    """
    df = df.copy()

    for col in TEXT_COLUMNS:
        if col not in df.columns:
            continue

        for idx, raw_value in df[col].items():
            if pd.isna(raw_value) or not isinstance(raw_value, str):
                continue

            original = raw_value
            value = original

            # --- Passo 1: unicode + invisíveis
            value = _clean_unicode(value)

            # --- Passo 2: whitespace
            value = _clean_whitespace(value)

            # --- Passo 3: marcadores taxonômicos (só em Nome_Cientifico)
            if col == "Nome_Cientifico":
                value = _normalize_scientific_markers(value)

            # --- Passo 4: capitalização
            if col == "Nome_Cientifico":
                value = _normalize_scientific_name_case(value)
            elif col in TITLE_CASE_COLUMNS:
                value = value.title()

            # --- Registrar correção se houve mudança
            if value != original:
                report.corrections.append(
                    CorrectionDetail(
                        row=idx + 2,  # +2 porque linha 1 = cabeçalho no Excel
                        column=col,
                        original=original,
                        corrected=value,
                        reason=_describe_correction(original, value, col),
                    )
                )
                df.at[idx, col] = value

    return df


def _describe_correction(original: str, corrected: str, col: str) -> str:
    """Produz uma descrição legível do motivo da correção."""
    reasons = []

    if original.strip() != original:
        reasons.append("espaços nas bordas removidos")

    if re.search(r"[ \t]{2,}", original):
        reasons.append("espaços duplos colapsados")

    if unicodedata.normalize("NFKC", original) != original or _INVISIBLE_CHARS.search(original):
        reasons.append("unicode normalizado / caracteres invisíveis removidos")

    if col == "Nome_Cientifico":
        if _SP_PATTERN.search(original) or _CF_PATTERN.search(original) or _AFF_PATTERN.search(original):
            reasons.append("marcador taxonômico padronizado (sp./cf./aff.)")
        if original.split()[0] != corrected.split()[0] if original.split() and corrected.split() else False:
            reasons.append("gênero capitalizado")

    if col in TITLE_CASE_COLUMNS and original != corrected:
        reasons.append("maiúsculas/minúsculas padronizadas")

    return "; ".join(reasons) if reasons else "valor normalizado"


# ---------------------------------------------------------------------------
# Chave de comparação normalizada (uso externo: rules, db_checker)
# ---------------------------------------------------------------------------

def normalized_key(value: str | None) -> str:
    """
    Retorna a chave canônica de um nome científico para comparação
    de duplicatas e busca no banco.
    """
    if not value or not isinstance(value, str):
        return ""
    v = unicodedata.normalize("NFKC", value.strip().lower())
    v = _INVISIBLE_CHARS.sub("", v)
    v = re.sub(r"[ \t]+", " ", v)
    return v
