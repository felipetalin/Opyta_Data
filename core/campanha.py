"""
core/campanha.py
================

Normalização de nomes de campanha para o padrão canônico

    C{NNN}-{YYYY}-{MM}-{SC|CH}

portado da evolução consolidada no Opyta_Data_Analysis (ver
`/memories/repo/arquitetura_dual_track.md`).

Motivação
---------
A campanha `28a-abr-26-SC` gerou a duplicata `28ª-abr-26-SC` por ambiguidade do
ordinal (ASCII ``a`` vs Unicode ``ª``). O padrão canônico elimina o ordinal e
qualquer caractere especial, garantindo ordenação correta (``028`` antes de
``100``) e unicidade:

- ``C``        — prefixo fixo (evita começar com número)
- ``NNN``      — número da campanha, zero-padded em 3 dígitos
- ``YYYY-MM``  — ano-mês ISO
- ``SC`` | ``CH`` — Seca | Chuva, sempre MAIÚSCULO

Exemplo
-------
>>> normalizar_nome_campanha("28ª-abr-26-SC")
'C028-2026-04-SC'
>>> normalizar_nome_campanha("10a-Mai-23")
'C010-2023-05-??'

Esta é uma **utilidade pura** (não acessa banco, não altera dados). Pode ser
usada nos fluxos de cadastro/migração/consolidação para padronizar nomes de
campanha antes da persistência, evitando duplicatas na origem.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

CANONICAL_RE = re.compile(r"^C\d{3}-\d{4}-\d{2}-(SC|CH)$")

_MESES = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
    # variações comuns
    "janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5,
    "junho": 6, "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}

# Marcador usado quando um componente não pôde ser inferido.
DESCONHECIDO = "??"


@dataclass(frozen=True)
class CampanhaParts:
    """Componentes extraídos de um nome de campanha (None = não inferido)."""
    numero: int | None
    ano: int | None
    mes: int | None
    estacao: str | None  # "SC" | "CH" | None

    @property
    def canonico(self) -> str:
        num = f"{self.numero:03d}" if self.numero is not None else "???"
        ano = f"{self.ano:04d}" if self.ano is not None else "????"
        mes = f"{self.mes:02d}" if self.mes is not None else "??"
        est = self.estacao or DESCONHECIDO
        return f"C{num}-{ano}-{mes}-{est}"

    @property
    def completo(self) -> bool:
        return None not in (self.numero, self.ano, self.mes) and self.estacao is not None


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _norm(value: object) -> str:
    return _strip_accents(str(value or "")).lower().strip()


def _inferir_ano(token: str) -> int | None:
    if not token.isdigit():
        return None
    val = int(token)
    if len(token) == 4:
        return val
    if len(token) == 2:
        # 00-69 -> 2000-2069 ; 70-99 -> 1970-1999
        return 2000 + val if val <= 69 else 1900 + val
    return None


def _inferir_estacao(texto: str) -> str | None:
    t = _norm(texto)
    if re.search(r"\bsc\b", t) or "seca" in t:
        return "SC"
    if re.search(r"\bch\b", t) or "chuva" in t or "chuvosa" in t:
        return "CH"
    return None


def parse_campanha(nome: str) -> CampanhaParts:
    """
    Extrai (numero, ano, mes, estacao) de um nome de campanha bagunçado.

    Aceita formatos como:
        "28a-abr-26-SC", "28ª-abr-26-SC", "10a-Mai-23",
        "C028-2026-04-SC", "Campanha 28 - Abril/2026 - Seca".
    Componentes não inferidos voltam como None.
    """
    bruto = str(nome or "")

    # Já está no padrão canônico?
    if CANONICAL_RE.match(bruto.strip().upper()):
        up = bruto.strip().upper()
        return CampanhaParts(
            numero=int(up[1:4]),
            ano=int(up[5:9]),
            mes=int(up[10:12]),
            estacao=up[13:15],
        )

    texto = _norm(bruto)

    # Estação
    estacao = _inferir_estacao(texto)

    # Mês (nome textual)
    mes: int | None = None
    for token in re.findall(r"[a-z]+", texto):
        if token in _MESES:
            mes = _MESES[token]
            break

    # Números presentes (campanha, mês-numérico, ano)
    numeros = re.findall(r"\d+", texto)

    numero: int | None = None
    ano: int | None = None

    # Número da campanha: primeiro grupo de dígitos (frequentemente com ordinal a/ª).
    if numeros:
        numero = int(numeros[0])

    # Ano: procura um token de 4 dígitos; senão um de 2 dígitos que não seja o número da campanha.
    for token in numeros:
        if len(token) == 4:
            ano = _inferir_ano(token)
            break
    if ano is None:
        for token in numeros[1:]:
            if len(token) == 2:
                ano = _inferir_ano(token)
                break

    # Mês numérico (ex.: "2026-04") se não havia nome de mês.
    if mes is None:
        # procura padrão YYYY-MM ou MM isolado plausível
        m = re.search(r"\b(\d{4})[-/](\d{1,2})\b", texto)
        if m:
            ano = ano or _inferir_ano(m.group(1))
            cand = int(m.group(2))
            if 1 <= cand <= 12:
                mes = cand

    return CampanhaParts(numero=numero, ano=ano, mes=mes, estacao=estacao)


def normalizar_nome_campanha(nome: str, *, strict: bool = False) -> str:
    """
    Converte um nome de campanha para o padrão canônico ``C{NNN}-{YYYY}-{MM}-{SC|CH}``.

    Componentes não inferidos aparecem como ``?`` (ex.: ``C010-2023-05-??``).

    Parameters
    ----------
    nome : str
        Nome bruto da campanha.
    strict : bool
        Se True, levanta ``ValueError`` quando algum componente não pôde ser
        inferido (útil para validação na origem). Default False.

    Returns
    -------
    str
        Nome canônico.
    """
    parts = parse_campanha(nome)
    canonico = parts.canonico
    if strict and not parts.completo:
        raise ValueError(
            f"Não foi possível normalizar a campanha {nome!r}: resultado parcial {canonico!r}"
        )
    return canonico


def is_canonico(nome: str) -> bool:
    """True se o nome já está no padrão canônico válido."""
    return bool(CANONICAL_RE.match(str(nome or "").strip().upper()))
