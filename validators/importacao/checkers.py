"""
validators/importacao/checkers.py
Validações específicas: pontos, espécies, esforço, referências cruzadas.
"""
from __future__ import annotations

import re
import unicodedata

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from .report import ValidationIssue, ValidationReport


def _norm_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text_value = str(value).replace("\u00A0", " ")
    text_value = unicodedata.normalize("NFKC", text_value)
    text_value = re.sub(r"\s+", " ", text_value).strip().lower()
    return text_value


def _campaign_code(value: object) -> str:
    """Retorna o codigo operacional da campanha, ex.: C008-2013-08 -> c008."""
    text_value = _norm_text(value)
    match = re.match(r"^(c\d+)", text_value)
    return match.group(1) if match else text_value


def _find_column(df: pd.DataFrame, predicates: list[tuple[str, ...]]) -> str | None:
    """Procura uma coluna usando combinações de fragmentos no nome normalizado."""
    if df is None or df.empty:
        return None

    cols_norm = {col: _norm_text(col) for col in df.columns}
    for tokens in predicates:
        for col, norm_col in cols_norm.items():
            if all(token in norm_col for token in tokens):
                return col
    return None


def check_campaign_consistency(
    df_pontos: pd.DataFrame,
    df_esforco: pd.DataFrame,
    df_resultados: pd.DataFrame,
    report: ValidationReport,
) -> None:
    """Valida coerencia de campanhas entre abas usando codigo operacional C###."""
    frames = {
        "Pontos_e_Campanhas": df_pontos,
        "Metadados_Esforco": df_esforco,
        "Resultados": df_resultados,
    }

    campaign_sets: dict[str, set[str]] = {}
    label_sets: dict[str, set[str]] = {}
    missing_columns: list[str] = []

    for name, df in frames.items():
        camp_col = _find_column(df, [("campanha",)])
        if df is None or df.empty or camp_col is None:
            missing_columns.append(name)
            continue

        labels = {
            _norm_text(value)
            for value in df[camp_col].dropna().tolist()
            if _norm_text(value)
        }
        label_sets[name] = labels
        campaign_sets[name] = {_campaign_code(value) for value in labels}

    if missing_columns:
        report.issues.append(
            ValidationIssue(
                code="MISSING_CAMPAIGN_COLUMNS",
                severity="block",
                message=(
                    "Nao foi possivel validar campanhas entre abas; "
                    f"coluna Campanha ausente/vazia em: {missing_columns}."
                ),
            )
        )
        return

    base_codes = campaign_sets["Pontos_e_Campanhas"]
    for name in ("Metadados_Esforco", "Resultados"):
        extra = sorted(campaign_sets[name] - base_codes)
        missing = sorted(base_codes - campaign_sets[name])
        if extra or missing:
            report.issues.append(
                ValidationIssue(
                    code="CAMPAIGN_CODE_MISMATCH",
                    severity="block",
                    message=(
                        f"{name}: campanhas C### divergentes de Pontos_e_Campanhas. "
                        f"Extras: {extra or 'nenhuma'}; ausentes: {missing or 'nenhuma'}."
                    ),
                )
            )

    for name, labels in label_sets.items():
        codes = {_campaign_code(value) for value in labels}
        if len(labels) != len(codes):
            report.issues.append(
                ValidationIssue(
                    code="CAMPAIGN_LABELS_COLLAPSE",
                    severity="info",
                    message=(
                        f"{name}: {len(labels)} rotulo(s) de campanha equivalem a "
                        f"{len(codes)} campanha(s) operacionais C###."
                    ),
                )
            )

    pontos_labels = label_sets["Pontos_e_Campanhas"]
    for name in ("Metadados_Esforco", "Resultados"):
        labels_not_in_points = sorted(label_sets[name] - pontos_labels)
        if labels_not_in_points:
            report.issues.append(
                ValidationIssue(
                    code="CAMPAIGN_LABEL_NOT_IN_POINTS",
                    severity="block",
                    message=(
                        f"{name}: rotulos de campanha existem nesta aba, mas nao existem "
                        "em Pontos_e_Campanhas: "
                        + ", ".join(labels_not_in_points[:20])
                        + (f" (+{len(labels_not_in_points) - 20})" if len(labels_not_in_points) > 20 else "")
                    ),
                )
            )


def check_pontos(df_pontos: pd.DataFrame, report: ValidationReport) -> None:
    """Valida coordenadas e nomes de pontos de coleta."""
    if df_pontos is None or df_pontos.empty:
        return

    # Procura colunas de coordenada
    lat_col = None
    lon_col = None
    for col in df_pontos.columns:
        col_lower = col.lower()
        if "latitude" in col_lower or "lat" in col_lower:
            lat_col = col
        elif "longitude" in col_lower or "lon" in col_lower or "x" in col_lower:
            lon_col = col

    if lat_col and lon_col:
        invalid_coords = []
        for idx, row in df_pontos.iterrows():
            try:
                lat = float(str(row[lat_col]).replace(",", ".").strip())
                lon = float(str(row[lon_col]).replace(",", ".").strip())
                if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                    invalid_coords.append(idx + 2)
            except (ValueError, TypeError):
                invalid_coords.append(idx + 2)

        if invalid_coords:
            report.issues.append(
                ValidationIssue(
                    code="INVALID_COORDINATES",
                    severity="block",
                    message=(
                        f"Coordenadas inválidas nas linhas: {invalid_coords[:5]}. "
                        f"Latitude deve estar em [-90, 90] e longitude em [-180, 180]."
                    ),
                    lines=invalid_coords[:10],
                    sheet="pontos",
                )
            )


def _to_float(value: object) -> float | None:
    if pd.isna(value):
        return None
    text_value = str(value).strip().replace(",", ".")
    if not text_value:
        return None
    try:
        return float(text_value)
    except (TypeError, ValueError):
        return None


def _col(df: pd.DataFrame, *names: str) -> str | None:
    if df is None or df.empty:
        return None
    cols_norm = {c: _norm_text(c) for c in df.columns}
    for wanted in names:
        wanted_norm = _norm_text(wanted)
        for col, norm_col in cols_norm.items():
            if norm_col == wanted_norm:
                return col
    return None


_REQUIRED_SPECIES_DB_COLUMNS = [
    "nome_cientifico",
    "grupo_biologico",
    "reino",
    "filo",
    "classe",
    "ordem",
    "familia",
    "genero",
    "status_ameaca_nacional",
    "status_ameaca_global",
    "origem",
    "habito_alimentar",
    "estrategia_reprodutiva",
    "valor_economico",
]

_OPTIONAL_TRI_STATE_SPECIES_DB_COLUMNS = [
    "cinegetica",
    "xerimbabo",
]


def _is_blank_catalog_value(value: object) -> bool:
    if pd.isna(value):
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def check_pontos_conflitantes_no_banco(
    df_capa: pd.DataFrame,
    df_pontos: pd.DataFrame,
    engine: Engine,
    report: ValidationReport,
) -> None:
    """
    Impede que passe planilha com mesmo (campanha+ponto) e coordenadas divergentes
    de um ponto já existente no banco.
    """
    if df_capa is None or df_capa.empty or df_pontos is None or df_pontos.empty:
        return

    codigo_col = _col(df_capa, "Codigo_Opyta")
    camp_col = _find_column(df_pontos, [("campanha",)])
    ponto_col = _find_column(df_pontos, [("ponto",)])
    lat_col = _find_column(df_pontos, [("latitude",), ("lat",)])
    lon_col = _find_column(df_pontos, [("longitude",), ("lon",)])

    missing = [
        name
        for name, col in [
            ("Codigo_Opyta", codigo_col),
            ("Campanha", camp_col),
            ("Ponto", ponto_col),
            ("Latitude", lat_col),
            ("Longitude", lon_col),
        ]
        if col is None
    ]
    if missing:
        report.issues.append(
            ValidationIssue(
                code="MISSING_POINT_CONFLICT_COLUMNS",
                severity="warning",
                message=(
                    "Não foi possível validar conflito espacial de pontos; "
                    f"colunas ausentes: {missing}."
                ),
            )
        )
        return

    codigo_opyta = _norm_text(df_capa.iloc[0].get(codigo_col))
    if not codigo_opyta:
        report.issues.append(
            ValidationIssue(
                code="EMPTY_PROJECT_CODE",
                severity="block",
                message="Codigo_Opyta vazio na Capa_Projeto.",
            )
        )
        return

    try:
        with engine.connect() as conn:
            id_projeto = conn.execute(
                text(
                    """
                    SELECT id_projeto
                    FROM projetos
                    WHERE LOWER(TRIM(REPLACE(codigo_interno_opyta, CHR(160), ''))) =
                          LOWER(TRIM(REPLACE(:codigo, CHR(160), '')))
                    """
                ),
                {"codigo": codigo_opyta},
            ).scalar()

            if not id_projeto:
                report.issues.append(
                    ValidationIssue(
                        code="PROJECT_NOT_FOUND",
                        severity="warning",
                        message=(
                            f"Projeto com Codigo_Opyta '{codigo_opyta}' não encontrado no banco; "
                            "validação de conflito de pontos foi pulada."
                        ),
                    )
                )
                return

            existing_rows = conn.execute(
                text(
                    """
                    SELECT
                        c.nome_campanha,
                        pc.nome_ponto,
                        pc.latitude,
                        pc.longitude
                    FROM pontos_coleta pc
                    JOIN campanhas c ON c.id_campanha = pc.id_campanha
                    WHERE pc.id_projeto = :id_projeto
                    """
                ),
                {"id_projeto": id_projeto},
            ).fetchall()
    except Exception as exc:
        report.issues.append(
            ValidationIssue(
                code="DB_POINT_CONFLICT_CHECK_ERROR",
                severity="warning",
                message=(
                    "Erro ao validar conflito de pontos no banco: "
                    f"{exc}."
                ),
            )
        )
        return

    existing_map: dict[tuple[str, str], tuple[float | None, float | None]] = {}
    for camp, ponto, lat, lon in existing_rows:
        existing_map[(_norm_text(camp), _norm_text(ponto))] = (
            _to_float(lat),
            _to_float(lon),
        )

    divergence_lines: list[int] = []
    examples: list[str] = []
    eps = 1e-6

    for idx, row in df_pontos.iterrows():
        camp = _norm_text(row.get(camp_col))
        ponto = _norm_text(row.get(ponto_col))
        if not camp or not ponto:
            continue

        lat_new = _to_float(row.get(lat_col))
        lon_new = _to_float(row.get(lon_col))
        key = (camp, ponto)
        if key not in existing_map:
            continue

        lat_old, lon_old = existing_map[key]
        if lat_old is None or lon_old is None or lat_new is None or lon_new is None:
            continue

        if abs(lat_old - lat_new) > eps or abs(lon_old - lon_new) > eps:
            divergence_lines.append(idx + 2)
            if len(examples) < 5:
                examples.append(
                    f"{row.get(camp_col)} / {row.get(ponto_col)} "
                    f"(banco: {lat_old}, {lon_old}; planilha: {lat_new}, {lon_new})"
                )

    if divergence_lines:
        report.issues.append(
            ValidationIssue(
                code="POINT_COORDINATE_DIVERGENCE",
                severity="block",
                message=(
                    f"{len(divergence_lines)} ponto(s) com mesmo nome/campanha já existem no banco "
                    "com coordenadas diferentes. Crie novo nome de ponto (ex.: sufixo -CXX-01) "
                    "ou ajuste a planilha. Exemplos: "
                    + " ; ".join(examples)
                ),
                lines=divergence_lines[:10],
                sheet="pontos",
            )
        )


def check_especies_no_banco(
    df_resultados: pd.DataFrame,
    engine: Engine,
    report: ValidationReport,
    allowed_species: set[str] | None = None,
    strict_unknown_species: bool = False,
) -> None:
    """
    Compara especies nos resultados com o banco de dados.
    Registra quais não foram encontradas. Em modo estrito, bloqueia a migração.
    """
    if df_resultados is None or df_resultados.empty:
        return

    # Procura coluna de espécie
    species_col = None
    for col in df_resultados.columns:
        col_lower = col.lower()
        if "especie" in col_lower or "nome_cientifico" in col_lower or "taxa" in col_lower:
            species_col = col
            break

    if not species_col:
        report.issues.append(
            ValidationIssue(
                code="NO_SPECIES_COLUMN",
                severity="warning",
                message="Não foi encontrada coluna de espécies nos resultados.",
            )
        )
        return

    # Carrega especies do banco e verifica se o cadastro mestre esta completo.
    try:
        with engine.connect() as conn:
            existing_cols = {
                row[0]
                for row in conn.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'especies'
                        """
                    )
                ).fetchall()
            }
            required_schema_cols = (
                _REQUIRED_SPECIES_DB_COLUMNS
                + _OPTIONAL_TRI_STATE_SPECIES_DB_COLUMNS
            )
            missing_db_cols = [col for col in required_schema_cols if col not in existing_cols]
            if missing_db_cols:
                report.issues.append(
                    ValidationIssue(
                        code="DB_SPECIES_SCHEMA_INCOMPLETE",
                        severity="block",
                        message=(
                            "Nao foi possivel validar completude do cadastro mestre "
                            "de especies; colunas ausentes no banco: "
                            f"{missing_db_cols}."
                        ),
                    )
                )
                return

            rows = conn.execute(
                text(
                    """
                    SELECT
                        nome_cientifico,
                        grupo_biologico,
                        reino,
                        filo,
                        classe,
                        ordem,
                        familia,
                        genero,
                        status_ameaca_nacional,
                        status_ameaca_global,
                        origem,
                        habito_alimentar,
                        estrategia_reprodutiva,
                        valor_economico,
                        cinegetica,
                        xerimbabo
                    FROM especies
                    """
                )
            ).mappings().all()
        db_species = {_norm_text(r["nome_cientifico"]) for r in rows}
        incomplete_species: dict[str, list[str]] = {}
        for row in rows:
            name_key = _norm_text(row["nome_cientifico"])
            missing_values = [
                col
                for col in _REQUIRED_SPECIES_DB_COLUMNS
                if _is_blank_catalog_value(row.get(col))
            ]
            if name_key and missing_values:
                incomplete_species[name_key] = missing_values
    except Exception as exc:
        report.issues.append(
            ValidationIssue(
                code="DB_QUERY_ERROR",
                severity="warning",
                message=f"Erro ao carregar espécies do banco: {exc}. Validação de espécies pulada.",
            )
        )
        return

    # Comparar
    allowed_species_norm = {
        _norm_text(species) for species in (allowed_species or set()) if _norm_text(species)
    }
    unknown = set()
    unknown_lines: list[int] = []
    incomplete_used: dict[str, tuple[str, list[str], list[int]]] = {}
    for idx, value in df_resultados[species_col].items():
        if pd.isna(value) or not isinstance(value, str) or value.strip() == "":
            continue
        value_norm = _norm_text(value)
        if value_norm not in db_species and value_norm not in allowed_species_norm:
            unknown.add(value.strip())
            unknown_lines.append(idx + 2)
        elif value_norm in incomplete_species:
            current = incomplete_used.get(
                value_norm,
                (value.strip(), incomplete_species[value_norm], []),
            )
            current[2].append(idx + 2)
            incomplete_used[value_norm] = current

    if unknown:
        report.total_especies_desconhecidas = len(unknown)
        report.especies_desconhecidas = sorted(unknown)[:50]  # Top 50
        severity = "block" if strict_unknown_species else "info"
        report.issues.append(
            ValidationIssue(
                code="UNKNOWN_SPECIES",
                severity=severity,
                message=(
                    f"Total de {len(unknown)} espécie(s) desconhecida(s) nos resultados. "
                    + (
                        "Bloqueio ativado: revise o cadastro mestre ou confirme que as espécies "
                        "estão presentes na aba 'Especies' da mesma planilha."
                        if strict_unknown_species
                        else "Serão cadastradas automaticamente ou necessitarão revisão."
                    )
                ),
                lines=unknown_lines[:10] if unknown_lines else [],
                sheet="resultados",
            )
        )

    if incomplete_used:
        examples = []
        lines: list[int] = []
        for name, missing_values, issue_lines in list(incomplete_used.values())[:10]:
            lines.extend(issue_lines[:2])
            examples.append(
                f"{name} (faltando: {', '.join(missing_values[:8])})"
            )
        report.issues.append(
            ValidationIssue(
                code="INCOMPLETE_SPECIES_CATALOG",
                severity="block",
                message=(
                    f"{len(incomplete_used)} especie(s) usadas nos resultados existem "
                    "no cadastro mestre, mas estao incompletas. Exemplos: "
                    + " ; ".join(examples)
                ),
                lines=lines[:10],
            )
        )


def check_esforco(df_esforco: pd.DataFrame, report: ValidationReport) -> None:
    """Valida dias de esforço (valores numéricos)."""
    if df_esforco is None or df_esforco.empty:
        return

    # Procura coluna de esforço
    esforco_col = None
    for col in df_esforco.columns:
        col_lower = col.lower()
        if "esforco" in col_lower or "dias" in col_lower or "effort" in col_lower:
            esforco_col = col
            break

    if not esforco_col:
        return

    total_dias = 0.0
    invalid_rows = []

    for idx, value in df_esforco[esforco_col].items():
        if pd.isna(value) or (isinstance(value, str) and value.strip() == ""):
            continue
        try:
            dias = float(str(value).strip().replace(",", "."))
            if dias < 0:
                invalid_rows.append(idx + 2)
            else:
                total_dias += dias
        except (ValueError, TypeError):
            invalid_rows.append(idx + 2)

    report.total_esforco_dias = total_dias

    if invalid_rows:
        report.issues.append(
            ValidationIssue(
                code="INVALID_EFFORT",
                severity="warning",
                message=(
                    f"Valores inválidos de esforço nas linhas: {invalid_rows[:5]}. "
                    f"Esforço deve ser numérico e não-negativo."
                ),
                lines=invalid_rows[:10],
                sheet="esforco",
            )
        )


def check_referencias_cruzadas(
    df_resultados: pd.DataFrame,
    df_pontos: pd.DataFrame,
    report: ValidationReport,
) -> None:
    """Valida se pares campanha+ponto dos resultados existem em Pontos_e_Campanhas."""
    if df_resultados is None or df_resultados.empty or df_pontos is None:
        return

    res_camp_col = _find_column(df_resultados, [("campanha",)])
    res_ponto_col = _find_column(df_resultados, [("ponto",)])
    pts_camp_col = _find_column(df_pontos, [("campanha",)])
    pts_ponto_col = _find_column(df_pontos, [("ponto",)])

    missing = [
        name
        for name, col in [
            ("Resultados.Campanha", res_camp_col),
            ("Resultados.Ponto", res_ponto_col),
            ("Pontos_e_Campanhas.Campanha", pts_camp_col),
            ("Pontos_e_Campanhas.Ponto", pts_ponto_col),
        ]
        if col is None
    ]
    if missing:
        report.issues.append(
            ValidationIssue(
                code="MISSING_CROSS_REFERENCE_COLUMNS",
                severity="block",
                message=(
                    "Nao foi possivel validar referencias cruzadas campanha+ponto; "
                    f"colunas ausentes: {missing}."
                ),
            )
        )
        return

    valid_pairs = {
        (_norm_text(row.get(pts_camp_col)), _norm_text(row.get(pts_ponto_col)))
        for _, row in df_pontos.iterrows()
        if _norm_text(row.get(pts_camp_col)) and _norm_text(row.get(pts_ponto_col))
    }

    missing_pairs: list[tuple[int, str, str]] = []
    for idx, row in df_resultados.iterrows():
        camp = _norm_text(row.get(res_camp_col))
        ponto = _norm_text(row.get(res_ponto_col))
        if not camp or not ponto:
            continue
        if (camp, ponto) not in valid_pairs:
            missing_pairs.append(
                (
                    idx + 2,
                    str(row.get(res_camp_col)).strip(),
                    str(row.get(res_ponto_col)).strip(),
                )
            )

    if missing_pairs:
        examples = [
            f"linha {line}: {camp} / {ponto}"
            for line, camp, ponto in missing_pairs[:5]
        ]
        report.issues.append(
            ValidationIssue(
                code="INVALID_POINT_REFERENCE",
                severity="block",
                message=(
                    f"{len(missing_pairs)} registro(s) de resultados referenciam "
                    "campanha+ponto ausente em Pontos_e_Campanhas. Exemplos: "
                    + " ; ".join(examples)
                ),
                lines=[p[0] for p in missing_pairs[:10]],
                sheet="resultados",
            )
        )


def check_resultados_vs_esforco(
    df_resultados: pd.DataFrame,
    df_esforco: pd.DataFrame,
    group: str,
    report: ValidationReport,
) -> None:
    """
    Valida vínculo obrigatório Resultado -> Metadados_Esforco por chave composta.
    Bloqueia migração quando existir resultado sem esforço correspondente.
    """
    if df_resultados is None or df_resultados.empty:
        return

    if df_esforco is None or df_esforco.empty:
        report.issues.append(
            ValidationIssue(
                code="EMPTY_EFFORT_SHEET",
                severity="block",
                message=(
                    f"Grupo '{group}': a aba Metadados_Esforco está vazia, mas há resultados informados."
                ),
            )
        )
        return

    # Campanha+ponto formam a base universal. Método e tipo entram na chave
    # somente quando existem nas duas abas; os modelos oficiais variam por grupo.
    res_camp = _find_column(df_resultados, [("campanha",)])
    res_ponto = _find_column(df_resultados, [("ponto",)])
    res_metodo = _find_column(df_resultados, [("metodo",), ("captura",)])
    res_tipo = _find_column(df_resultados, [("tipo", "amostr")])

    esf_camp = _find_column(df_esforco, [("campanha",)])
    esf_ponto = _find_column(df_esforco, [("ponto",)])
    esf_metodo = _find_column(df_esforco, [("metodo",), ("captura",)])
    esf_tipo = _find_column(df_esforco, [("tipo", "amostr")])

    res_species = _find_column(
        df_resultados,
        [("nome", "cient"), ("especie",), ("taxa",)],
    )

    missing_res = [name for name, col in [("Campanha", res_camp), ("Ponto", res_ponto)] if col is None]
    missing_esf = [name for name, col in [("Campanha", esf_camp), ("Ponto", esf_ponto)] if col is None]

    if missing_res or missing_esf:
        parts = []
        if missing_res:
            parts.append(f"colunas ausentes em resultados: {missing_res}")
        if missing_esf:
            parts.append(f"colunas ausentes em Metadados_Esforco: {missing_esf}")
        report.issues.append(
            ValidationIssue(
                code="MISSING_EFFORT_LINK_COLUMNS",
                severity="block",
                message=(
                    f"Grupo '{group}': não foi possível validar vínculo resultado-esforço; "
                    + "; ".join(parts)
                    + "."
                ),
            )
        )
        return

    def _has_values(df: pd.DataFrame, col: str | None) -> bool:
        return col is not None and any(_norm_text(value) for value in df[col].dropna())

    use_method = _has_values(df_resultados, res_metodo) and _has_values(df_esforco, esf_metodo)
    use_type = _has_values(df_resultados, res_tipo) and _has_values(df_esforco, esf_tipo)

    def _key(
        row: pd.Series,
        camp_col: str,
        ponto_col: str,
        metodo_col: str | None,
        tipo_col: str | None,
    ) -> tuple[str, ...]:
        values = [_campaign_code(row.get(camp_col)), _norm_text(row.get(ponto_col))]
        if use_method and metodo_col:
            values.append(_norm_text(row.get(metodo_col)))
        if use_type and tipo_col:
            values.append(_norm_text(row.get(tipo_col)))
        return tuple(values)

    effort_keys = set()
    for _, row in df_esforco.iterrows():
        key = _key(row, esf_camp, esf_ponto, esf_metodo, esf_tipo)
        if all(key):
            effort_keys.add(key)

    if not effort_keys:
        report.issues.append(
            ValidationIssue(
                code="EMPTY_EFFORT_KEYS",
                severity="block",
                message=(
                    f"Grupo '{group}': nenhum esforço válido encontrado em Metadados_Esforco "
                    "(chave campanha+ponto e discriminadores compartilhados)."
                ),
            )
        )
        return

    invalid_refs: list[tuple[int, str, tuple[str, ...]]] = []
    for idx, row in df_resultados.iterrows():
        key = _key(row, res_camp, res_ponto, res_metodo, res_tipo)
        if not all(key) or key not in effort_keys:
            especie = _norm_text(row.get(res_species)) if res_species else ""
            invalid_refs.append((idx + 2, especie, key))

    if invalid_refs:
        examples = []
        for line, especie, key in invalid_refs[:5]:
            examples.append(
                f"linha {line} | especie='{especie or '-'}' | "
                f"esforco='{' | '.join(key)}'"
            )

        report.issues.append(
            ValidationIssue(
                code="INVALID_EFFORT_REFERENCE",
                severity="block",
                message=(
                    f"Grupo '{group}': {len(invalid_refs)} registro(s) de resultados sem esforço válido "
                    "nos metadados (campanha+ponto e discriminadores compartilhados). Exemplos: "
                    + " ; ".join(examples)
                ),
                lines=[line for line, _, _ in invalid_refs[:10]],
                sheet="resultados",
            )
        )
