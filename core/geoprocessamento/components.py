from __future__ import annotations

import pandas as pd
import streamlit as st

from app.ui.layout import inject_saas_styles, render_action_buttons, render_executive_summary
from .queries import ModoGeo


VALID_MODES: list[ModoGeo] = ["Fisico", "Biota"]


BIO_INDICATORS = [
    "riqueza",
    "abundancia_total",
    "biomassa_total",
    "shannon",
    "pielou",
    "numero_taxons",
    "bmwp_total",
    "riqueza_ept",
    "abundancia_ept",
]

PHYSICAL_INDICATORS = [
    "iqa",
    "parametros_nao_conformes",
    "parametros_avaliados",
    "parametros_com_limite",
    "pontos_amostrados",
]


def inject_geo_styles() -> None:
    inject_saas_styles()
    st.markdown(
        """
        <style>
        .geo-hero {
            padding: 18px 22px;
            border-radius: 18px;
            margin-bottom: 1rem;
            border: 1px solid rgba(15, 23, 42, 0.08);
            background: linear-gradient(135deg, #F8FAFC 0%, #E0F2FE 48%, #ECFCCB 100%);
        }
        .geo-hero h1 {
            margin: 0;
            color: #0F172A;
            font-size: 2rem;
        }
        .geo-hero p {
            margin: 0.35rem 0 0 0;
            color: #334155;
            font-size: 0.98rem;
        }
        .geo-section-title {
            font-size: 1.05rem;
            font-weight: 700;
            color: #0F172A;
            margin: 1rem 0 0.6rem 0;
        }
        .geo-subtle {
            color: #475569;
            font-size: 0.88rem;
        }
        .geo-insight {
            border-radius: 14px;
            padding: 12px 14px;
            border: 1px solid #CBD5E1;
            background: #FFFFFF;
            margin-bottom: 0.75rem;
        }
        .geo-insight strong {
            color: #0F172A;
        }
        .geo-toolbar {
            border-radius: 16px;
            padding: 12px 14px;
            border: 1px solid rgba(15, 23, 42, 0.08);
            background: rgba(255, 255, 255, 0.98);
            margin-bottom: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    inject_geo_styles()
    st.markdown(
        """
        <div class="geo-hero">
            <h1>Geoambiental</h1>
            <p>Dashboard analitico para leitura espacial, comparacao de campanhas e identificacao rapida de padroes.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_mode_selector() -> ModoGeo:
    return st.radio(
        "Modo de visualizacao",
        options=VALID_MODES,
        index=1,
        horizontal=True,
    )


def render_filter_bar_title() -> None:
    st.markdown('<div class="geo-toolbar"><div class="geo-section-title">Filtros analiticos</div><div class="geo-subtle">Refine projeto, empreendimento, campanha e grupo para dirigir o mapa, os rankings e os insights.</div></div>', unsafe_allow_html=True)


def render_empty_state() -> None:
    st.info("Nenhum dado encontrado para os filtros selecionados. Ajuste projeto, empreendimento, campanha ou grupo e tente novamente.")


def render_project_filter(projetos: list[str]) -> list[str]:
    return st.multiselect("Projeto", options=projetos, key="geo_projetos", placeholder="Selecione um ou mais projetos")


def render_enterprise_filter(empreendimentos: list[str], disabled: bool = False) -> list[str]:
    if disabled:
        st.multiselect("Empreendimento", options=[], disabled=True)
        return []
    return st.multiselect(
        "Empreendimento",
        options=empreendimentos,
        key="geo_empreendimentos",
        placeholder="Filtre por empreendimento",
    )


def render_campaign_filter(campanhas: list[str], disabled: bool = False) -> list[str]:
    if disabled:
        st.multiselect("Campanha", options=[], disabled=True)
        return []
    return st.multiselect(
        "Campanha",
        options=campanhas,
        key="geo_campanhas",
        placeholder="Selecione campanhas",
    )


def render_biological_group_filter(grupos: list[str], disabled: bool = False) -> list[str]:
    if disabled:
        st.multiselect("Grupo biologico", options=[], disabled=True)
        return []
    return st.multiselect(
        "Grupo biologico",
        options=grupos,
        key="geo_grupos_biologicos",
        placeholder="Selecione grupos",
    )


def render_indicator_selector(modo: ModoGeo, df: pd.DataFrame) -> str:
    if modo != "Biota":
        options = ["iqa", "parametros_nao_conformes", "parametros_avaliados", "parametros_com_limite"]
        return st.selectbox("Indicador do mapa", options=options, index=0, key="geo_indicador_fisico")

    options = ["riqueza", "shannon", "pielou", "abundancia_total", "biomassa_total", "numero_taxons"]
    if "bmwp_total" in df.columns:
        options.extend(["bmwp_total", "riqueza_ept", "abundancia_ept"])
    return st.selectbox("Indicador do mapa", options=options, index=0, key="geo_indicador")


def render_context_bar(
    modo: ModoGeo,
    projetos: list[str],
    empreendimentos: list[str],
    campanhas: list[str],
    grupos_biologicos: list[str],
    indicador: str,
    total_pontos: int,
) -> None:
    st.caption(
        " | ".join(
            [
                f"Modo: {modo}",
                f"Projetos: {len(projetos) if projetos else 'Todos'}",
                f"Empreendimentos: {len(empreendimentos) if empreendimentos else 'Todos'}",
                f"Campanhas: {len(campanhas) if campanhas else 'Todas'}",
                f"Grupos: {len(grupos_biologicos) if grupos_biologicos else 'Todos'}",
                f"Indicador foco: {indicador}",
                f"Pontos visiveis: {total_pontos}",
            ]
        )
    )


def render_data_quality_warning(df: pd.DataFrame, modo: ModoGeo, indicador: str) -> None:
    if df.empty:
        return

    if modo == "Fisico":
        st.info("O modo Fisico mostra a leitura de agua superficial com foco em conformidade e IQA.")
        total_pontos = int(df["ponto"].nunique()) if "ponto" in df.columns else 0
        if indicador == "iqa" and "parametros_com_limite" in df.columns and total_pontos > 0:
            pontos_com_limite = int((pd.to_numeric(df["parametros_com_limite"], errors="coerce").fillna(0) > 0).sum())
            if pontos_com_limite == 0:
                st.warning("O IQA ficou zerado porque nenhum ponto filtrado possui parametro com limite regulatorio disponivel.")
            elif pontos_com_limite < total_pontos:
                st.info(f"Cobertura parcial de IQA: {pontos_com_limite}/{total_pontos} ponto(s) possuem base suficiente para calculo.")
        return

    total_pontos = int(df["ponto"].nunique()) if "ponto" in df.columns else 0
    if indicador == "bmwp_total" and "bmwp_registros_com_score" in df.columns and total_pontos > 0:
        pontos_com_bmwp = int((pd.to_numeric(df["bmwp_registros_com_score"], errors="coerce").fillna(0) > 0).sum())
        if pontos_com_bmwp == 0:
            st.warning("BMWP ficou zerado porque nao ha pontuacao BMWP preenchida nos dados filtrados.")
        elif pontos_com_bmwp < total_pontos:
            st.info(f"Cobertura parcial de BMWP: {pontos_com_bmwp}/{total_pontos} ponto(s) com score preenchido.")

    if indicador in {"riqueza_ept", "abundancia_ept"} and "ept_registros_com_ordem" in df.columns and total_pontos > 0:
        pontos_com_ordem = int((pd.to_numeric(df["ept_registros_com_ordem"], errors="coerce").fillna(0) > 0).sum())
        if pontos_com_ordem == 0:
            st.warning("EPT ficou zerado porque nao ha ordem taxonomica preenchida nos dados filtrados.")
        elif pontos_com_ordem < total_pontos:
            st.info(f"Cobertura parcial de EPT: {pontos_com_ordem}/{total_pontos} ponto(s) com ordem preenchida.")


def reset_geo_filters() -> None:
    st.cache_data.clear()
    for key in [
        "geo_projetos",
        "geo_empreendimentos",
        "geo_campanhas",
        "geo_grupos_biologicos",
        "geo_indicador",
        "geo_indicador_fisico",
        "geo_table_search",
        "geo_table_campaign",
        "geo_table_sort",
        "geo_table_only_critical",
    ]:
        if key in st.session_state:
            st.session_state.pop(key)


def render_cards(resumo: dict[str, float], modo: ModoGeo) -> None:
    if modo == "Biota":
        render_executive_summary(
            "KPIs",
            [
                {"label": "Riqueza", "value": f"{resumo.get('riqueza_media', 0.0):.2f}", "hint": "Media por ponto", "status": "ok"},
                {"label": "Diversidade", "value": f"{resumo.get('shannon_medio', 0.0):.2f}", "hint": "Indice Shannon medio", "status": "info"},
                {"label": "Equitabilidade", "value": f"{resumo.get('pielou_medio', 0.0):.2f}", "hint": "Indice Pielou medio", "status": "info"},
                {"label": "Abundancia total", "value": f"{resumo.get('abundancia_total', 0.0):,.0f}", "hint": "Soma dos registros", "status": "ok"},
                {"label": "Pontos", "value": f"{resumo.get('pontos', 0.0):,.0f}", "hint": "Pontos analisados", "status": "info"},
                {"label": "Campanhas", "value": f"{resumo.get('campanhas', 0.0):,.0f}", "hint": "Cobertura temporal", "status": "info"},
            ],
        )

        st.markdown('<div class="geo-section-title">Blocos de indicadores</div>', unsafe_allow_html=True)
        render_executive_summary(
            "Biodiversidade",
            [
                {"label": "Riqueza media", "value": f"{resumo.get('riqueza_media', 0.0):.2f}", "hint": "Numero medio de taxa", "status": "ok"},
                {"label": "Shannon medio", "value": f"{resumo.get('shannon_medio', 0.0):.2f}", "hint": "Diversidade", "status": "info"},
                {"label": "Pielou medio", "value": f"{resumo.get('pielou_medio', 0.0):.2f}", "hint": "Distribuicao entre taxa", "status": "info"},
            ],
        )
        render_executive_summary(
            "Esforco amostral",
            [
                {"label": "Pontos", "value": f"{resumo.get('pontos', 0.0):,.0f}", "hint": "Cobertura espacial", "status": "info"},
                {"label": "Campanhas", "value": f"{resumo.get('campanhas', 0.0):,.0f}", "hint": "Cobertura temporal", "status": "info"},
                {"label": "Projetos", "value": f"{resumo.get('projetos', 0.0):,.0f}", "hint": "Escopo filtrado", "status": "info"},
            ],
        )
        render_executive_summary(
            "Abundancia",
            [
                {"label": "Abundancia total", "value": f"{resumo.get('abundancia_total', 0.0):,.0f}", "hint": "Total observado", "status": "ok"},
                {"label": "Biomassa total", "value": f"{resumo.get('biomassa_total', 0.0):,.2f}", "hint": "Massa acumulada", "status": "info"},
                {"label": "BMWP total", "value": f"{resumo.get('bmwp_total', 0.0):,.0f}", "hint": "Somente quando aplicavel", "status": "warn" if resumo.get('bmwp_total', 0.0) > 0 else "info"},
            ],
        )
        return

    render_executive_summary(
        "KPIs",
        [
            {"label": "IQA", "value": f"{resumo.get('iqa_medio', 0.0):.1f}", "hint": "Media por ponto", "status": "ok"},
            {"label": "Nao conformes", "value": f"{resumo.get('parametros_nao_conformes', 0.0):,.0f}", "hint": "Soma dos desvios", "status": "warn"},
            {"label": "Parametros com limite", "value": f"{resumo.get('parametros_com_limite', 0.0):,.0f}", "hint": "Base calculavel", "status": "info"},
            {"label": "Pontos", "value": f"{resumo.get('pontos', 0.0):,.0f}", "hint": "Pontos avaliados", "status": "info"},
            {"label": "Campanhas", "value": f"{resumo.get('campanhas', 0.0):,.0f}", "hint": "Cobertura temporal", "status": "info"},
            {"label": "Projetos", "value": f"{resumo.get('projetos', 0.0):,.0f}", "hint": "Escopo filtrado", "status": "info"},
        ],
    )

    st.markdown('<div class="geo-section-title">Blocos de indicadores</div>', unsafe_allow_html=True)
    render_executive_summary(
        "Biodiversidade",
        [
            {"label": "IQA medio", "value": f"{resumo.get('iqa_medio', 0.0):.1f}", "hint": "Qualidade da agua", "status": "ok"},
            {"label": "Parametros com limite", "value": f"{resumo.get('parametros_com_limite', 0.0):,.0f}", "hint": "Base regulatoria", "status": "info"},
            {"label": "Matriz", "value": "Agua superficial", "hint": "Escopo atual", "status": "info"},
        ],
    )
    render_executive_summary(
        "Esforco amostral",
        [
            {"label": "Pontos", "value": f"{resumo.get('pontos', 0.0):,.0f}", "hint": "Cobertura espacial", "status": "info"},
            {"label": "Campanhas", "value": f"{resumo.get('campanhas', 0.0):,.0f}", "hint": "Cobertura temporal", "status": "info"},
            {"label": "Projetos", "value": f"{resumo.get('projetos', 0.0):,.0f}", "hint": "Escopo filtrado", "status": "info"},
        ],
    )
    render_executive_summary(
        "Abundancia",
        [
            {"label": "Nao conformes", "value": f"{resumo.get('parametros_nao_conformes', 0.0):,.0f}", "hint": "Eventos fora do padrao", "status": "warn"},
            {"label": "Parametros com limite", "value": f"{resumo.get('parametros_com_limite', 0.0):,.0f}", "hint": "Base de comparacao", "status": "info"},
            {"label": "Regra atual", "value": "IQA + conformidade", "hint": "Leitura executiva", "status": "info"},
        ],
    )


def _critical_mask(series: pd.Series, metric: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if metric in {"riqueza", "shannon", "pielou", "iqa", "bmwp_total"}:
        return numeric.fillna(0) <= numeric.quantile(0.2)
    if metric in {"parametros_nao_conformes"}:
        return numeric.fillna(0) >= numeric.quantile(0.8)
    return pd.Series([False] * len(series), index=series.index)


def render_ranking(df: pd.DataFrame, modo: ModoGeo, top_n: int = 10) -> None:
    st.subheader("Ranking de pontos")
    if df.empty:
        st.info("Sem dados suficientes para montar ranking.")
        return

    if modo == "Biota":
        metric_tabs = [
            ("Riqueza", "riqueza", False),
            ("Diversidade", "shannon", False),
            ("Equitabilidade", "pielou", False),
            ("Abundancia", "abundancia_total", False),
        ]
    else:
        metric_tabs = [
            ("IQA", "iqa", False),
            ("Nao conformes", "parametros_nao_conformes", True),
            ("Parametros avaliados", "parametros_avaliados", False),
        ]

    tabs = st.tabs([name for name, _, _ in metric_tabs])
    for tab, (label, metric, ascending) in zip(tabs, metric_tabs):
        with tab:
            if metric not in df.columns:
                st.info(f"Metrica {metric} indisponivel para este recorte.")
                continue

            ranking = df[[c for c in ["ponto", "campanha", "projeto", metric] if c in df.columns]].copy()
            ranking[metric] = pd.to_numeric(ranking[metric], errors="coerce").fillna(0)
            ranking = ranking.sort_values(metric, ascending=ascending).head(top_n).reset_index(drop=True)
            if ranking.empty:
                st.info("Sem pontos para exibir.")
                continue

            leader = ranking.iloc[0]
            st.markdown(
                f"**Top ponto em {label}:** {leader.get('ponto', '-')} | campanha {leader.get('campanha', '-')} | valor {leader.get(metric, 0):.2f}"
            )
            ranking.insert(0, "posicao", range(1, len(ranking) + 1))
            st.dataframe(
                ranking,
                use_container_width=True,
                hide_index=True,
                column_config={
                    metric: st.column_config.ProgressColumn(
                        label,
                        min_value=float(ranking[metric].min()),
                        max_value=float(ranking[metric].max()) if float(ranking[metric].max()) > float(ranking[metric].min()) else float(ranking[metric].min()) + 1,
                        format="%.2f",
                    )
                },
            )


def render_table(df: pd.DataFrame, indicador: str | None = None, modo: ModoGeo = "Biota") -> None:
    st.subheader("Tabela analitica")
    if df.empty:
        render_empty_state()
        return

    table_df = df.copy()
    numeric_candidates = [c for c in BIO_INDICATORS + PHYSICAL_INDICATORS if c in table_df.columns]
    default_sort = indicador if indicador in table_df.columns else (numeric_candidates[0] if numeric_candidates else table_df.columns[0])

    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    busca = c1.text_input("Busca rapida", key="geo_table_search", placeholder="Ponto, projeto ou campanha")
    campanha_filtro = c2.selectbox(
        "Campanha",
        options=["(todas)"] + sorted(table_df["campanha"].astype(str).unique().tolist()) if "campanha" in table_df.columns else ["(todas)"],
        key="geo_table_campaign",
    )
    sort_by = c3.selectbox("Ordenar por", options=[default_sort] + [c for c in numeric_candidates if c != default_sort], key="geo_table_sort")
    only_critical = c4.checkbox("So criticos", key="geo_table_only_critical")

    if busca:
        mask = pd.Series(False, index=table_df.index)
        for col in ["ponto", "projeto", "campanha", "empreendimento", "grupo_biologico"]:
            if col in table_df.columns:
                mask = mask | table_df[col].astype(str).str.contains(busca, case=False, na=False)
        table_df = table_df[mask].copy()

    if campanha_filtro != "(todas)" and "campanha" in table_df.columns:
        table_df = table_df[table_df["campanha"].astype(str) == campanha_filtro].copy()

    if sort_by in table_df.columns:
        table_df[sort_by] = pd.to_numeric(table_df[sort_by], errors="coerce")
        ascending = sort_by in {"parametros_nao_conformes"} and modo != "Biota"
        table_df = table_df.sort_values(sort_by, ascending=ascending).copy()

    if only_critical and sort_by in table_df.columns:
        critical = _critical_mask(table_df[sort_by], sort_by)
        table_df = table_df[critical].copy()

    def _highlight(s: pd.Series):
        if s.name not in numeric_candidates:
            return ["" for _ in s]
        crit = _critical_mask(s, s.name)
        return ["background-color: #FEF2F2; color: #991B1B; font-weight: 600;" if flag else "" for flag in crit]

    st.dataframe(table_df.style.apply(_highlight), use_container_width=True, height=480)


def build_automatic_insights(df: pd.DataFrame, modo: ModoGeo) -> list[str]:
    if df.empty:
        return []

    insights: list[str] = []
    if modo == "Biota":
        if {"ponto", "riqueza"}.issubset(df.columns):
            ranking = df[["ponto", "campanha", "riqueza"]].copy()
            ranking["riqueza"] = pd.to_numeric(ranking["riqueza"], errors="coerce").fillna(0)
            best = ranking.sort_values("riqueza", ascending=False).iloc[0]
            worst = ranking.sort_values("riqueza", ascending=True).iloc[0]
            insights.append(f"Maior riqueza no ponto {best['ponto']} ({best['campanha']}), com valor {best['riqueza']:.0f}.")
            insights.append(f"Menor riqueza no ponto {worst['ponto']} ({worst['campanha']}), indicando prioridade para revisao local.")
        if {"campanha", "shannon"}.issubset(df.columns):
            comp = df.groupby("campanha", dropna=False)["shannon"].mean().sort_values(ascending=False)
            if len(comp) >= 2:
                insights.append(
                    f"A campanha com melhor diversidade media foi {comp.index[0]} ({comp.iloc[0]:.2f}), enquanto {comp.index[-1]} teve o menor valor ({comp.iloc[-1]:.2f})."
                )
        if "abundancia_total" in df.columns:
            total = pd.to_numeric(df["abundancia_total"], errors="coerce").fillna(0)
            if total.std() > 0 and total.mean() > 0 and total.std() / total.mean() > 1:
                insights.append("A abundancia apresenta alta dispersao entre pontos, sugerindo hotspots ecologicos e necessidade de leitura espacial cuidadosa.")
    else:
        if {"ponto", "iqa"}.issubset(df.columns):
            comp = df[["ponto", "campanha", "iqa"]].copy()
            comp["iqa"] = pd.to_numeric(comp["iqa"], errors="coerce").fillna(0)
            best = comp.sort_values("iqa", ascending=False).iloc[0]
            worst = comp.sort_values("iqa", ascending=True).iloc[0]
            insights.append(f"Melhor IQA observado em {best['ponto']} ({best['campanha']}), com valor {best['iqa']:.1f}.")
            insights.append(f"Pior IQA observado em {worst['ponto']} ({worst['campanha']}), demandando investigacao prioritaria.")
        if "parametros_nao_conformes" in df.columns:
            non_conf = pd.to_numeric(df["parametros_nao_conformes"], errors="coerce").fillna(0)
            if non_conf.sum() > 0:
                insights.append(f"Foram contabilizadas {int(non_conf.sum())} nao conformidades no recorte atual.")
    return insights[:4]


def render_insights(df: pd.DataFrame, modo: ModoGeo) -> None:
    st.subheader("Insights automaticos")
    insights = build_automatic_insights(df, modo)
    if not insights:
        st.info("Ainda nao ha padroes suficientes para gerar insights automáticos neste recorte.")
        return
    for idx, insight in enumerate(insights, start=1):
        st.markdown(
            f'<div class="geo-insight"><strong>Insight {idx}</strong><br>{insight}</div>',
            unsafe_allow_html=True,
        )


def render_actions(df: pd.DataFrame, modo: ModoGeo) -> str | None:
    st.subheader("Acoes")
    action = render_action_buttons(
        [
            {"label": "Comparar campanhas", "key": "geo_compare", "primary": True},
            {"label": "Limpar recorte", "key": "geo_reset"},
        ]
    )

    csv_data = df.to_csv(index=False).encode("utf-8") if not df.empty else b""
    st.download_button(
        "Exportar dados",
        data=csv_data,
        file_name=f"geoambiental_{modo.lower()}.csv",
        mime="text/csv",
        key=f"geo_export_{modo.lower()}",
    )

    report_lines = [
        f"RELATORIO GEOAMBIENTAL - {modo}",
        f"Registros: {len(df)}",
        f"Projetos: {df['projeto'].nunique() if 'projeto' in df.columns and not df.empty else 0}",
        f"Campanhas: {df['campanha'].nunique() if 'campanha' in df.columns and not df.empty else 0}",
        f"Pontos: {df['ponto'].nunique() if 'ponto' in df.columns and not df.empty else 0}",
        "",
    ]
    report_lines.extend([f"- {item}" for item in build_automatic_insights(df, modo)])
    st.download_button(
        "Gerar relatorio",
        data="\n".join(report_lines).encode("utf-8"),
        file_name=f"relatorio_geoambiental_{modo.lower()}.txt",
        mime="text/plain",
        key=f"geo_report_{modo.lower()}",
    )
    return action


def render_campaign_comparison(df: pd.DataFrame, modo: ModoGeo) -> None:
    st.subheader("Comparacao entre campanhas")
    if df.empty or "campanha" not in df.columns or df["campanha"].nunique() < 2:
        st.info("Selecione pelo menos duas campanhas para comparar desempenho e padroes espaciais.")
        return

    if modo == "Biota":
        metrics = [c for c in ["riqueza", "shannon", "pielou", "abundancia_total"] if c in df.columns]
    else:
        metrics = [c for c in ["iqa", "parametros_nao_conformes", "parametros_avaliados"] if c in df.columns]

    if not metrics:
        st.info("Nao ha metricas suficientes para comparacao neste recorte.")
        return

    comp = df.groupby("campanha", dropna=False)[metrics].mean(numeric_only=True).reset_index()
    st.dataframe(comp, use_container_width=True, hide_index=True)
    for metric in metrics:
        st.bar_chart(comp.set_index("campanha")[[metric]], use_container_width=True)
