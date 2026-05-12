# Plano de Melhorias - Streamlit (Fase 1: Próx. 1-2 meses)

**Data**: 2026-04-16  
**Estratégia**: Manter Streamlit como framework principal, melhorar UX/performance incrementalmente  
**Timeline**: 1-2 semanas por bloco

---

## Blocos de Melhoria (Priorizados)

### 🎯 Bloco 1: Padrão Único de UX (1 semana)
**Objetivo**: Criar linguagem visual consistente entre todas as páginas  
**Alinhado com**: META_PROXIMAS_ETAPAS.txt → Direcionar #1

#### Actions
- [x] Criar `core/ui/design_system.py` com componentes padronizados:
  - `render_section_header()` — Títulos com ícone + descrição
  - `render_status_badge()` — Status badges (✅ Concluído, ⏳ Processando, ❌ Erro, ⚠️ Aviso)
  - `render_metric_card()` — Cards KPI padronizados (não inline like current)
  - `render_pipeline_step()` — Indicadores de etapa do pipeline (parcial)
  
- [~] Refatorar componentes existentes em `core/geoprocessamento/components.py`:
  - Unificar estilo de renderização (fonte, cores, espaçamento)
  - Substituir `st.metric()` inline por `render_metric_card()` reutilizável (parcial)
  - Aplicar ícones consistentes em todas as ações

- [x] Aplicar em 2 páginas piloto:
  - `app/pages/01_Importacao.py` (mensagens e header padronizados)
  - `app/pages/6_Geoprocessamento.py` (integração inicial do design system)

**Impacto**: Usuário percebe coerência; reduz confusão visual; facilita adopção futuro.

---

### 🔍 Bloco 2: Cache Agressivo + Performance (1 semana)
**Objetivo**: Reduzir re-execução de queries pesadas  
**Alinhado com**: UX responsiva (menos "Re-running" messages no Streamlit)

#### Actions
- [x] Revisar `core/geoprocessamento/services.py`:
  - Mover `carregar_dados_geo()` para `@st.cache_data(ttl=3600)` (1 hora)
  - Mover `calcular_resumo_geo()` para cache com key=(projeto_id, modo_geo, filtros_hash)
  - Implementar cache robusto em funções de resumo/preparo de mapa

- [~] Implementar cache em queries (`core/geoprocessamento/queries.py`):
  - `get_geo_biota()` → cache 30min (dados históricos, não mudam)
  - `get_geo_fisico_com_empreendimento()` → cache 30min
  - Fallback views → cache mais agressivo (1 hora)

- [ ] Adicionar `st.cache_resource` em `core/engine.py`:
  - Engine SQLAlchemy reutilizado em todas as páginas (já faz isso, confirmar)

- [ ] Testabilidade:
  - Adicionar `@st.cache_data(show_spinner="Carregando dados...")` com spinners customizados
  - Medir tempo antes/depois com `st.write(f"⏱️ Carregamento: {elapsed}s")`

**Impacto**: Página geo recarrega em 500ms ao invés de 3-5s; usuário fica feliz.

---

### 📊 Bloco 3: Dashboard de Status Operacional (1-2 semanas)
**Objetivo**: Criar visão executiva do pipeline (descrita em META_PROXIMAS_ETAPAS.txt → Prioridade #2)  
**Nova página**: `app/pages/00_Pipeline_Status.py`

#### Componentes
- [x] **Status Resumido** (Cards):
  - Últimas 24h: Importações processadas, validações OK/ERRO, migrações concluídas
  - Indicador: ✅ Sistema operacional | ⚠️ Pendências em validação | ❌ Erros críticos

- [x] **Timeline do Pipeline** (Plotly):
  - Eixo Y: Projetos / Grupos biológicos
  - Eixo X: Data
  - Blocos coloridos: Etapa (Upload→Validação→Migração→Pronto)
  - Tooltips: Arquivos processados, erros, tempo decorrido

- [x] **Rastreamento de Dados**:
  - Tabela: Projeto | Grupo Biológico | Última Importação | Status | Registros | Validação %
  - Clickable: clique=detalhes na validação
  
- [x] **Alertas & Insights**:
  - "3 projetos aguardando validação há >2 dias"
  - "Taxa de erro em Bentos: 12% (acima de 5% histórico)"
  - "Dados de Ictio estão 30 dias desatualizados"

**Data Source**: Logs de importação + tabelas de auditoria (criar se não existir)

---

### 🎨 Bloco 4: Plotly Interativo p/ Geoambiental (1-2 semanas)
**Objetivo**: Evoluir além de Streamlit estático, adicionar dinâmica sem sair do Streamlit  
**Alinhado com**: META_PROXIMAS_ETAPAS.txt → Direcionar #4 (leitura executiva)

#### Actions
- [~] Substituir cards estáticos por Plotly Sunburst/TreeMap:
  - Visualizar hierarquia: Projeto → Campanha → Ponto → Indicadores
  - Clicar em bubble = destaca pontos no mapa
  - Hover = mostra valores completos

- [x] Mapa Interativo (já usa leafmap, melhorar):
  - Adicionar `st.plotly_figure()` com scatter de pontos + cores por indicador
  - Permitir filtro por range deslizante (min-max) SEM re-executar query
  - Click em ponto = popup detalhado (campanha, datas, série histórica mini-gráfico)

- [x] Gráficos de Série Temporal:
  - Por Ponto: indicador ao longo de campanhas (linha + marker)
  - Por Campanha: distribuição de pontos (box plot ou distribuição)
  - Eixo secundário: compara 2 indicadores

**Impacto**: Usuário explora dados sem re-executar página; sente dinamismo sem sair Streamlit.

---

### 🔐 Bloco 5: Auditoria & Qualidade de Dados (2 semanas)
**Objetivo**: Criar camada de visibilidade sobre qualidade (META_PROXIMAS_ETAPAS.txt → Prioridade #3)  
**Nova página**: `app/pages/05_Qualidade_Dados.py`

#### Seções
- [ ] **Cobertura de Dados**:
  - Heatmap: Projeto × Grupo Biológico → % de cobertura temporal
  - Identifica "brancos" (sem dados) facilmente

- [ ] **Outliers & Anomalias**:
  - Flags automáticas: valores >3σ do histórico, mudanças abruptas
  - Tabela clickable para revisar cada anomalia

- [ ] **Validação Histórica**:
  - Gráfico: Taxa de erro de validação ao longo do tempo (por grupo)
  - Identifica qual biotaé problemática

- [ ] **Lineage de Dados**:
  - Para cada registro: "Importado em X, validação Y, versão Z, migrado por usuário W"
  - Rastreamento completo para auditoria

**Data Source**: Adicionar timestamps e user_id em logs de importação

---

### 📋 Bloco 6: Modelos Oficiais de Planilha (1 semana)
**Objetivo**: Distribuir templates + docs (META_PROXIMAS_ETAPAS.txt → Diretriz de Produto)

#### Actions
- [x] Criar gerador de modelos oficiais em `core/modelos_oficiais.py` (sem binários no repo):
  - modelo mestre de espécies
  - modelo mestre de parâmetros
  - modelos de importação por grupo

- [x] Embutir nos modelos:
  - Abas com instruções (Leia-me, Dicionário, Exemplos)
  - Validações de estrutura e cuidados de preenchimento
  - Cores visuais para campos obrigatórios vs opcionais

- [x] Disponibilizar em app:
  - `app/pages/01_Importacao.py` → botão "Baixar modelo oficial"
  - `app/pages/00_Base_Mestre.py` → downloads de espécies e parâmetros no topo
  - validação estrutural prévia para cadastro de parâmetros

---

## Arquitetura de Implementação

### Estrutura de Pastas (Nova)
```
core/
├── ui/
│   ├── __init__.py
│   ├── design_system.py     ← Bloco 1
│   ├── status_badges.py     ← Reutilizável
│   └── metric_cards.py      ← Reutilizável
│
├── cache/
│   ├── __init__.py
│   └── cache_utils.py       ← Bloco 2 (hash filters, key builders)
│
└── audit/
    ├── __init__.py
    └── audit_logger.py      ← Bloco 5 (registrar todas as ops)

app/pages/
├── 00_Pipeline_Status.py    ← Bloco 3 (nova página)
├── 05_Qualidade_Dados.py    ← Bloco 5 (nova página)
├── 01_Importacao.py         ← Use design_system
├── 6_Geoprocessamento.py    ← Use cache + Plotly
└── ...

assets/
└── modelos_oficiais/        ← Bloco 6 (novos arquivos)
    ├── template_especies_v1.0.xlsx
    ├── template_importacao_bentos_v1.0.xlsx
    └── ...
```

---

## Sequência Recomendada

**Semana 1**: Blocos 1 + 2 (UX visual + cache rápido = impacto imediato)  
**Semana 2**: Bloco 4 (Plotly interativo = usuário vê dinâmica)  
**Semana 3-4**: Blocos 3 + 5 + 6 (infra pesada mas não breaking changes)

---

## Métricas de Sucesso

| Métrica | Baseline | Meta |
|---|---|---|
| Tempo de load (Geo page) | 3-5s | <1s |
| Eventos de "Re-running" por sessão | 5-8 | <2 |
| % de usuários exploram dados (clicks/interações) | ~30% | >70% |
| Tempo para entender pipeline (first time user) | ~10 min | <3 min |
| Taxa de erro pré-validação (com modelos) | ~25% | <10% |

---

## Registro de Avanço (2026-04-16)

- ✅ Bloco 1 (parcial): Design system criado e aplicado em páginas piloto.
- ✅ Bloco 2 (núcleo): Cache agressivo aplicado no geoprocessamento (ttl 3600).
- ✅ Bloco 3: Página `00_Pipeline_Status.py` entregue e deployada.
- ✅ Bloco 4 (núcleo): Abas Plotly interativas no Geoambiental (scatter, série temporal, distribuição).
- ✅ Bloco 6 (núcleo): Modelos oficiais para importação e Base Mestre, com Leia-me/Dicionário e downloads no início.
- 🔧 Hotfix aplicado: correção de `ImportError` em modelos oficiais via carregamento lazy do `openpyxl`.

## Próximos Passos (Atualizado)

1. **Estabilização (imediato)**:
  - revisar no Streamlit Cloud os fluxos de `00_Base_Mestre`, `01_Importacao`, `00_Pipeline_Status` e `6_Geoprocessamento`;
  - validar geração/download dos modelos para todos os grupos.
2. **Fidelidade final dos modelos de importação (curto prazo)**:
  - alinhar colunas da aba `Resultados_*` exatamente por script de migração;
  - revisar se `Metadados_Esforco` atende todos os grupos biológicos com exemplos mínimos.
3. **Fechamento do Bloco 1 (curto prazo)**:
  - completar padronização visual em `core/geoprocessamento/components.py`;
  - substituir métricas e alertas restantes por componentes do design system.
4. **Bloco 5 (médio prazo)**:
  - iniciar página `05_Qualidade_Dados.py` com cobertura temporal e alertas de anomalia.

---

**Status**: ✅ Em execução | 🚀 Avanço relevante concluído

