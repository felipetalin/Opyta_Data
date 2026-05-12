# Matriz de Priorização - Melhorias Streamlit

**Legenda**:  
🚀 = Alto impacto + fácil (Faça AGORA)  
⚡ = Alto impacto + médio (Depois de 🚀)  
📌 = Médio impacto + fácil (Para manter momentum)  
🎯 = Futuro (Mais complexo, strategic)

---

## Matriz 2x2 (Impacto vs Esforço)

```
ALTO IMPACTO
      ↑
      │
🚀    │   ⚡
      │
      │
      └────────────→ BAIXO ESFORÇO
📌    │   🎯
      │
```

Detalhado:

### 🚀 Faça AGORA (Impacto Alto + Esforço Baixo)

**Bloco 2a: Cache Agressivo (2-3 dias)**
- Implementar `@st.cache_data` em carregar_dados_geo() + queries
- ROI: Page geo recarrega em 500ms → Usuário acha "rápido"
- Code: <100 linhas de mudança
- Risco: Muito baixo

**Bloco 1a: Design System (4 dias)**
- Criar `render_metric_card()`, `render_status_badge()`, `render_section_header()`
- ROI: Visual coerência em 2-3 páginas = percepção de "produto pronto"
- Code: ~200 linhas (templates reutilizáveis)
- Risco: Baixo (aditivo, não quebra existente)

---

### ⚡ Depois Faça (Impacto Alto + Esforço Médio)

**Bloco 4: Plotly Interativo (1-2 semanas)**
- Scatter plot clicável no mapa
- Série temporal com hover
- ROI: Sente dinâmica + explore sem re-run
- Code: ~300-400 linhas (Plotly é poderoso, poucas linhas fazem muito)
- Risco: Médio (API Plotly estável, Streamlit integra bem)

**Bloco 3: Pipeline Status (1 semana)**
- Nova página mostrando progress real-time
- ROI: Usuário VIRA FÃ (sente confiança no sistema)
- Code: ~200 linhas (queries simples, Plotly timeline)
- Risco: Médio-baixo (depende de logging estruturado)

---

### 📌 Para Manter Momentum (Impacto Médio + Esforço Baixo)

**Bloco 6: Modelos Oficiais (3-4 dias)**
- Criar 3-4 Excel templates com instruções embutidas
- ROI: Reduz erros de validação em >50%
- Code: 0 (é documentação + design no Excel)
- Risco: Nenhum

---

### 🎯 Futuro (Complexidade Alta)

**Bloco 5: Auditoria & Qualidade (2-3 semanas)**
- Requer refactor em logging + nova página
- ROI: Longo-prazo (confiança de dados)
- Code: ~500 linhas + schema DB
- Risco: Médio (precisa coordenar dados históricos)

---

## Cronograma Recomendado

### **Semana 1 (Próx. 5-7 dias)**
- **Mon-Tue**: Bloco 2a (Cache) — COMEÇA HOJE SE QUER IMPACTO RÁPIDO
- **Wed-Thu**: Bloco 1a (Design System)
- **Fri**: Testes + deploy em teste

### **Semana 2 (7-14 dias)**
- **Mon-Wed**: Bloco 4 (Plotly)
- **Thu-Fri**: Bloco 3 (Pipeline Status) — começa estruturação

### **Semana 3 (14-21 dias)**
- **Mon-Tue**: Bloco 6 (Modelos)
- **Wed+**: Bloco 5 (Auditoria) — depende de feedback usuários

---

## Qual Começar HOJE?

**Prioridade #1**: Bloco 2a (Cache)
- **Por quê?**: Impacto visual imediato (usuário nota <1s load)
- **Como começa?**: Abrir `core/geoprocessamento/services.py`, envolver `carregar_dados_geo()` em `@st.cache_data(ttl=3600)`
- **Tempo**: 30-45 minutos
- **Risco**: Nenhum (pode revert imediatamente se quebrar)

**Prioridade #2**: Bloco 1a (Design System)
- **Por quê?**: Visual coerência = percepção de "produto mature"
- **Como começa?**: Criar `core/ui/design_system.py` com 3 funções básicas
- **Tempo**: 2-3 horas
- **Risco**: Baixo (aditivo)

---

## Quer Que Eu Comece?

- **Opção A**: Começar Bloco 2a (Cache) AGORA — está em 30 min
- **Opção B**: Planejar tudo primeiro, depois executar em sprint
- **Opção C**: Escolha outro bloco (qual te interessa mais?)

