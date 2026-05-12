# Estratégia Arquitetônica - Decisões Tecnológicas

**Data**: 2026-04-16  
**Decisão**: Manter Streamlit como framework principal (por enquanto)  
**Status**: ✅ Aprovado

---

## Análise: Streamlit vs Next.js + Vercel

### Contexto
Reflexão sobre migração total do Streamlit para Next.js (Vercel), motivada pela dinâmica superior do Next.js e UX mais flexível.

### Comparação Técnica

| Aspecto | Streamlit | Next.js |
|---|---|---|
| **Execução** | Re-executa script a cada interação | Carregamento eficiente, estados locais |
| **Análises Python** | Direto (pandas, numpy, scipy) | Precisa backend separado |
| **Linguagens** | 1 (Python) | 2 (Node.js + Python backend) |
| **Deploy** | Simples (Streamlit Cloud) | Complexo (frontend + backend) |
| **UX Dinâmica** | Limitada | Excelente |
| **Escalabilidade** | Dolorosa >100 usuários | Infraestrutura robusta |

### Análise de Custo-Benefício

#### ❌ Migração Total (Não recomendado)
- **Esforço**: 3-4 meses de desenvolvimento
- **Risco**: Duplicar lógica em JS ou manter Python internamente (pior dos dois mundos)
- **ROI**: Positivo em UX, negativo em velocidade de entrega
- **Viável**: Sim, mas custo muito alto neste momento

#### ✅ Arquitetura Híbrida (Recomendado para futuro)
```
Next.js Frontend (UI dinâmica, read-only)
    ↓ (API HTTP)
FastAPI/Flask Backend (Python, análises complexas)
    ↓
PostgreSQL (Supabase)
```
- **Esforço**: ~3 semanas
- **ROI**: Alto (UI moderna + análises mantidas)
- **Timeline**: Med-term (2-3 meses)

#### ✅ Upgrade Streamlit (Estratégia imediata)
- Customizar tema e componentes
- Usar `st.cache_data` agressivamente
- Adicionar Plotly Dash para dashboards dinâmicos
- **Esforço**: 1-2 semanas
- **ROI**: Rápido, baixo risco

---

## Estado Atual do Opyta

✅ **Análises sofisticadas**: Shannon, Pielou, IQA, agregações geo complexas (Python-nativas)  
✅ **Volume de dados**: Crescente (múltiplas campanhas, espécies, pontos)  
✅ **Usuários operacionais**: Admins, analistas (não casual users)  
❓ **Necessidade de dinâmica real-time**: Não crítica no MVP

---

## Estratégia de Evolução

### Fase 1: IMEDIATO (Próx. 1-2 meses)
- **Ação**: Manter Streamlit como é
- **Melhorias**: Otimizar cache, melhorar UX com Plotly, temas
- **Objetivo**: Sistema "apresentável" conforme META_PROXIMAS_ETAPAS.txt

### Fase 2: MÉDIO PRAZO (2-3 meses)
- **Ação**: Criar Portal Next.js READ-ONLY (como originalmente planejado)
- **Dados**: Pré-processados, consumidos via API Streamlit existente
- **Objetivo**: Experiência read-only bonita para stakeholders não-técnicos
- **Impacto no Streamlit**: Zero (coexistem)

### Fase 3: LONGO PRAZO (4+ meses)
- **Decisão**: Se portal se torna crítico
- **Ação**: Evoluir para arquitetura híbrida (separar análises em FastAPI)
- **Ganho**: Scalability, independência de linguagem, APIs reutilizáveis

---

## Por que Manter Streamlit AGORA

1. **Análises complexas**: Biota, físico, geo requerem Python. Não vale reescrever em JS.
2. **Velocidade de entrega**: Streamlit permite prototipagem rápida. Você já tá em fase operacional.
3. **Equipe**: Seu conhecimento é Python. Não é o momento de agregar complexidade (2 linguagens).
4. **Risco**: Migração total = risco alto de regressão. Incrementalismo = risco baixo.
5. **ROI**: Melhorias no Streamlit > Reescrever tudo em Next.js (por enquanto).

---

## Próximos Passos

- ✅ Documentado: Estratégia híbrida como roadmap longo-prazo
- ⏳ Imediato: Proceder com Portal Next.js READ-ONLY (como planejado, isolado)
- ⏳ Streamlit: Continue operacional, sem mudanças de arquitetura

---

**Decisão tomada**: Streamlit é a ferramenta certa para análises complexas. É uma decisão arquitetônica sólida, não uma limitação.
