# Exceções de Regras de Validação por Grupo Biológico

Documento que registra as exceções de regras de validação de espécies conforme particularidades de cada grupo biológico são descobertas.

---

## 📋 Estrutura

Cada grupo pode ter duas tipos de exceção:

1. **`skip_genus_check`** — Táxons que não têm gênero bem-definido
   - Pula a validação de coerência entre `Genero` e primeira palavra de `Nome_Cientifico`
   - Útil para táxons parciaes, ordens, cílios, etc.

2. **`single_word_valid`** — Nomes científicos com uma única palavra que são válidos
   - Pula o warning de `SINGLE_WORD_NAME`
   - Útil para táxons taxonomicamente válidos como Bdelloida, Monogenonta, etc.

---

## 🔬 Exceções Atuais

### Zooplâncton

**Táxons especiais sem gênero bem-definido** (adicionados em commits `02967f9`, `3944892`):

```python
"Zooplâncton": {
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
    "single_word_valid": {
        "Bdelloida",
    },
}
```

**Motivo:** Esses táxons (cílios, ordens larvais e grupos de rotíferos) não seguem a nomenclatura binomial tradicional e são representantes de níveis taxonômicos acima de espécie. Não possuem gênero bem-definido e devem ser aceitos com `Genero = "N.A."`.

### Bentos

**Táxons especiais** (adicionados em commit `3944892`):

```python
"Bentos": {
    "skip_genus_check": {
        "Mitilideo sp.",
    },
}
```

**Motivo:** Mitilídeos são moluscos bivalves que podem aparecer apenas no nível de família/subfamília (Mytilidae), sem espécie bem-definida. A forma "Mitilideo sp." é válida quando a identificação não chega ao nível de espécie.

**Exemplo de linha válida (sem warnings):**
| Nome_Cientifico | Grupo_Biologico | Genero |
|---|---|---|
| Ciliado ni | Zooplâncton | N.A. |
| Cyclopoida (nauplius) | Zooplâncton | N.A. |
| Bdelloida | Zooplâncton | N.A. |

---

## ➕ Como Adicionar Novas Exceções

1. **Editar arquivo:** `validators/especies/rules.py`

2. **Localizar:** O dicionário `_EXCEPTIONS_BY_GROUP` no início do arquivo

3. **Adicionar entrada** para novo grupo (ou estender existente):

   ```python
   "Meu Grupo": {
       "skip_genus_check": {
           "Táxon nivel alto 1",
           "Táxon nivel alto 2",
       },
       "single_word_valid": {
           "Táxon Válido",
       },
   }
   ```

4. **Commit:** Com mensagem clara indicando o grupo e motivo
   ```
   feat(validators): adicionar exceções para {Grupo}
   
   - {descrição da particularidade}
   - referência ao commit que adicionou ou ao feedback do usuário
   ```

5. **Atualizar este documento** com a nova entrada

---

## 🧪 Testando Exceções Localmente

```python
from validators.especies.rules import _is_exception_for_group

# Test 1: Verificar se um táxon é exceção
resultado = _is_exception_for_group(
    "Ciliado ni",
    "Zooplâncton",
    "skip_genus_check"
)
print(resultado)  # True

# Test 2: Validar planilha inteira
from validators.especies import validate_especies_file
from core.engine import get_engine

report = validate_especies_file(arquivo_excel, engine=get_engine())

if report.can_proceed:
    print("Planilha válida!")
else:
    print(f"Bloqueios: {len(report.blocks)}")
    print(f"Warnings: {len(report.warnings)}")
```

---

## 📝 Protocolo para Novos Grupos

Quando um novo grupo gerar warnings ou bloqueios recorrentes:

1. **Diagnóstico:** Executar validação e documentar quais são os TAXones que geram problemas

2. **Classificação:** Determinar se a exceção é:
   - Táxon sem gênero (→ `skip_genus_check`)
   - Táxon nome único válido (→ `single_word_valid`)
   - Outra particularidade (→ pode exigir nova regra)

3. **Implementação:** Adicionar à lista apropriada

4. **Teste:** Validar com dados reais do grupo

5. **Documentação:** Atualizar este arquivo com exemplos

---

## 🔗 Referências

- **Implementação:** [validators/especies/rules.py](rules.py) — função `_is_exception_for_group()`
- **Uso no pipeline:** [validators/especies/pipeline.py](pipeline.py) — chamado durante `run_rules()`
- **UI:** Warnings de `GENUS_MISMATCH` e `SINGLE_WORD_NAME` deixam de aparecer para táxons em exceção

---

## 📊 Estatísticas

| Grupo | Exceções skip_genus_check | Exceções single_word_valid | Data Adição |
|---|---|---|---|
| Zooplâncton | 8 | 1 | 2026-04-09 |
| Bentos | 1 | 0 | 2026-04-09 |
| *Próximo grupo* | — | — | — |
