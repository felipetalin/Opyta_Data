# Opyta Data Deployment Checklist

> 🎯 **REGRA DE OURO:** Após fazer commit local, **SEMPRE** faça `git push origin deploy-cloud` IMEDIATAMENTE. Caso contrário, a versão web não verá as mudanças mesmo que o local esteja correto.

## O Erro Comum (Que Sempre Acontece)

**Sintoma:** "O código local está certo mas a web não mostra as mudanças nem depois de reboot/refresh"

**Causa:** `git commit` foi feito mas `git push` não foi feito.

**Confir**:
```bash
git log --oneline -3                    # vê 3 commits locais
git log --oneline -3 origin/deploy-cloud # vê 3 commits remotos
```

Se o commit mais recente está apenas no primeiro comando, **não foi pushado**.

**Solução:**
```bash
git push origin deploy-cloud
```

Depois disso, o deploy automaticamente puxa as mudanças.

---

## Situação identificada

- O código local estava correto em `core/sidebar.py`, `app/main.py` e `app/pages/03_Analises.py`.
- A versão web continuava exibindo a interface antiga porque o deploy estava usando uma versão anterior do branch.
- O problema não era um bug de lógica na sidebar, mas uma versão desatualizada da aplicação em produção.

## Causa raiz

- Modificações locais não estavam commitadas e pushadas para o branch de deploy (`deploy-cloud`).
- O ambiente web estava rodando uma versão antiga desse branch.

## Problema adicional já identificado

**Sintoma:** o editor mostra a página já alterada, mas a versão web continua antiga mesmo após commit, push e reboot.

**Causa possível:** o conteúdo alterado estava apenas no buffer do editor ou havia divergência entre o arquivo físico no disco e o conteúdo inspecionado durante a sessão. Na prática, isso fez parecer que a mudança estava pronta, mas o `HEAD` remoto ainda continha a versão antiga.

**Como validar antes de publicar:**
```bash
git show HEAD:app/pages/01_Importacao.py
Get-Content app/pages/01_Importacao.py
git diff -- app/pages/01_Importacao.py
```

**Regra operacional:** quando a interface web não refletir uma alteração, confirmar nesta ordem:

1. o arquivo físico no disco contém o bloco novo
2. o `git diff` mostra a mudança esperada
3. o commit contém a mudança esperada
4. o `origin/deploy-cloud` contém esse commit

Se qualquer uma dessas etapas falhar, o problema não é reboot nem cache, e sim divergência entre arquivo editado, conteúdo commitado e versão publicada.

## Passo a passo para validar antes do deploy

1. Confirme a branch atual:
   ```bash
   git branch --show-current
   ```

2. Verifique as alterações não commitadas:
   ```bash
   git status --short
   ```

3. Faça commit das alterações relevantes:
   ```bash
   git add <arquivos>
   git commit -m "mensagem descritiva"
   ```

4. **IMEDIATAMENTE após commit, faça push:**
   ```bash
   git push origin deploy-cloud
   ```
   ⚠️ **NUNCA PULE ESTE PASSO** — Este é o passo que causa o problema se esquecido.

5. Confira se o push foi bem-sucedido:
   ```bash
   git log --oneline -1 origin/deploy-cloud
   ```
   O commit deve aparecer aqui.

5.1. Quando a mudança for visual, confirme o conteúdo do arquivo publicado:
   ```bash
   git show HEAD:app/pages/01_Importacao.py
   ```
   Se o bloco novo não aparecer aqui, ele ainda não entrou no commit real.

6. Verifique se o deploy usa o branch correto (`deploy-cloud`).

7. Reinicie o servidor/serviço web após o deploy.

## Checklist de deploy

- [ ] Branch local correta (`deploy-cloud`)
- [ ] `git status` limpo após commit
- [ ] **`git push origin deploy-cloud` FEITO IMEDIATAMENTE após commit** ⚠️ (Este é o passo crítico)
- [ ] Verificado que o commit está em `origin/deploy-cloud`
- [ ] Deploy disparado na plataforma correta
- [ ] Servidor web reiniciado após deploy
- [ ] Verificação da interface web atualizada

## Observações específicas para a sidebar

A sidebar deve ser montada apenas em `core/sidebar.py` com a estrutura:

- DADOS
  - Início
  - Base Mestre
  - Importação
  - Consolidação
  - Análises
- SAÍDA
  - Exportação

Se a versão web mostrar algo diferente, o primeiro diagnóstico deve ser:

- foi feito deploy da branch errada?
- o servidor foi reiniciado?
- a versão web está vindo de cache ou de outro ambiente?
