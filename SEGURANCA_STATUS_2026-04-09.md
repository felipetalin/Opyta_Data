# Segurança - Status da Sessão (2026-04-09)

## Objetivo
Registrar o que foi feito na frente de segurança, o que está ativo hoje e quais são os próximos passos recomendados.

## O que foi implementado

### 1) Consolidação em modo seguro
Arquivo principal: app/pages/02_Consolidacao.py

- Simulação antes da gravação real.
- Confirmação explícita para permitir gravação real.
- Backup lógico automático antes da consolidação real.
- Rollback seguro por seleção de backup.
- Snapshot de segurança antes da restauração.
- Bloqueio da consolidação real quando a simulação falha.

### 2) Endurecimento da autenticação
Arquivo principal: app/main.py

- Remoção de credenciais hardcoded no código.
- Autenticação por AUTH_USERS_JSON com hash PBKDF2.
- Bloqueio temporário após tentativas inválidas de login (lockout).
- Mensagens de erro e contagem de tentativas para reduzir brute force.

### 3) Sessão e logout
Arquivos principais: core/sidebar.py e app/main.py

- Expiração de sessão por inatividade.
- Limpeza de estado no logout.
- Atualização de último acesso após login válido.

### 4) Higiene de repositório
Arquivo principal: .gitignore

- Inclusão de runtime/ para evitar commit acidental de artefatos operacionais e arquivos temporários.

## Estado atual

- Deploy atualizado no branch deploy-cloud.
- Segurança funcional validada com a equipe durante a sessão.
- Login operacional com AUTH_USERS_JSON configurado no ambiente.
- Senha temporária unificada mantida por decisão operacional da equipe (mudança futura já planejada).

## Riscos que permanecem (aceitos temporariamente)

- Senha temporária simples (123456) compartilhada entre usuários.
- Ausência de MFA na autenticação do app.
- Ausência de RBAC completo por perfil para separar permissões por operação crítica.

## Próximos passos recomendados

### Prioridade alta
1. Trocar senha única por senhas fortes individuais por usuário.
2. Implementar perfil de permissão (RBAC): leitura, operação e administração.
3. Restringir operações críticas (consolidação e rollback) apenas para perfis autorizados.

### Prioridade média
4. Adicionar auditoria de segurança com trilha de usuário, IP, ação e timestamp para operações críticas.
5. Revisar privilégios da conta de banco usada pela aplicação (princípio do menor privilégio).
6. Habilitar e exigir 2FA no GitHub e no provedor de deploy.

### Prioridade contínua
7. Revisão periódica de secrets no ambiente e rotação de chaves.
8. Testes de acesso (login, lockout e sessão expirada) a cada release relevante.

## Como usar este documento

- Consulte este arquivo antes de nova alteração de segurança.
- Atualize este registro ao final de cada etapa importante.
- Em caso de incidente, use esta linha do tempo como base para diagnóstico.
