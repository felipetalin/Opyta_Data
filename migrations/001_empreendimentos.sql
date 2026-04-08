-- =============================================================================
-- MIGRATION 001 — SUPORTE A MÚLTIPLOS EMPREENDIMENTOS
-- =============================================================================
-- Contexto:
--   Um projeto pode conter vários empreendimentos/subprojetos.
--   Cada empreendimento possui seus próprios pontos, campanhas e resultados,
--   herdados via: pontos_coleta → esforcos_amostragem → resultados_*
--
-- ORDEM DE EXECUÇÃO: rodar no Supabase SQL Editor em uma única transação.
-- =============================================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Criar tabela empreendimentos
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS empreendimentos (
    id_empreendimento SERIAL PRIMARY KEY,
    id_projeto        INTEGER NOT NULL
                          REFERENCES projetos(id_projeto) ON DELETE CASCADE,
    nome              TEXT    NOT NULL,
    descricao         TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (id_projeto, nome)
);

COMMENT ON TABLE empreendimentos IS
    'Empreendimentos ou subprojetos dentro de um projeto Opyta.';
COMMENT ON COLUMN empreendimentos.id_projeto IS
    'FK para projetos — um empreendimento pertence a exatamente um projeto.';
COMMENT ON COLUMN empreendimentos.nome IS
    'Nome do empreendimento (único dentro do projeto).';

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Adicionar id_empreendimento em pontos_coleta (nullable → retrocompatível)
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE pontos_coleta
    ADD COLUMN IF NOT EXISTS id_empreendimento INTEGER
        REFERENCES empreendimentos(id_empreendimento) ON DELETE SET NULL;

COMMENT ON COLUMN pontos_coleta.id_empreendimento IS
    'FK opcional para empreendimentos. NULL = ponto sem empreendimento específico.';

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Substituir constraint única original por dois índices parciais
--
--    O índice parcial para IS NULL reproduz exatamente o comportamento antigo
--    de UNIQUE(id_projeto, id_campanha, nome_ponto) para todos os registros
--    atuais e futuros que não usam empreendimento.
--
--    O índice parcial para IS NOT NULL garante unicidade quando o campo é
--    preenchido, permitindo o mesmo nome_ponto em empreendimentos distintos
--    do mesmo (projeto, campanha).
-- ─────────────────────────────────────────────────────────────────────────────

-- 3a. Remover constraint original (ajuste o nome real conforme o Supabase)
--     Obtenha o nome exato com:
--       SELECT constraint_name FROM information_schema.table_constraints
--       WHERE table_name = 'pontos_coleta' AND constraint_type = 'UNIQUE';
--
--     Nomes comuns gerados pelo Supabase:
ALTER TABLE pontos_coleta
    DROP CONSTRAINT IF EXISTS pontos_coleta_id_projeto_id_campanha_nome_ponto_key;

-- 3b. Índice para linhas SEM empreendimento (comportamento original)
CREATE UNIQUE INDEX IF NOT EXISTS uq_pontos_coleta_sem_empreendimento
    ON pontos_coleta (id_projeto, id_campanha, nome_ponto)
    WHERE id_empreendimento IS NULL;

-- 3c. Índice para linhas COM empreendimento
CREATE UNIQUE INDEX IF NOT EXISTS uq_pontos_coleta_com_empreendimento
    ON pontos_coleta (id_projeto, id_campanha, id_empreendimento, nome_ponto)
    WHERE id_empreendimento IS NOT NULL;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Índice auxiliar de performance (empreendimentos por projeto)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_empreendimentos_id_projeto
    ON empreendimentos (id_projeto);

CREATE INDEX IF NOT EXISTS idx_pontos_coleta_id_empreendimento
    ON pontos_coleta (id_empreendimento)
    WHERE id_empreendimento IS NOT NULL;

COMMIT;

-- =============================================================================
-- PÓS-MIGRAÇÃO — verificação
-- =============================================================================
-- Rode após COMMIT para confirmar:
--
--   SELECT column_name, data_type, is_nullable
--   FROM information_schema.columns
--   WHERE table_name = 'pontos_coleta' AND column_name = 'id_empreendimento';
--
--   SELECT indexname, indexdef
--   FROM pg_indexes
--   WHERE tablename = 'pontos_coleta';
--
--   SELECT COUNT(*) FROM empreendimentos;
-- =============================================================================
