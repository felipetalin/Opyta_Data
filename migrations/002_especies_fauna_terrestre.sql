-- =============================================================================
-- MIGRATION 002 — EXPANSÃO DO CADASTRO DE ESPÉCIES (FAUNA TERRESTRE)
-- =============================================================================
-- Contexto:
--   A tabela especies já suporta atributos ecológicos de grupos aquáticos
--   (bmwp_score, status_ameaca_*, habito_alimentar…).
--   Adicionamos aqui campos relevantes para grupos terrestres:
--   Avifauna, Herpetofauna e Mastofauna, sem quebrar compatibilidade.
--
-- Todos os campos são nullable → dados existentes não são afetados.
-- ORDEM DE EXECUÇÃO: rodar no Supabase SQL Editor.
-- =============================================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────────────────────
-- status_estadual: listas estaduais de ameaça (ex: "EN - Minas Gerais 2021")
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE especies
    ADD COLUMN IF NOT EXISTS status_estadual TEXT;

COMMENT ON COLUMN especies.status_estadual IS
    'Status de ameaça em lista estadual (ex: "VU — COPAM/MG 2023").';

-- ─────────────────────────────────────────────────────────────────────────────
-- status_copam: enquadramento específico na Deliberação COPAM
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE especies
    ADD COLUMN IF NOT EXISTS status_copam TEXT;

COMMENT ON COLUMN especies.status_copam IS
    'Enquadramento na lista COPAM/MG (ex: "EN", "VU", "LC").';

-- ─────────────────────────────────────────────────────────────────────────────
-- cites: apêndice CITES de comércio internacional
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE especies
    ADD COLUMN IF NOT EXISTS cites TEXT;

COMMENT ON COLUMN especies.cites IS
    'Apêndice CITES (I, II, III ou NULL se não listada).';

-- ─────────────────────────────────────────────────────────────────────────────
-- guilda_alimentar: categoria trófica funcional
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE especies
    ADD COLUMN IF NOT EXISTS guilda_alimentar TEXT;

COMMENT ON COLUMN especies.guilda_alimentar IS
    'Guilda alimentar funcional (ex: "Insetívoro", "Frugívoro", "Carnívoro").';

-- ─────────────────────────────────────────────────────────────────────────────
-- dependencia_florestal: grau de dependência de habitats florestais
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE especies
    ADD COLUMN IF NOT EXISTS dependencia_florestal TEXT;

COMMENT ON COLUMN especies.dependencia_florestal IS
    'Dependência de florestas (ex: "Alta", "Média", "Baixa", "Independente").';

-- ─────────────────────────────────────────────────────────────────────────────
-- endemismo: categoria de endemismo geográfico (coluna texto, distinto de
--            endemismo_especies que armazena mapeamento bacia/bioma)
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE especies
    ADD COLUMN IF NOT EXISTS endemismo TEXT;

COMMENT ON COLUMN especies.endemismo IS
    'Categoria de endemismo geográfico (ex: "Endêmica do Brasil", "Cerrado").';

-- ─────────────────────────────────────────────────────────────────────────────
-- sensibilidade_ambiental: resposta a distúrbios antrópicos
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE especies
    ADD COLUMN IF NOT EXISTS sensibilidade_ambiental TEXT;

COMMENT ON COLUMN especies.sensibilidade_ambiental IS
    'Sensibilidade a distúrbios (ex: "Alta", "Média", "Baixa").';

-- ─────────────────────────────────────────────────────────────────────────────
-- migratorio: indicação de comportamento migratório
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE especies
    ADD COLUMN IF NOT EXISTS migratorio TEXT;

COMMENT ON COLUMN especies.migratorio IS
    'Comportamento migratório (ex: "Sim", "Não", "Parcialmente").';

-- ─────────────────────────────────────────────────────────────────────────────
-- raridade: grau de raridade local/regional
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE especies
    ADD COLUMN IF NOT EXISTS raridade TEXT;

COMMENT ON COLUMN especies.raridade IS
    'Raridade local ou regional (ex: "Rara", "Comum", "Muito Rara").';

COMMIT;

-- =============================================================================
-- PÓS-MIGRAÇÃO — verificação
-- =============================================================================
-- Confirme que as 9 colunas existem:
--
--   SELECT column_name, data_type, is_nullable
--   FROM information_schema.columns
--   WHERE table_name = 'especies'
--     AND column_name IN (
--         'status_estadual','status_copam','cites','guilda_alimentar',
--         'dependencia_florestal','endemismo','sensibilidade_ambiental',
--         'migratorio','raridade'
--     )
--   ORDER BY ordinal_position;
-- =============================================================================
