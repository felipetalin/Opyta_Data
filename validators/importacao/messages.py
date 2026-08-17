"""
validators/importacao/messages.py
Orientação de correção por código de achado e rótulos de aba, usados na
renderização do relatório de validação (ver render.py). Mantido separado do
código de checagem para facilitar revisão/atualização por quem não mexe em
lógica de validação.
"""
from __future__ import annotations

DEFAULT_FIX_GUIDANCE = (
    "Revise a aba indicada conforme a mensagem acima e valide a planilha novamente."
)

FIX_GUIDANCE: dict[str, str] = {
    # --- Estrutura do arquivo ---
    "FILE_OPEN_ERROR": "Confirme que o arquivo é um .xlsx válido e não está corrompido ou aberto em outro programa.",
    "GROUP_NOT_RECOGNIZED": "Selecione um grupo biológico válido na lista antes de validar.",
    "MISSING_SHEETS": "Baixe o modelo oficial deste grupo e copie os dados para dentro dele, sem renomear as abas.",
    "RESULTS_SHEET_NOT_FOUND": "Renomeie a aba de resultados exatamente como no modelo oficial do grupo selecionado.",
    "SHEET_PARSE_ERROR": "Verifique se a aba não tem células mescladas, fórmulas quebradas ou formatação incomum na linha de cabeçalho.",
    "EMPTY_CAPA": "Preencha a aba Capa_Projeto com pelo menos o Codigo_Opyta do projeto.",
    "EMPTY_PONTOS": "Preencha a aba Pontos_e_Campanhas com ao menos uma campanha e um ponto.",
    "EMPTY_RESULTADOS": "Preencha a aba de resultados do grupo com ao menos um registro.",

    # --- Campanhas ---
    "MISSING_CAMPAIGN_COLUMNS": "Confirme que a coluna 'Campanha' existe e está preenchida em todas as abas (Pontos_e_Campanhas, Metadados_Esforco, Resultados).",
    "CAMPAIGN_CODE_MISMATCH": "Copie o rótulo exato da campanha (texto idêntico, sem redigitar) de Pontos_e_Campanhas para as demais abas.",
    "CAMPAIGN_LABELS_COLLAPSE": "Duas grafias diferentes da campanha foram entendidas como a mesma (ex.: '10a' e '10ª'). Padronize para um único rótulo em todas as abas.",
    "CAMPAIGN_LABEL_NOT_IN_POINTS": "Adicione essa campanha/ponto em Pontos_e_Campanhas, ou corrija o nome se for erro de digitação.",

    # --- Pontos e coordenadas ---
    "INVALID_COORDINATES": "Verifique se as colunas de Latitude e Longitude não foram trocadas e se o formato é decimal (ex.: -20,1632), não graus/minutos/segundos.",
    "MISSING_POINT_CONFLICT_COLUMNS": "Confirme que as colunas Codigo_Opyta, Campanha, Ponto, Latitude e Longitude existem e estão nomeadas como no modelo oficial.",
    "EMPTY_PROJECT_CODE": "Preencha o campo Codigo_Opyta na aba Capa_Projeto com o código do projeto já cadastrado no Supabase.",
    "PROJECT_NOT_FOUND": "Confirme o Codigo_Opyta com quem administra os projetos antes de migrar; sem ele, não é possível checar conflito de coordenadas com o banco.",
    "DB_POINT_CONFLICT_CHECK_ERROR": "Tente validar novamente; se persistir, avise o responsável técnico (pode ser instabilidade de conexão com o banco).",
    "POINT_COORDINATE_DIVERGENCE": "Este ponto já existe no banco com outra coordenada. Confirme com quem fez campo qual coordenada é a correta, ou use um novo nome de ponto (ex.: sufixo -C02-01) se for um ponto realmente diferente.",

    # --- Espécies ---
    "NO_SPECIES_COLUMN": "Confirme que a aba de resultados tem uma coluna de espécie (ex.: Nome_Cientifico ou Especie).",
    "DB_SPECIES_SCHEMA_INCOMPLETE": "Este é um problema de estrutura do banco, não da sua planilha. Avise o responsável técnico.",
    "DB_QUERY_ERROR": "Tente validar novamente; se persistir, avise o responsável técnico (pode ser instabilidade de conexão com o banco).",
    "UNKNOWN_SPECIES": "Confirme se é erro de digitação do nome científico. Se for espécie realmente nova, cadastre-a na aba Cadastro_Especies/Especies desta mesma planilha com todos os campos preenchidos.",
    "INCOMPLETE_SPECIES_CATALOG": "Esta espécie já existe no banco, mas com cadastro incompleto. Isso é corrigido na Base Mestre (cadastro de espécies), não nesta planilha de importação.",

    # --- Esforço ---
    "INVALID_EFFORT": "O valor de esforço deve ser numérico e não-negativo. Use ponto ou vírgula como separador decimal.",
    "MISSING_CROSS_REFERENCE_COLUMNS": "Confirme que as colunas Campanha e Ponto existem em Resultados e em Pontos_e_Campanhas.",
    "INVALID_POINT_REFERENCE": "Corrija o nome da campanha ou do ponto para que sejam idênticos aos já cadastrados na aba Pontos_e_Campanhas (confira espaços e acentuação).",
    "EMPTY_EFFORT_SHEET": "Preencha a aba Metadados_Esforco: todo resultado precisa de um esforço correspondente.",
    "MISSING_EFFORT_LINK_COLUMNS": "Confirme que Campanha, Ponto, Metodo_de_Captura e Tipo_de_Amostragem existem tanto em Resultados quanto em Metadados_Esforco.",
    "EMPTY_EFFORT_KEYS": "Preencha ao menos uma linha completa (campanha+ponto+método+tipo) em Metadados_Esforco.",
    "INVALID_EFFORT_REFERENCE": "Confira se Campanha, Ponto, Método de Captura e Tipo de Amostragem estão escritos exatamente igual entre esta aba e Metadados_Esforco (inclusive maiúsculas/acentos).",

    # --- Cadastro de espécies embutido (prefixo CADASTRO_ESPECIES_*) ---
    "CADASTRO_ESPECIES_MISSING_REQUIRED_COLUMNS": "Baixe o modelo oficial de cadastro de espécies e confira se nenhuma coluna obrigatória foi removida ou renomeada.",
    "CADASTRO_ESPECIES_MISSING_REQUIRED_VALUE": "Preencha a célula indicada; se o valor for realmente desconhecido, combine com a equipe um marcador padrão (ex.: N/I) em vez de deixar vazio.",
    "CADASTRO_ESPECIES_SINGLE_WORD_NAME": "Use o formato 'Gênero epíteto' ou 'Gênero sp.' no nome científico (exceto para Zoobentos/Zooplâncton, que aceitam outros níveis taxonômicos).",
    "CADASTRO_ESPECIES_GENUS_MISMATCH": "Confirme que o campo Gênero corresponde à primeira palavra do Nome_Cientifico.",
    "CADASTRO_ESPECIES_INVALID_BMWP_SCORE": "O BMWP_Score deve ser numérico, ou um dos marcadores aceitos (N.A., N/I).",
    "CADASTRO_ESPECIES_INTRA_SHEET_DUPLICATE": "Duas linhas desta aba citam a mesma espécie; serão consolidadas automaticamente, mas confirme que os demais campos não divergem.",
}

SHEET_LABELS: dict[str | None, str] = {
    "capa": "Capa_Projeto",
    "pontos": "Pontos_e_Campanhas",
    "esforco": "Metadados_Esforco",
    "resultados": "Resultados_<Grupo>",
    "cadastro_especies": "Cadastro_Especies / Especies",
    None: "-",
}

# Abas cujas linhas afetadas podem ser editadas diretamente na tela (data_editor).
# Deixado de fora, por exemplo, o que depende de correção fora desta planilha
# (ex.: INCOMPLETE_SPECIES_CATALOG, que se resolve na Base Mestre).
EDITABLE_SHEET_KEYS = {"pontos", "esforco", "resultados", "cadastro_especies"}
