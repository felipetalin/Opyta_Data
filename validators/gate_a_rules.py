from __future__ import annotations

GATE_A_SPECIES_RULES = [
    "A aba Especies deve existir e usar cabecalhos reconhecidos.",
    "Nome_Cientifico e Grupo_Biologico sao obrigatorios.",
    "Classificacao taxonomica completa e obrigatoria: Reino, Filo, Classe, Ordem, Familia e Genero.",
    "Categorias obrigatorias: Status_Ameaca_Nacional, Status_Ameaca_Global, Origem, Habito_Alimentar, Estrategia_Reprodutiva, Valor_Economico, Cinegetica e Xerimbabo.",
    "Nomes cientificos duplicados, genero incoerente e valores nao numericos em campos tecnicos devem ser revisados.",
]

GATE_A_IMPORT_RULES = [
    "Codigo_Opyta deve estar preenchido na Capa_Projeto.",
    "Campanhas sao contadas pelo codigo operacional C###, mesmo que o rotulo contenha ano/mes.",
    "Rotulos de campanha em Resultados e Metadados_Esforco devem existir em Pontos_e_Campanhas.",
    "Todo resultado deve referenciar um par Campanha + Ponto existente em Pontos_e_Campanhas.",
    "Todo resultado deve ter esforco correspondente por Campanha + Ponto + Metodo_de_Captura + Tipo_de_Amostragem.",
    "Coordenadas devem ser validas e convertiveis para numero decimal.",
    "Toda especie usada nos resultados deve existir no cadastro mestre ou na aba Especies/Cadastro_Especies da mesma planilha.",
    "Especies existentes no banco tambem precisam ter cadastro mestre completo; especie com buraco bloqueia a importacao.",
]

GATE_A_UPDATE_NOTES = [
    "Quando um novo erro real for encontrado em projeto, transforme em regra de bloqueio ou aviso aqui e no validador correspondente.",
    "Bloqueios devem impedir cadastro/migracao; avisos devem indicar revisao sem travar o fluxo.",
    "A mensagem do erro deve apontar aba, chave e exemplos suficientes para correcao na planilha.",
]
