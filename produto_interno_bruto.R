source("X:/POWER BI/IBGE/ibge_pesquisas.R")

json_agregados$nome |> unique()

tabelas_ibge |>
  dplyr::filter(pesquisa_nome == "Produto Interno Bruto dos Municípios")

source("X:/POWER BI/IBGE/ibge_funcao_5.R")

# --- Decodificador de unidade de medida para silvicultura ---
metadados <- "https://servicodados.ibge.gov.br/api/v3/agregados/5938/metadados"
resposta <- httr::GET(metadados)
metadados_classificacao <- httr::content(resposta, "text", encoding = "UTF-8") |> 
  jsonlite::fromJSON(simplifyDataFrame = FALSE)


metadados_classificacao |> dplyr::glimpse()

# ============================================================================
# 1. DEFINIR PERÍODOS (ANOS) A PROCESSAR
# ============================================================================
ano_inicio <- lubridate::year(lubridate::today()) - 6
ano_fim   <- lubridate::year(lubridate::today())   # pode tentar até o ano atual, mas será pulado se não houver dados
anos <- seq(ano_inicio, ano_fim, by = 1)
anos |> dplyr::glimpse()

periodo <- ano_inicio

produto_interno_bruto_estados <- busca_ibge(5938, periodos = periodo, localidades = "N3[all]")

produto_interno_bruto_estados |> dplyr::glimpse()

produto_interno_bruto_estados |> print()


writexl::write_xlsx(produto_interno_bruto_estados, 
                    "produto_interno_bruto_estados.xlsx")


##################################################### Multiplicando por Mil ####

# Selecionar as colunas que terminam com "mil_reais"
colunas_mil <- names(produto_interno_bruto_estados)[endsWith(names(produto_interno_bruto_estados), "mil_reais")]

# Converter cada coluna de mil_reais para numérico, multiplicar por 1000 e renomear
produto_interno_bruto_estados <- dplyr::mutate(
  produto_interno_bruto_estados,
  dplyr::across(
    dplyr::all_of(colunas_mil),
    ~ as.numeric(ifelse(.x == "-", NA_character_, .x)) * 1000
  )
)

# Renomear as colunas trocando o sufixo _mil_reais por _reais
produto_interno_bruto_estados <- dplyr::rename_with(
  produto_interno_bruto_estados,
  ~ gsub("_mil_reais$", "_reais", .x),
  dplyr::all_of(colunas_mil)
)
#######  
# Converter ano para data (01-01-ano)
produto_interno_bruto_estados$periodo <- lubridate::make_date(
  year = as.numeric(produto_interno_bruto_estados$periodo),
  month = 1,
  day = 1
)

########################################################  separando as tabelas


# Tabela de impostos (colunas com "impostos_l_quidos_de_subs_dios")
produto_interno_bruto_estado_impostos <- dplyr::select(
  produto_interno_bruto_estados,
  localidade_id,
  localidade_nome,
  periodo,
  dplyr::contains("impostos_l_quidos_de_subs_dios")
)

# Tabela de valor adicionado (colunas com "valor_adicionado_bruto")
produto_interno_bruto_estados_valor_adicionado <- dplyr::select(
  produto_interno_bruto_estados,
  localidade_id,
  localidade_nome,
  periodo,
  dplyr::contains("valor_adicionado_bruto")
)

produto_interno_bruto_estado_impostos |> dplyr::glimpse()

produto_interno_bruto_estados_valor_adicionado |> dplyr::glimpse()


writexl::write_xlsx(produto_interno_bruto_estado_impostos, 
                    "produto_interno_bruto_estado_impostos.xlsx")

writexl::write_xlsx(produto_interno_bruto_estados_valor_adicionado, 
                    "produto_interno_bruto_estados_valor_adicionado.xlsx")

############################# produto_interno_bruto_estados_valor_adicionado

# Transformar a tabela original no formato longo e depois wide por setor
produto_interno_bruto_estados_valor_adicionado <-
  produto_interno_bruto_estados_valor_adicionado |>
  tidyr::pivot_longer(
    cols = -c(localidade_id, localidade_nome, periodo),
    names_to = "variavel",
    values_to = "valor"
  ) |>
  dplyr::mutate(
    # Identificar o setor econômico
    setor = dplyr::case_when(
      stringr::str_starts(variavel, "valor_adicionado_bruto_a_pre_os_correntes_total") ~ "total",
      stringr::str_starts(variavel, "valor_adicionado_bruto_a_pre_os_correntes_da_agropecu_ria") ~ "agropecuaria",
      stringr::str_starts(variavel, "valor_adicionado_bruto_a_pre_os_correntes_da_ind_stria") ~ "industria",
      stringr::str_starts(variavel, "valor_adicionado_bruto_a_pre_os_correntes_dos_servi_os_exclusive") ~ "servicos",
      stringr::str_starts(variavel, "valor_adicionado_bruto_a_pre_os_correntes_da_administra_o_defesa") ~ "administracao",
      TRUE ~ NA_character_
    ),
    # Identificar o nível (absoluto ou participação geográfica)
    nivel = dplyr::case_when(
      stringr::str_detect(variavel, "participa_o.*_da_microrregi") ~ "microrregiao",
      stringr::str_detect(variavel, "participa_o.*_da_mesorregi") ~ "mesorregiao",
      stringr::str_detect(variavel, "participa_o.*_da_unidade_da_federa") ~ "uf",
      stringr::str_detect(variavel, "participa_o.*_da_grande_regi") ~ "grande_regiao",
      stringr::str_detect(variavel, "participa_o.*_do_brasil") ~ "brasil",
      stringr::str_starts(variavel, "valor_adicionado") ~ "valor_absoluto",
      TRUE ~ NA_character_
    ),
    # Converter valor para numérico (hífen vira NA)
    valor = as.numeric(ifelse(valor == "-", NA_character_, valor))
  ) |>
  dplyr::filter(!is.na(setor), !is.na(nivel)) |>
  # Transformar os níveis em colunas novamente, agrupando por setor
  tidyr::pivot_wider(
    id_cols = c(localidade_id, localidade_nome, periodo, setor),
    names_from = nivel,
    values_from = valor
  ) |>
  # Renomear as colunas conforme solicitado
  dplyr::rename(
    valor_adicionado_bruto_a_pre_os_correntes_reais = valor_absoluto,
    participa_o_do_valor_adicionado_bruto_a_pre_os_correntes_total_no_valor_adicionado_bruto_a_pre_os_correntes_da_microrregi_o_geogr_fica_ = microrregiao,
    participa_o_do_valor_adicionado_bruto_a_pre_os_correntes_total_no_valor_adicionado_bruto_a_pre_os_correntes_da_mesorregi_o_geogr_fica_ = mesorregiao,
    participa_o_do_valor_adicionado_bruto_a_pre_os_correntes_total_no_valor_adicionado_bruto_a_pre_os_correntes_da_unidade_da_federa_o_ = uf,
    participa_o_do_valor_adicionado_bruto_a_pre_os_correntes_total_no_valor_adicionado_bruto_a_pre_os_correntes_da_grande_regi_o_ = grande_regiao,
    participa_o_do_valor_adicionado_bruto_a_pre_os_correntes_total_no_valor_adicionado_bruto_a_pre_os_correntes_do_brasil_ = brasil
  )



















