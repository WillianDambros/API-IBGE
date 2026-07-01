# ============================================================================
# SCRIPT: PIB dos Municípios (Agregado 5938) + Índice de Gini (5939)
# ============================================================================

# Carregar pacotes necessários
#library(dplyr)
#library(stringr)
#library(fuzzyjoin)
#library(readr)
#library(purrr)
#library(httr)
#library(jsonlite)
#library(lubridate)
#library(curl)
#library(readxl)
#library(RPostgres)
#library(DBI)

# Fonte da função de busca do IBGE (ajuste o caminho)
source("X:/POWER BI/IBGE/ibge_funcao_5.R")

# ============================================================================
# 1. CONFIGURAÇÕES INICIAIS
# ============================================================================

# Período: últimos 20 anos completos (ajuste conforme necessidade)
ano_inicio <- lubridate::year(lubridate::today()) - 20
ano_fim   <- lubridate::year(lubridate::today())  # evita ano incompleto
anos <- seq(ano_inicio, ano_fim, by = 1)

# ============================================================================
# 2. DECODIFICADOR TERRITORIAL PARA MUNICÍPIOS DO MT
# ============================================================================

compilado_decodificador_endereco <- paste0(
  "https://github.com/WillianDambros/data_source/raw/",
  "refs/heads/main/compilado_decodificador.xlsx"
)
decodificador_endereco <- paste0(getwd(), "/compilado_decodificador.xlsx")
curl::curl_download(compilado_decodificador_endereco, decodificador_endereco)

territorialidade_mt <- readxl::read_excel(
  decodificador_endereco,
  sheet = "territorialidade_municipios_mt",
  col_types = "text"
)

normalizar <- function(x) {
  x |>
    stringr::str_to_lower() |>
    stringr::str_replace_all(" \\(mt\\)", "") |>
    stringr::str_replace_all("[^a-z0-9 ]", "") |>
    stringr::str_squish()
}

territorialidade_mt <- territorialidade_mt |>
  dplyr::select(
    territorio_geo_munícipios,
    rpseplan10340_munícipio_polo_decodificado,
    rpseplan10340_regiao_decodificado,
    imeia_regiao,
    imeia_municipios_polo_economico,
    territorio_latitude,
    territorio_longitude
  ) |>
  dplyr::rename(municipio_decod_original = territorio_geo_munícipios) |>
  dplyr::mutate(
    municipio_norm = normalizar(municipio_decod_original),
    dplyr::across(
      c(territorio_latitude, territorio_longitude),
      ~ readr::parse_number(.x, locale = readr::locale(decimal_mark = ","))
    )
  ) |>
  dplyr::filter(!is.na(municipio_norm))

# ============================================================================
# 3. FUNÇÕES DE PÓS-PROCESSAMENTO PARA PIB
# ============================================================================

# Converte mil_reais para reais, participações para numérico, e período para data
processar_pib_bruto <- function(df) {
  # 1. Converter colunas mil_reais para reais (multiplicar por 1000)
  colunas_mil <- names(df)[endsWith(names(df), "mil_reais")]
  if (length(colunas_mil) > 0) {
    df <- df |>
      dplyr::mutate(dplyr::across(dplyr::all_of(colunas_mil), ~ as.numeric(.x) * 1000)) |>
      dplyr::rename_with(~ gsub("_mil_reais$", "_reais", .x), dplyr::all_of(colunas_mil))
  }
  
  # 2. Converter colunas de participação (começam com "participa_o") para numérico
  colunas_participacao <- names(df)[startsWith(names(df), "participa_o")]
  if (length(colunas_participacao) > 0) {
    df <- df |>
      dplyr::mutate(dplyr::across(dplyr::all_of(colunas_participacao), ~ as.numeric(.x)))
  }
  
  # 3. Converter TODAS as outras colunas (que não são de identificação) para numérico
  colunas_identificacao <- c("localidade_id", "localidade_nome", "periodo")
  colunas_para_converter <- setdiff(names(df), colunas_identificacao)
  colunas_para_converter <- setdiff(colunas_para_converter, c(colunas_mil, colunas_participacao))
  
  if (length(colunas_para_converter) > 0) {
    df <- df |>
      dplyr::mutate(dplyr::across(dplyr::all_of(colunas_para_converter), ~ as.numeric(.x)))
  }
  
  # 4. Converter periodo para Date
  df$periodo <- lubridate::make_date(year = as.numeric(df$periodo), month = 1, day = 1)
  
  return(df)
}

# Separa e transforma os dados em duas tabelas: macro e setores
transformar_pib_long <- function(df) {
  # --- Tabela macro (PIB + impostos + VAB total) ---
  tabela_macro <- df |>
    dplyr::select(
      localidade_id, localidade_nome, periodo,
      dplyr::contains("produto_interno_bruto"),
      dplyr::contains("impostos_l_quidos_de_subs_dios"),
      dplyr::contains("valor_adicionado_bruto_a_pre_os_correntes_total")
    ) |>
    tidyr::pivot_longer(
      cols = -c(localidade_id, localidade_nome, periodo),
      names_to = "variavel",
      values_to = "valor"
    ) |>
    dplyr::mutate(valor = as.numeric(valor)) |>
    dplyr::mutate(
      indicador = dplyr::case_when(
        stringr::str_detect(variavel, "produto_interno_bruto") ~ "pib",
        stringr::str_detect(variavel, "impostos_l_quidos_de_subs_dios") ~ "imposto",
        stringr::str_detect(variavel, "valor_adicionado_bruto_a_pre_os_correntes_total") ~ "vab_total",
        TRUE ~ NA_character_
      ),
      nivel = dplyr::case_when(
        stringr::str_ends(variavel, "_reais") ~ "valor_absoluto",
        stringr::str_detect(variavel, "microrregi") ~ "microrregiao",
        stringr::str_detect(variavel, "mesorregi") ~ "mesorregiao",
        stringr::str_detect(variavel, "unidade_da_federa") ~ "uf",
        stringr::str_detect(variavel, "grande_regi") ~ "grande_regiao",
        stringr::str_detect(variavel, "brasil") ~ "brasil",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::filter(!is.na(indicador), !is.na(nivel)) |>
    tidyr::pivot_wider(
      id_cols = c(localidade_id, localidade_nome, periodo, indicador),
      names_from = nivel,
      values_from = valor
    )
  
  niveis_macro <- c("valor_absoluto", "microrregiao", "mesorregiao", "uf", "grande_regiao", "brasil")
  for (n in niveis_macro) if (!n %in% names(tabela_macro)) tabela_macro[[n]] <- NA_real_
  
  tabela_macro <- tabela_macro |>
    dplyr::rename(
      valor_absoluto_reais = dplyr::any_of("valor_absoluto"),
      participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_da_microrregi_o_geogr_fica_ = dplyr::any_of("microrregiao"),
      participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_da_mesorregi_o_geogr_fica_ = dplyr::any_of("mesorregiao"),
      participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_da_unidade_da_federa_o_ = dplyr::any_of("uf"),
      participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_da_grande_regi_o_ = dplyr::any_of("grande_regiao"),
      participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_do_brasil_ = dplyr::any_of("brasil")
    )
  
  # --- Tabela setores (VAB por atividade) ---
  tabela_setores <- df |>
    dplyr::select(
      localidade_id, localidade_nome, periodo,
      dplyr::contains("valor_adicionado_bruto_a_pre_os_correntes_da_agropecu_ria"),
      dplyr::contains("valor_adicionado_bruto_a_pre_os_correntes_da_ind_stria"),
      dplyr::contains("valor_adicionado_bruto_a_pre_os_correntes_dos_servi_os_exclusive"),
      dplyr::contains("valor_adicionado_bruto_a_pre_os_correntes_da_administra_o_defesa")
    ) |>
    tidyr::pivot_longer(
      cols = -c(localidade_id, localidade_nome, periodo),
      names_to = "variavel",
      values_to = "valor"
    ) |>
    dplyr::mutate(
      setor = dplyr::case_when(
        stringr::str_detect(variavel, "agropecu_ria") ~ "agropecuaria",
        stringr::str_detect(variavel, "ind_stria") ~ "industria",
        stringr::str_detect(variavel, "servi_os_exclusive") ~ "servicos",
        stringr::str_detect(variavel, "administra_o_defesa") ~ "administracao",
        TRUE ~ NA_character_
      ),
      nivel = dplyr::case_when(
        stringr::str_ends(variavel, "_reais") ~ "valor_absoluto",
        stringr::str_detect(variavel, "participa_o.*_no_valor_adicionado_bruto_a_pre_os_correntes_total_") ~ "participacao_total",
        stringr::str_detect(variavel, "da_microrregi_o_geogr_fica_") ~ "microrregiao",
        stringr::str_detect(variavel, "da_mesorregi_o_geogr_fica_") ~ "mesorregiao",
        stringr::str_detect(variavel, "da_unidade_da_federa_o_") ~ "uf",
        stringr::str_detect(variavel, "da_grande_regi_o_") ~ "grande_regiao",
        stringr::str_detect(variavel, "do_brasil_") ~ "brasil",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::filter(!is.na(setor), !is.na(nivel)) |>
    tidyr::pivot_wider(
      id_cols = c(localidade_id, localidade_nome, periodo, setor),
      names_from = nivel,
      values_from = valor
    )
  
  niveis_setores <- c("valor_absoluto", "participacao_total", "microrregiao", "mesorregiao", "uf", "grande_regiao", "brasil")
  for (n in niveis_setores) if (!n %in% names(tabela_setores)) tabela_setores[[n]] <- NA_real_
  
  tabela_setores <- tabela_setores |>
    dplyr::rename(
      valor_absoluto_reais = dplyr::any_of("valor_absoluto"),
      participa_o_do_valor_adicionado_bruto_a_pre_os_correntes_da_agropecu_ria_no_valor_adicionado_bruto_a_pre_os_correntes_total_ = dplyr::any_of("participacao_total"),
      participacao_microrregiao = dplyr::any_of("microrregiao"),
      participacao_mesorregiao = dplyr::any_of("mesorregiao"),
      participacao_uf = dplyr::any_of("uf"),
      participacao_grande_regiao = dplyr::any_of("grande_regiao"),
      participacao_brasil = dplyr::any_of("brasil")
    )
  
  return(list(macro = tabela_macro, setores = tabela_setores))
}

# Enriquecimento municipal (adicionar colunas do decodificador territorial)
enriquecer_municipios <- function(tabela) {
  tabela <- tabela |>
    dplyr::mutate(
      municipio_ibge_clean = stringr::str_remove(localidade_nome, " \\(MT\\)") |> stringr::str_trim(),
      municipio_ibge_norm = normalizar(municipio_ibge_clean)
    )
  
  join_result <- fuzzyjoin::stringdist_left_join(
    tabela,
    territorialidade_mt,
    by = c("municipio_ibge_norm" = "municipio_norm"),
    method = "jw",
    max_dist = 0.15,
    distance_col = "dist_match"
  )
  
  melhor_correspondencia <- join_result |>
    dplyr::group_by(dplyr::across(-dplyr::any_of(c("municipio_decod_original", 
                                                   "rpseplan10340_munícipio_polo_decodificado",
                                                   "rpseplan10340_regiao_decodificado",
                                                   "imeia_regiao",
                                                   "imeia_municipios_polo_economico",
                                                   "territorio_latitude",
                                                   "territorio_longitude",
                                                   "municipio_norm",
                                                   "dist_match")))) |>
    dplyr::slice_min(order_by = dist_match, n = 1, with_ties = FALSE) |>
    dplyr::ungroup()
  
  resultado <- melhor_correspondencia |>
    dplyr::select(-municipio_ibge_clean, -municipio_ibge_norm, -municipio_norm, -dist_match)
  
  return(resultado)
}

# ============================================================================
# 4. FUNÇÃO PARA RENOMEAR COLUNAS (ANTES DE ESCREVER NO BANCO)
# ============================================================================

renomear_para_banco <- function(df, tipo) {
  if (tipo == "macro") {
    nomes <- names(df)
    nomes_curtos <- dplyr::case_when(
      nomes == "valor_absoluto_reais" ~ "valor_absoluto_reais",
      nomes == "participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_da_microrregi_o_geogr_fica_" ~ "pib_part_microrregiao",
      nomes == "participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_da_mesorregi_o_geogr_fica_" ~ "pib_part_mesorregiao",
      nomes == "participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_da_unidade_da_federa_o_" ~ "pib_part_uf",
      nomes == "participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_da_grande_regi_o_" ~ "pib_part_grande_regiao",
      nomes == "participa_o_do_produto_interno_bruto_a_pre_os_correntes_no_produto_interno_bruto_a_pre_os_correntes_do_brasil_" ~ "pib_part_brasil",
      TRUE ~ nomes
    )
    names(df) <- nomes_curtos
  } else if (tipo == "setores") {
    nomes <- names(df)
    nomes_curtos <- dplyr::case_when(
      nomes == "valor_absoluto_reais" ~ "valor_absoluto_reais",
      nomes == "participa_o_do_valor_adicionado_bruto_a_pre_os_correntes_da_agropecu_ria_no_valor_adicionado_bruto_a_pre_os_correntes_total_" ~ "vab_part_total",
      nomes == "participacao_microrregiao" ~ "vab_part_microrregiao",
      nomes == "participacao_mesorregiao" ~ "vab_part_mesorregiao",
      nomes == "participacao_uf" ~ "vab_part_uf",
      nomes == "participacao_grande_regiao" ~ "vab_part_grande_regiao",
      nomes == "participacao_brasil" ~ "vab_part_brasil",
      TRUE ~ nomes
    )
    names(df) <- nomes_curtos
  }
  return(df)
}

# ============================================================================
# 5. FUNÇÃO PARA PROCESSAR ÍNDICE DE GINI (TABELA 5939)
# ============================================================================

processar_gini <- function(dados) {
  # 1. Converter período para data (ano)
  dados <- dados |>
    dplyr::mutate(periodo = lubridate::ymd(paste0(periodo, "-01-01")))
  
  # 2. Identificar TODAS as colunas que contêm "gini" (case-insensitive)
  colunas_gini <- grep("gini", names(dados), ignore.case = TRUE, value = TRUE)
  
  # 3. Converter essas colunas para numérico
  dados <- dados |>
    dplyr::mutate(dplyr::across(dplyr::all_of(colunas_gini), as.numeric))
  
  # 4. Renomear na ordem esperada (PIB total, Agropecuária, Indústria, Serviços, Administração)
  novos_nomes <- c("gini_pib_total", "gini_agropecuaria", "gini_industria",
                   "gini_servicos", "gini_administracao")
  
  if (length(colunas_gini) == length(novos_nomes)) {
    nomes_rename <- stats::setNames(colunas_gini, novos_nomes)
    dados <- dados |> dplyr::rename(!!!nomes_rename)
  } else {
    warning("Número de colunas de Gini inesperado (", length(colunas_gini), 
            "). Renomeação parcial.")
    qtd <- min(length(colunas_gini), length(novos_nomes))
    nomes_rename <- stats::setNames(colunas_gini[1:qtd], novos_nomes[1:qtd])
    dados <- dados |> dplyr::rename(!!!nomes_rename)
  }
  
  return(dados)
}

# ============================================================================
# 6. FUNÇÃO AUXILIAR PARA BUSCAR MUNICÍPIOS EM BLOCOS (EVITA TIMEOUT)
# ============================================================================

buscar_municipios_mt <- function(ano, tamanho_bloco = 50) {
  # Obtém todos os códigos dos municípios do MT
  codigos <- jsonlite::fromJSON("https://servicodados.ibge.gov.br/api/v1/localidades/estados/51/municipios")$id
  
  # Divide em blocos
  blocos <- split(codigos, ceiling(seq_along(codigos) / tamanho_bloco))
  
  dados_municipios <- list()
  
  for (i in seq_along(blocos)) {
    cat("  Bloco", i, "de", length(blocos), "- municípios:", length(blocos[[i]]), "\n")
    localidades <- paste0("N6[", paste(blocos[[i]], collapse = ","), "]")
    temp <- busca_ibge(5938, periodos = ano, localidades = localidades)
    if (!is.null(temp)) {
      dados_municipios[[i]] <- temp
    } else {
      cat("  ⚠️ Bloco", i, "sem dados.\n")
    }
  }
  
  # Combina todos os blocos
  if (length(dados_municipios) > 0) {
    resultado <- dplyr::bind_rows(dados_municipios)
    return(resultado)
  } else {
    return(NULL)
  }
}

# ============================================================================
# 7. CONEXÃO COM O BANCO DE DADOS
# ============================================================================
source("X:/POWER BI/NOVOCAGED/conexao.R")   # cria objeto 'conexao'
schema_name <- "ibge"
DBI::dbExecute(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

# ============================================================================
# 8. LOOP PRINCIPAL POR ANO
# ============================================================================

for (i in seq_along(anos)) {
  ano <- anos[i]
  primeiro_ano <- (i == 1)
  
  message("\n=========================================")
  message("Processando ano: ", ano)
  message("=========================================\n")
  
  periodo <- as.character(ano)
  
  # --- Buscar dados dos estados (Unidades da Federação) ---
  cat("Baixando dados estaduais (PIB 5938)...\n")
  dados_estados <- busca_ibge(5938, periodos = periodo, localidades = "N3[all]")
  if (is.null(dados_estados)) {
    message("⚠️ Ano ", ano, " - sem dados estaduais. Pulando...")
    next
  }
  
  # --- Buscar dados dos municípios do MT (usando blocos) ---
  cat("Baixando dados municipais (MT) em blocos...\n")
  dados_municipios <- buscar_municipios_mt(periodo, tamanho_bloco = 50)
  if (is.null(dados_municipios)) {
    message("⚠️ Ano ", ano, " - sem dados municipais (MT). Pulando...")
    next
  }
  
  # --- Processamento dos dados estaduais ---
  cat("Processando dados estaduais...\n")
  dados_estados <- processar_pib_bruto(dados_estados)
  resultados_estados <- transformar_pib_long(dados_estados)
  macro_estados <- resultados_estados$macro
  setores_estados <- resultados_estados$setores
  
  # --- Processamento dos dados municipais (MT) ---
  cat("Processando dados municipais (MT)...\n")
  dados_municipios <- processar_pib_bruto(dados_municipios)
  resultados_municipios <- transformar_pib_long(dados_municipios)
  macro_municipios <- resultados_municipios$macro
  setores_municipios <- resultados_municipios$setores
  
  # --- Enriquecimento municipal com territorialidade ---
  macro_municipios <- enriquecer_municipios(macro_municipios)
  setores_municipios <- enriquecer_municipios(setores_municipios)
  
  # ========================================================================
  # 9. PROCESSAMENTO DO ÍNDICE DE GINI (TABELA 5939)
  # ========================================================================
  cat("Baixando dados de Gini (estados e Brasil)...\n")
  dados_gini_estados <- busca_ibge(5939, periodos = periodo, localidades = "N3[all]")
  dados_gini_brasil  <- busca_ibge(5939, periodos = periodo, localidades = "N1[all]")
  
  if (!is.null(dados_gini_estados) && !is.null(dados_gini_brasil)) {
    gini_estados <- processar_gini(dados_gini_estados) |> dplyr::mutate(nivel = "estado")
    gini_brasil  <- processar_gini(dados_gini_brasil)  |> dplyr::mutate(nivel = "brasil")
    gini_total   <- dplyr::bind_rows(gini_estados, gini_brasil)
    
    # Escreve a tabela de Gini no banco
    message("Escrevendo tabela: ", schema_name, ".pib_gini (ano ", ano, ")")
    RPostgres::dbWriteTable(
      conn = conexao,
      name = DBI::Id(schema = schema_name, table = "pib_gini"),
      value = gini_total,
      row.names = FALSE,
      overwrite = primeiro_ano,
      append = !primeiro_ano
    )
  } else {
    message("⚠️ Ano ", ano, " - dados de Gini não disponíveis. Pulando...")
  }
  # ========================================================================
  
  # --- Preparar lista de tabelas de PIB ---
  tabelas_para_banco <- list(
    pib_macro_estados = macro_estados,
    pib_setores_estados = setores_estados,
    pib_macro_municipios_mt = macro_municipios,
    pib_setores_municipios_mt = setores_municipios
  )
  
  # --- Escrever no banco com renomeação de colunas ---
  for (nome_tabela in names(tabelas_para_banco)) {
    tipo <- ifelse(grepl("macro", nome_tabela), "macro", "setores")
    tabela_renomeada <- renomear_para_banco(tabelas_para_banco[[nome_tabela]], tipo)
    
    message("Escrevendo tabela: ", schema_name, ".", nome_tabela, " (ano ", ano, ")")
    RPostgres::dbWriteTable(
      conn = conexao,
      name = DBI::Id(schema = schema_name, table = nome_tabela),
      value = tabela_renomeada,
      row.names = FALSE,
      overwrite = primeiro_ano,
      append = !primeiro_ano
    )
  }
  
  message("✅ Ano ", ano, " concluído.\n")
}

# Fechar conexão
DBI::dbDisconnect(conexao)
message("\n🎉 Todos os anos processados com sucesso!")
