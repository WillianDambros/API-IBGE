# ============================================================================ #
# SCRIPT: PIB per capita - Estados e Municípios de Mato Grosso (anual)
# ============================================================================ #

# ---------------------------------------------------------------------------- #
# 0. CARREGAR PACOTES (incluindo DBI e driver do banco)
# ---------------------------------------------------------------------------- #

#library(dplyr)
#library(stringr)
#library(readr)
#library(lubridate)
#library(curl)
#library(readxl)
#library(jsonlite)
#library(fuzzyjoin)
#library(DBI)          # <-- necessário para dbConnect, dbWriteTable, etc.
#library(RPostgres)    # ou odbc, conforme seu driver

# ---------------------------------------------------------------------------- #
# 1. CARREGAR FUNÇÃO DE CONSULTA DO IBGE E CONEXÃO COM O BANCO
# ---------------------------------------------------------------------------- #

# Altere os caminhos conforme sua estrutura
source("X:/POWER BI/IBGE/ibge_funcao_5.R")    # função busca_ibge
source("X:/POWER BI/NOVOCAGED/conexao.R")      # deve criar o objeto 'conexao'

# Verifique se a conexão foi criada
if (!exists("conexao")) stop("Objeto 'conexao' não encontrado. Verifique o arquivo conexao.R")

# ---------------------------------------------------------------------------- #
# 2. CONFIGURAÇÕES
# ---------------------------------------------------------------------------- #

ano_inicio <- lubridate::year(lubridate::today()) - 20
ano_fim    <- lubridate::year(lubridate::today())
anos       <- seq(ano_inicio, ano_fim, by = 1)

schema_name <- "ibge"   # schema onde as tabelas serão gravadas
DBI::dbExecute(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

# ---------------------------------------------------------------------------- #
# 3. DECODIFICADOR TERRITORIAL (baixa uma vez)
# ---------------------------------------------------------------------------- #

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

# ---------------------------------------------------------------------------- #
# 4. FUNÇÕES AUXILIARES
# ---------------------------------------------------------------------------- #

processar_pib_bruto <- function(df) {
  colunas_mil <- names(df)[endsWith(names(df), "mil_reais")]
  if (length(colunas_mil) > 0) {
    df <- df |>
      dplyr::mutate(dplyr::across(dplyr::all_of(colunas_mil), ~ as.numeric(.x) * 1000)) |>
      dplyr::rename_with(~ gsub("_mil_reais$", "_reais", .x), dplyr::all_of(colunas_mil))
  }
  
  colunas_participacao <- names(df)[startsWith(names(df), "participa_o")]
  if (length(colunas_participacao) > 0) {
    df <- df |>
      dplyr::mutate(dplyr::across(dplyr::all_of(colunas_participacao), ~ as.numeric(.x)))
  }
  
  colunas_identificacao <- c("localidade_id", "localidade_nome", "periodo")
  colunas_para_converter <- setdiff(names(df), colunas_identificacao)
  colunas_para_converter <- setdiff(colunas_para_converter, c(colunas_mil, colunas_participacao))
  
  if (length(colunas_para_converter) > 0) {
    df <- df |>
      dplyr::mutate(dplyr::across(dplyr::all_of(colunas_para_converter), ~ as.numeric(.x)))
  }
  
  df$periodo <- lubridate::make_date(year = as.numeric(df$periodo), month = 1, day = 1)
  return(df)
}

processar_populacao <- function(df) {
  colunas_identificacao <- c("localidade_id", "localidade_nome", "periodo")
  colunas_para_converter <- setdiff(names(df), colunas_identificacao)
  
  if (length(colunas_para_converter) > 0) {
    df <- df |>
      dplyr::mutate(dplyr::across(dplyr::all_of(colunas_para_converter), ~ as.numeric(.x)))
  }
  
  df <- df |>
    dplyr::mutate(periodo = lubridate::ymd(paste0(periodo, "-01-01")))
  return(df)
}

buscar_municipios_mt <- function(ano, tabela = 5938, tamanho_bloco = 50) {
  codigos_municipios <- jsonlite::fromJSON(
    "https://servicodados.ibge.gov.br/api/v1/localidades/estados/51/municipios"
  )$id
  
  blocos <- split(codigos_municipios, ceiling(seq_along(codigos_municipios) / tamanho_bloco))
  
  dados_municipios <- list()
  for (i in seq_along(blocos)) {
    cat("   Bloco", i, "de", length(blocos), "- municípios:", length(blocos[[i]]), "\n")
    localidades <- paste0("N6[", paste(blocos[[i]], collapse = ","), "]")
    temp <- busca_ibge(tabela, periodos = as.character(ano), localidades = localidades)
    if (!is.null(temp) && nrow(temp) > 0) {
      dados_municipios[[i]] <- temp
    } else {
      cat("   ⚠️ Bloco", i, "sem dados.\n")
    }
  }
  
  if (length(dados_municipios) > 0) {
    return(dplyr::bind_rows(dados_municipios))
  } else {
    return(NULL)
  }
}

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
    dplyr::group_by(dplyr::across(-dplyr::any_of(c(
      "municipio_decod_original",
      "rpseplan10340_munícipio_polo_decodificado",
      "rpseplan10340_regiao_decodificado",
      "imeia_regiao",
      "imeia_municipios_polo_economico",
      "territorio_latitude",
      "territorio_longitude",
      "municipio_norm",
      "dist_match"
    )))) |>
    dplyr::slice_min(order_by = dist_match, n = 1, with_ties = FALSE) |>
    dplyr::ungroup()
  
  resultado <- melhor_correspondencia |>
    dplyr::select(-municipio_ibge_clean, -municipio_ibge_norm, -municipio_norm, -dist_match)
  
  return(resultado)
}

renomear_para_banco <- function(df) {
  nomes <- names(df)
  nomes <- stringr::str_replace_all(nomes, " ", "_")
  nomes <- stringr::str_replace_all(nomes, "[^a-zA-Z0-9_]", "")
  nomes <- stringr::str_to_lower(nomes)
  names(df) <- nomes
  return(df)
}

# ---------------------------------------------------------------------------- #
# 5. LOOP PRINCIPAL POR ANO
# ---------------------------------------------------------------------------- #

for (i in seq_along(anos)) {
  ano <- anos[i]
  primeiro_ano <- (i == 1)
  
  message("\n=========================================")
  message("Processando ano: ", ano)
  message("=========================================\n")
  
  # ---------- PIB ----------
  cat("Baixando PIB dos estados...\n")
  ibge_pib_estados <- tryCatch({
    busca_ibge(5938, periodos = as.character(ano), localidades = "N3[all]") |>
      dplyr::select(localidade_id, localidade_nome, periodo,
                    produto_interno_bruto_a_pre_os_correntes_mil_reais)
  }, error = function(e) NULL)
  
  if (is.null(ibge_pib_estados) || nrow(ibge_pib_estados) == 0) {
    message("⚠️ Ano ", ano, " - sem dados de PIB para estados. Pulando...")
    next
  }
  
  cat("Baixando PIB dos municípios de MT (blocos)...\n")
  ibge_pib_municipios <- tryCatch({
    buscar_municipios_mt(ano, tabela = 5938, tamanho_bloco = 50)
  }, error = function(e) NULL)
  
  if (is.null(ibge_pib_municipios) || nrow(ibge_pib_municipios) == 0) {
    message("⚠️ Ano ", ano, " - sem dados de PIB para municípios MT. Pulando...")
    next
  }
  
  ibge_pib_municipios <- ibge_pib_municipios |>
    dplyr::select(localidade_id, localidade_nome, periodo,
                  produto_interno_bruto_a_pre_os_correntes_mil_reais)
  
  ibge_pib_estados   <- processar_pib_bruto(ibge_pib_estados)
  ibge_pib_municipios <- processar_pib_bruto(ibge_pib_municipios)
  
  # ---------- POPULAÇÃO ----------
  cat("Baixando população dos estados...\n")
  ibge_populacao_estados <- tryCatch({
    busca_ibge(6579, periodos = as.character(ano), localidades = "N3[all]")
  }, error = function(e) NULL)
  
  if (is.null(ibge_populacao_estados) || nrow(ibge_populacao_estados) == 0) {
    message("⚠️ Ano ", ano, " - sem dados de população para estados. Pulando...")
    next
  }
  
  cat("Baixando população dos municípios de MT (blocos)...\n")
  ibge_populacao_municipios_mt <- tryCatch({
    buscar_municipios_mt(ano, tabela = 6579, tamanho_bloco = 50)
  }, error = function(e) NULL)
  
  if (is.null(ibge_populacao_municipios_mt) || nrow(ibge_populacao_municipios_mt) == 0) {
    message("⚠️ Ano ", ano, " - sem dados de população para municípios MT. Pulando...")
    next
  }
  
  ibge_populacao_estados <- processar_populacao(ibge_populacao_estados)
  ibge_populacao_municipios_mt <- processar_populacao(ibge_populacao_municipios_mt)
  
  # ---------- LEFT JOIN E PER CAPITA ----------
  pib_percapita_estados <- dplyr::left_join(
    ibge_pib_estados,
    ibge_populacao_estados |>
      dplyr::select(localidade_id, periodo, popula_o_residente_estimada_pessoas),
    by = c("localidade_id", "periodo")
  ) |>
    dplyr::mutate(
      pib_per_capita_reais = produto_interno_bruto_a_pre_os_correntes_reais / 
        popula_o_residente_estimada_pessoas
    )
  
  pib_percapita_municipios_mt <- dplyr::left_join(
    ibge_pib_municipios,
    ibge_populacao_municipios_mt |>
      dplyr::select(localidade_id, periodo, popula_o_residente_estimada_pessoas),
    by = c("localidade_id", "periodo")
  ) |>
    dplyr::mutate(
      pib_per_capita_reais = produto_interno_bruto_a_pre_os_correntes_reais / 
        popula_o_residente_estimada_pessoas
    )
  
  # ---------- ENRIQUECER MUNICÍPIOS ----------
  pib_percapita_municipios_mt <- enriquecer_municipios(pib_percapita_municipios_mt)
  
  # ---------- RENOMEAR COLUNAS ----------
  pib_percapita_estados <- renomear_para_banco(pib_percapita_estados)
  pib_percapita_municipios_mt <- renomear_para_banco(pib_percapita_municipios_mt)
  
  # ---------- ESCREVER NO BANCO ----------
  message("Escrevendo tabela: ", schema_name, ".pib_percapita_estados (ano ", ano, ")")
  DBI::dbWriteTable(
    conn = conexao,
    name = DBI::Id(schema = schema_name, table = "pib_percapita_estados"),
    value = pib_percapita_estados,
    row.names = FALSE,
    overwrite = primeiro_ano,
    append = !primeiro_ano
  )
  
  message("Escrevendo tabela: ", schema_name, ".pib_percapita_municipios_mt (ano ", ano, ")")
  DBI::dbWriteTable(
    conn = conexao,
    name = DBI::Id(schema = schema_name, table = "pib_percapita_municipios_mt"),
    value = pib_percapita_municipios_mt,
    row.names = FALSE,
    overwrite = primeiro_ano,
    append = !primeiro_ano
  )
  
  message("✅ Ano ", ano, " concluído.\n")
}

# ---------------------------------------------------------------------------- #
# 6. FINALIZAR (opcional: desconectar)
# ---------------------------------------------------------------------------- #

# Se quiser fechar a conexão após o loop, descomente a linha abaixo:
# DBI::dbDisconnect(conexao)

message("\n✅ Todos os anos processados com sucesso!")