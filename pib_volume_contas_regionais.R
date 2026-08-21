# Consulta API Sidra
# https://apisidra.ibge.gov.br/

# https://apisidra.ibge.gov.br/ quais são os endpoints dessa api?

source("X:/POWER BI/IBGE/ibge_tabelas/ibge_pesquisas_metadados_v2.R")

# parece não haver a pesquisa contas regionais https://apisidra.ibge.gov.br/
# (não encontrei respectivos numeros de tabelas nesta pesquisa no site sidra)

endereco <- 
  "https://ftp.ibge.gov.br/Contas_Regionais/2023/xls/Especiais_2010_2023_xls.zip"

tryCatch({
    curl::curl_download(endereco, # file name
      quiet = T,
      destfile = paste0("X:/POWER BI/IBGE/contas_regionais/",
                        "contas_regionais_especiais.zip"))
  }, error = function(err) {warning("file could not be downloaded")})

?unzip()

caminho <- "X:/POWER BI/IBGE/contas_regionais/contas_regionais_especiais.zip"

unzip(caminho,
      files = "tab03.xls")

arquivo_caminho <- "X:/POWER BI/IBGE/contas_regionais/tab03.xls"

#############################


volume_pib <- readxl::read_xls(arquivo_caminho, col_names = F)

?readxl::read_xls()

volume_pib <- volume_pib |>
  dplyr::filter(
    is.na(volume_pib[[1]]) | 
      !stringr::str_detect(stringr::str_trim(volume_pib[[1]]),   # limpa espaços e \n,
                           "Tabela|Federação|^Norte$|Nordeste|Sudeste|^Sul$|Centro-Oeste|Fonte") # Por algum motivo funciona Sul funciona assim
  ) |> 
  dplyr::slice(-1) |>
  janitor::row_to_names(row_number = 1) |>
  dplyr::rename(Região = 1)

#volume_pib[[1]] |> 
#  stringr::str_subset("Sul") |> 
#  unique()

volume_pib <- volume_pib |>
  dplyr::mutate(across(`2010`:`2023`, as.numeric)) |>
  tidyr::pivot_longer(cols = -Região, names_to = "ano",
                      values_to = "volume_pib") |>
  dplyr::mutate(volume_pib = as.numeric(volume_pib))

pib_volume_contas_regionais <- volume_pib |>
  dplyr::mutate(
    ano = as.Date(paste0(ano, "-01-01"))   # cria nova coluna data
  )

pib_volume_contas_regionais

# ============================================================================
# 7. CONEXÃO COM O BANCO DE DADOS
# ============================================================================

source("X:/POWER BI/NOVOCAGED/conexao.R")   # cria objeto 'conexao'
schema_name <- "ibge"
DBI::dbExecute(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

table_name <- "pib_volume_contas_regionais"

DBI::dbSendQuery(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

RPostgres::dbWriteTable(conexao,
                        name = DBI::Id(schema = schema_name,
                                       table = table_name),
                        value = pib_volume_contas_regionais,
                        row.names = FALSE, overwrite = TRUE)

RPostgres::dbDisconnect(conexao)
