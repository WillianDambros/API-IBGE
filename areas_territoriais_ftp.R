# =============================================================================
# SCRIPT: Download e Tratamento de Áreas Territoriais (IBGE)
# Fonte: https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/areas_territoriais/
# Formato: .xls
# Destino: pasta do projeto (here::here())
# =============================================================================

# 0. Pacotes necessários ------------------------------------------------------
# here, readxl, dplyr, stringr, lubridate

# 1. Parâmetros ---------------------------------------------------------------
ano     <- 2025
arquivo <- paste0("AR_BR_RG_UF_RGINT_RGI_MUN_", ano, ".xls")
base_url <- paste0(
  "https://geoftp.ibge.gov.br/organizacao_do_territorio/",
  "estrutura_territorial/areas_territoriais/", ano, "/"
)
url_xls <- paste0(base_url, arquivo)

# 2. Função para baixar o arquivo ---------------------------------------------
# Usa utils::download.file (base R) — mais estável no Windows do que httr.
baixar_arquivo <- function(url, destino = here::here(basename(url))) {
  message("Baixando: ", basename(url))
  message("Destino:  ", destino)
  
  # Se já existe e está travado, avisa antes de tentar sobrescrever
  if (file.exists(destino)) {
    teste <- try(file.remove(destino), silent = TRUE)
    if (inherits(teste, "try-error") || !isTRUE(teste)) {
      stop(
        "Não foi possível sobrescrever '", destino, "'.\n",
        "Verifique se o arquivo está aberto no Excel ou se a pasta é somente leitura."
      )
    }
  }
  
  utils::download.file(
    url      = url,
    destfile = destino,
    mode     = "wb",   # binário: obrigatório no Windows para .xls
    quiet    = FALSE
  )
  
  return(destino)
}

# 3. Download ----------------------------------------------------------------
caminho_xls <- baixar_arquivo(url_xls)

# 4. Leitura dos dados --------------------------------------------------------
dados_xls <- readxl::read_xls(caminho_xls, sheet = 1)

# 5. Tratamento dos dados -----------------------------------------------------
processar_areas_territoriais <- function(df) {
  # 5.1. Guardar o nome da primeira coluna ANTES de qualquer renomeação
  primeira_coluna <- names(df)[1]
  
  # 5.2. Detectar colunas que terminam com _<4 dígitos> (ex.: AR_MUN_2025)
  cols_com_ano <- names(df)[stringr::str_detect(names(df), "_\\d{4}$")]
  
  # 5.3. Extrair o ano (assume o mesmo para todas as colunas detectadas)
  ano_extraido <- stringr::str_extract(cols_com_ano[1], "\\d{4}$")
  
  # 5.4. Renomear as colunas removendo o sufixo _<ano>
  df <- df |>
    dplyr::rename_with(
      ~ stringr::str_remove(.x, "_\\d{4}$"),
      .cols = dplyr::all_of(cols_com_ano)
    )
  
  # 5.5. Adicionar a coluna periodo como data (1º de janeiro do ano)
  df <- df |>
    dplyr::mutate(
      periodo = lubridate::make_date(
        year  = as.integer(ano_extraido),
        month = 1,
        day   = 1
      )
    )
  
  # 5.6. Remover linhas vazias, cabeçalhos repetidos e observações ("OBS:")
  df <- df |>
    dplyr::filter(
      !is.na(.data[[primeira_coluna]]),
      .data[[primeira_coluna]] != "",
      !stringr::str_detect(.data[[primeira_coluna]], "OBS:")
    )
  
  return(df)
}

dados_tratados <- processar_areas_territoriais(dados_xls)
dados_tratados
# 6. Salvamento dos dados processados (pasta do projeto) ---------------------
# saveRDS(dados_tratados, here::here(paste0("areas_territoriais_", ano, ".rds")))
# utils::write.csv(
#   dados_tratados,
#   here::here(paste0("areas_territoriais_", ano, ".csv")),
#   row.names = FALSE
# )

message("Processamento concluído.")

# ============================================================================
# 6. CONEXÃO COM O BANCO DE DADOS
# ============================================================================

source("X:/POWER BI/NOVOCAGED/conexao.R")   # cria objeto 'conexao'
schema_name <- "ibge"
DBI::dbExecute(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

table_name <- "areas_territoriais_municipios_ftp"

DBI::dbSendQuery(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

RPostgres::dbWriteTable(conexao,
                        name = DBI::Id(schema = schema_name,
                                       table = table_name),
                        value = dados_tratados,
                        row.names = FALSE, overwrite = TRUE)

RPostgres::dbDisconnect(conexao)