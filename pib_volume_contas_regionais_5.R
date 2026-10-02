# Consulta API Sidra
# https://apisidra.ibge.gov.br/

# https://apisidra.ibge.gov.br/ quais são os endpoints dessa api?

source("X:/POWER BI/IBGE/ibge_tabelas/ibge_pesquisas_metadados_v2.R")

# parece não haver a pesquisa contas regionais https://apisidra.ibge.gov.br/
# (não encontrei respectivos numeros de tabelas nesta pesquisa no site sidra)




######################### Contas_Regionais ### disponiveis


# https://ftp.ibge.gov.br/Contas_Regionais/2023/xls/Conta_da_Producao_2002_2023_xls.zip

# https://ftp.ibge.gov.br/Contas_Regionais/2023/xls/Conta_da_Producao_2010_2023_xls.zip

# https://ftp.ibge.gov.br/Contas_Regionais/2023/xls/Especiais_2002_2023_xls.zip

# https://ftp.ibge.gov.br/Contas_Regionais/2023/xls/Especiais_2010_2023_xls.zip




# https://ftp.ibge.gov.br/Contas_Regionais/2023/xls/Conta_da_Producao_2010_2023_xls.zip  # Este agora

# https://ftp.ibge.gov.br/Contas_Regionais/2023/xls/Especiais_2010_2023_xls.zip

# ==================================================================
# 0. Definição da função de processamento (inalterada)
# ==================================================================
processar_valor_adicionado_bruto <- function(valor_adicionado_bruto_ftp) {
  
  valor_adicionado_bruto_ftp <- valor_adicionado_bruto_ftp |>
    dplyr::filter(
      dplyr::row_number() >= stringr::str_which(
        stringr::str_to_lower(...1),
        "valor adicionado bruto"
      )[1]
    )
  
  linha_titulo <- base::which(
    stringr::str_detect(
      stringr::str_to_lower(valor_adicionado_bruto_ftp$...1),
      "valor adicionado bruto"
    )
  )[1]
  
  territorio_extraido <- valor_adicionado_bruto_ftp$...1[linha_titulo + 1]
  setor_extraido      <- valor_adicionado_bruto_ftp$...1[linha_titulo + 2]
  
  valor_adicionado_bruto_ftp <- valor_adicionado_bruto_ftp |>
    dplyr::mutate(
      TERRITORIO = territorio_extraido,
      SETOR      = setor_extraido
    )
  
  linha_titulo <- base::which(
    stringr::str_detect(
      stringr::str_to_lower(valor_adicionado_bruto_ftp$...1),
      "valor adicionado bruto"
    )
  )[1]
  
  territorio_dinamico <- valor_adicionado_bruto_ftp$...1[linha_titulo + 1]
  setor_dinamico      <- valor_adicionado_bruto_ftp$...1[linha_titulo + 2]
  
  valor_adicionado_bruto_ftp <- valor_adicionado_bruto_ftp |>
    dplyr::filter(
      !stringr::str_detect(stringr::str_to_lower(...1), "valor adicionado bruto"),
      !stringr::str_detect(stringr::str_to_lower(...1), stringr::str_to_lower(territorio_dinamico)),
      !stringr::str_detect(stringr::str_to_lower(...1), stringr::str_to_lower(setor_dinamico)),
      !stringr::str_detect(stringr::str_to_lower(...1), "fonte")
    )
  
  cabecalho <- base::unlist(valor_adicionado_bruto_ftp[1, 1:6])
  
  valor_adicionado_bruto_ftp <- valor_adicionado_bruto_ftp |>
    dplyr::slice(-1)
  
  base::names(valor_adicionado_bruto_ftp) <- c(
    cabecalho,
    "TERRITORIO",
    "SETOR"
  )
  
  return(valor_adicionado_bruto_ftp)
}

# ==================================================================
# 1. Download (inalterado)
# ==================================================================
endereco <- "https://ftp.ibge.gov.br/Contas_Regionais/2023/xls/Conta_da_Producao_2010_2023_xls.zip"
caminho_zip <- base::paste0(here::here(), "/Conta_da_Producao_2010_2023_xls.zip")

tryCatch({
  curl::curl_download(
    url      = endereco,
    destfile = caminho_zip,
    quiet    = TRUE
  )
}, error = function(err) {
  base::warning("file could not be downloaded: ", base::conditionMessage(err))
})

# ==================================================================
# 2. Extrair TODOS os arquivos .xls para uma subpasta
# ==================================================================
pasta_extracao <- base::paste0(here::here(), "/contas_regionais")
if (!base::dir.exists(pasta_extracao)) base::dir.create(pasta_extracao)

utils::unzip(caminho_zip, exdir = pasta_extracao)

# ==================================================================
# 3. Listar todos os arquivos .xls extraídos
# ==================================================================
arquivos <- base::list.files(
  path       = pasta_extracao,
  pattern    = "^Tabela\\d+\\.xls$",
  full.names = TRUE
)

base::length(arquivos)   # confere se são 33
base::basename(arquivos) # confere os nomes

# ==================================================================
# 4. Função que processa UM arquivo inteiro (todas as abas de setores)
# ==================================================================
processar_arquivo_contas <- function(caminho_arquivo) {
  
  abas <- readxl::excel_sheets(caminho_arquivo)
  
  # Remove "Sumário" e a primeira aba de dados (TabelaXX.1 = Total das Atividades)
  abas_dados <- abas |>
    stringr::str_subset(
      pattern = stringr::regex("^sumário$|^tabela\\d+\\.1$", ignore_case = TRUE),
      negate  = TRUE
    )
  
  abas_dados |>
    purrr::map(function(aba) {
      planilha <- readxl::read_xls(
        path      = caminho_arquivo,
        sheet     = aba,
        col_names = FALSE
      )
      processar_valor_adicionado_bruto(planilha)
    }) |>
    dplyr::bind_rows()
}

# ==================================================================
# 5. Processar TODOS os arquivos e combinar
# ==================================================================
resultados <- arquivos |>
  purrr::map(function(arq) {
    tryCatch({
      res <- processar_arquivo_contas(arq)
      res$ARQUIVO_ORIGEM <- base::basename(arq)
      res
    }, error = function(e) {
      base::warning("Falhou em ", base::basename(arq), ": ", base::conditionMessage(e))
      NULL
    })
  }) |>
  purrr::compact() |>
  dplyr::bind_rows()

# ==================================================================
# 6. Conferir o resultado
# ==================================================================
resultados |> dplyr::glimpse()

resultados |>
  dplyr::distinct(ARQUIVO_ORIGEM, TERRITORIO) |>
  dplyr::arrange(ARQUIVO_ORIGEM) |>
  print(n = 40)
################################### verificando se a asoma das partes dão o todo

# ==================================================================
# 1. Base completa com valor numérico e região atribuída
# ==================================================================
mapa_regiao <- tibble::tribble(
  ~TERRITORIO,           ~REGIAO,
  "Rondônia",            "Região Norte",
  "Acre",                "Região Norte",
  "Amazonas",            "Região Norte",
  "Roraima",             "Região Norte",
  "Pará",                "Região Norte",
  "Amapá",               "Região Norte",
  "Tocantins",           "Região Norte",
  "Maranhão",            "Região Nordeste",
  "Piauí",               "Região Nordeste",
  "Ceará",               "Região Nordeste",
  "Rio Grande do Norte", "Região Nordeste",
  "Paraíba",             "Região Nordeste",
  "Pernambuco",          "Região Nordeste",
  "Alagoas",             "Região Nordeste",
  "Sergipe",             "Região Nordeste",
  "Bahia",               "Região Nordeste",
  "Minas Gerais",        "Região Sudeste",
  "Espírito Santo",      "Região Sudeste",
  "Rio de Janeiro",      "Região Sudeste",
  "São Paulo",           "Região Sudeste",
  "Paraná",              "Região Sul",
  "Santa catarina",      "Região Sul",
  "Rio Grande do Sul",   "Região Sul",
  "Mato Grosso do Sul",  "Região Centro-Oeste",
  "Mato Grosso",         "Região Centro-Oeste",
  "Goiás",               "Região Centro-Oeste",
  "Distrito Federal",    "Região Centro-Oeste"
)

col_valor <- "VALOR A PREÇO CORRENTE\n(1 000 000 R$)"

base_completa <- resultados |>
  dplyr::mutate(
    VALOR  = base::as.numeric(.data[[col_valor]]),
    REGIAO = mapa_regiao$REGIAO[base::match(TERRITORIO, mapa_regiao$TERRITORIO)]
  )

# ==================================================================
# 2. Soma dos estados (por ano) — soma todos os setores de todos os estados
# ==================================================================
soma_estados <- base_completa |>
  dplyr::filter(!base::is.na(REGIAO)) |>
  dplyr::group_by(ANO) |>
  dplyr::summarise(
    SOMA_ESTADOS = base::sum(VALOR, na.rm = TRUE),
    .groups      = "drop"
  )

# ==================================================================
# 3. Valor do Brasil por ano — soma todos os setores do Brasil
# ==================================================================
valor_brasil <- base_completa |>
  dplyr::filter(TERRITORIO == "Brasil") |>
  dplyr::group_by(ANO) |>
  dplyr::summarise(
    VALOR_BRASIL = base::sum(VALOR, na.rm = TRUE),
    .groups      = "drop"
  )

# ==================================================================
# 4. Comparação
# ==================================================================
comparacao_brasil <- soma_estados |>
  dplyr::left_join(valor_brasil, by = "ANO") |>
  dplyr::mutate(
    DIFERENCA     = SOMA_ESTADOS - VALOR_BRASIL,
    DIFERENCA_PCT = (DIFERENCA / VALOR_BRASIL) * 100,
    BATE          = base::abs(DIFERENCA_PCT) < 0.01
  )

comparacao_brasil |> print(n = 20)

####################################################### voltando para o codigo

# ==================================================================
# Manter apenas estados (excluindo Brasil e Regiões)
# ==================================================================
dados_estados <- base_completa |>
  dplyr::filter(!base::is.na(REGIAO))   # só estados têm REGIAO atribuída

# ==================================================================
# Conferência
# ==================================================================
dados_estados |>
  dplyr::distinct(TERRITORIO) |>
  dplyr::arrange(TERRITORIO) |>
  print(n = 40)


dados_estados <- dados_estados |>
  dplyr::mutate(
    TERRITORIO = dplyr::case_when(
      TERRITORIO == "Santa catarina" ~ "Santa Catarina",
      TRUE                           ~ TERRITORIO
    )
  )

dados_estados |>
  dplyr::distinct(TERRITORIO) |>
  dplyr::arrange(TERRITORIO) |>
  print(n = 40)

####################################################### tratamento do todo #####

dados_estados |> dplyr::glimpse()

dados_estados <- dados_estados |>
  dplyr::mutate(
    ANO_DATA = lubridate::ymd(base::paste0(ANO, "-01-01")),
    ANO      = base::as.integer(ANO)
  ) |>
  dplyr::relocate(ANO, ANO_DATA)   # opcional: deixa ANO e ANO_DATA na frente

dados_estados <- dados_estados |>
  dplyr::mutate(
    dplyr::across(
      tidyselect::matches(
        stringr::regex("valor|índice|indice", ignore_case = TRUE)
      ),
      base::as.numeric
    )
  )

dados_estados <- dados_estados |>
  dplyr::select(-VALOR)






# lembrar de renomear adequadamente o arquivo



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