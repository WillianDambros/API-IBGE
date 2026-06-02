#source("X:/POWER BI/IBGE/ibge_pesquisas.R")

#json_agregados$nome |> unique()

#tabelas_ibge |>
#  dplyr::filter(pesquisa_nome == "Produção da Extração Vegetal e da Silvicultura")

# ============================================================================
# CONFIGURAÇÕES INICIAIS
# ============================================================================

# Carregar pacotes necessários (apenas uma vez)
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

# Fonte da função de busca do IBGE (ajuste o caminho se necessário)
source("X:/POWER BI/IBGE/ibge_funcao_3.R")

# ============================================================================
# 1. DEFINIR PERÍODOS (ANOS) A PROCESSAR
# ============================================================================
ano_inicio <- lubridate::year(lubridate::today()) - 10
ano_fim   <- lubridate::year(lubridate::today())   # pode tentar até o ano atual, mas será pulado se não houver dados
anos <- seq(ano_inicio, ano_fim, by = 1)

# ============================================================================
# 2. CARREGAR DECODIFICADORES (UMA ÚNICA VEZ, FORA DO LOOP)
# ============================================================================

# --- Decodificador territorial para municípios de MT ---
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

# --- Decodificador de unidade de medida para extração vegetal ---
extracao_metadados <- "https://servicodados.ibge.gov.br/api/v3/agregados/289/metadados"
resposta <- httr::GET(extracao_metadados)
extracao_metadados_classificacao <- httr::content(resposta, "text", encoding = "UTF-8") |> 
  jsonlite::fromJSON(simplifyDataFrame = FALSE)

decodificador_extracao <- purrr::map_dfr(
  .x = extracao_metadados_classificacao$classificacoes[[1]]$categorias,
  .f = ~ tibble::tibble(
    codigo = .x$id,
    significado = .x$nome,
    unidade_de_medida = .x$unidade,
    int = .x$nivel
  )
)

# --- Decodificador de unidade de medida para silvicultura ---
silvicultura_metadados <- "https://servicodados.ibge.gov.br/api/v3/agregados/291/metadados"
resposta2 <- httr::GET(silvicultura_metadados)
silvicultura_metadados_classificacao <- httr::content(resposta2, "text", encoding = "UTF-8") |> 
  jsonlite::fromJSON(simplifyDataFrame = FALSE)

tipo_produto_silvicultura <- purrr::map_dfr(
  .x = silvicultura_metadados_classificacao$classificacoes[[1]]$categorias,
  .f = ~ tibble::tibble(
    codigo = .x$id,
    significado = .x$nome,
    unidade_de_medida = .x$unidade,
    int = .x$nivel
  )
)

# ============================================================================
# 3. FUNÇÕES AUXILIARES (PÓS-PROCESSAMENTO)
# ============================================================================

# Função de pós-processamento comum (remove percentuais, converte números, trata mil_reais)
processar_tabela_bruta <- function(df) {
  df |>
    dplyr::select(-dplyr::contains("percentual", ignore.case = TRUE)) |>
    dplyr::mutate(data_ano = as.Date(paste0(periodo, "-01-01"))) |>
    dplyr::mutate(dplyr::across(
      -dplyr::any_of(c("localidade_id", "localidade_nome", "periodo", 
                       "data_ano", "Tipo de produto extrativo",
                       "Tipo de produto da silvicultura", "Espécie florestal")),
      ~ readr::parse_number(.x)
    )) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::contains("mil_reais"),
        ~ .x * 1000,
        .names = "{.col}_reais"
      )
    ) |>
    dplyr::rename_with(
      ~ stringr::str_replace(.x, "_mil_reais_reais", "_reais"),
      dplyr::ends_with("_reais")
    ) |>
    dplyr::select(-dplyr::ends_with("_mil_reais"))
}

# Função para enriquecer tabelas municipais com dados territoriais
enriquecer_municipal <- function(tabela_ibge) {
  tabela_ibge <- tabela_ibge |>
    dplyr::mutate(
      municipio_ibge_clean = stringr::str_remove(localidade_nome, " \\(MT\\)") |> stringr::str_trim(),
      municipio_ibge_norm = normalizar(municipio_ibge_clean)
    )
  
  join_result <- fuzzyjoin::stringdist_left_join(
    tabela_ibge,
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
    dplyr::select(-municipio_ibge_clean, -municipio_ibge_norm, -municipio_norm, -dist_match) |>
    dplyr::select(-dplyr::ends_with("mil_reais"))
  
  return(resultado)
}

# Função para adicionar unidade de medida (extração vegetal) - SEM colunas extras
adicionar_unidade_medida_extracao <- function(tabela) {
  tabela_clean <- tabela |>
    dplyr::mutate(produto_clean = stringr::str_squish(`Tipo de produto extrativo`))
  
  decod_clean <- decodificador_extracao |>
    dplyr::mutate(significado_clean = stringr::str_squish(significado))
  
  joined <- fuzzyjoin::stringdist_left_join(
    tabela_clean,
    decod_clean,
    by = c("produto_clean" = "significado_clean"),
    method = "jw",
    max_dist = 0.05,
    distance_col = "dist_match"
  )
  
  resultado <- joined |>
    dplyr::group_by(dplyr::across(-c("codigo", "significado", "unidade_de_medida",
                                     "int", "significado_clean", "dist_match"))) |>
    dplyr::slice_min(order_by = dist_match, n = 1, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::select(-produto_clean, -significado_clean, -dist_match,
                  -codigo, -significado, -int) |>   # 🔥 REMOVE COLUNAS EXTRAS
    dplyr::mutate(unidade_de_medida = dplyr::if_else(
      `Tipo de produto extrativo` == "Total", "Total", unidade_de_medida
    ))
  
  return(resultado)
}

# Função para adicionar unidade de medida (silvicultura) - SEM colunas extras
adicionar_unidade_medida_silvicultura <- function(tabela) {
  tabela_clean <- tabela |>
    dplyr::mutate(produto_clean = stringr::str_squish(`Tipo de produto da silvicultura`))
  
  decod_clean <- tipo_produto_silvicultura |>
    dplyr::mutate(significado_clean = stringr::str_squish(significado))
  
  joined <- fuzzyjoin::stringdist_left_join(
    tabela_clean,
    decod_clean,
    by = c("produto_clean" = "significado_clean"),
    method = "jw",
    max_dist = 0.05,
    distance_col = "dist_match"
  )
  
  resultado <- joined |>
    dplyr::group_by(dplyr::across(-c("codigo", "significado", "unidade_de_medida",
                                     "int", "significado_clean", "dist_match"))) |>
    dplyr::slice_min(order_by = dist_match, n = 1, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::select(-produto_clean, -significado_clean, -dist_match,
                  -codigo, -significado, -int) |>   # 🔥 REMOVE COLUNAS EXTRAS
    dplyr::mutate(unidade_de_medida = dplyr::if_else(
      `Tipo de produto da silvicultura` == "Total", "Total", unidade_de_medida
    ))
  
  return(resultado)
}

# Funções para criar coluna categoria_produto (extração e silvicultura)
extrair_codigo_principal <- function(x) stringr::str_extract(x, "^\\d+")
mapear_categoria <- function(codigo) {
  dplyr::case_when(
    codigo == "1" ~ "Alimentícios",
    codigo == "2" ~ "Aromáticos, medicinais, tóxicos e corantes",
    codigo == "3" ~ "Borrachas",
    codigo == "4" ~ "Ceras",
    codigo == "5" ~ "Fibras",
    codigo == "6" ~ "Gomas não elásticas",
    codigo == "7" ~ "Produtos madeireiros e energéticos",
    codigo == "8" ~ "Oleaginosos",
    codigo == "9" ~ "Pinheiro brasileiro",
    codigo == "10" ~ "Tanantes",
    TRUE ~ "Outros"
  )
}
adicionar_categoria_extracao <- function(tabela) {
  tabela |>
    dplyr::mutate(categoria_produto = dplyr::if_else(
      `Tipo de produto extrativo` == "Total",
      "Total",
      mapear_categoria(extrair_codigo_principal(`Tipo de produto extrativo`))
    ))
}

extrair_codigo_silvicultura <- function(x) stringr::str_extract(x, "^\\d+(\\.\\d+)?")
mapear_categoria_silvicultura <- function(codigo) {
  dplyr::case_when(
    codigo == "1.1" ~ "Carvão vegetal",
    codigo == "1.2" ~ "Lenha",
    codigo == "1.3" ~ "Madeira em tora",
    stringr::str_detect(codigo, "^2") ~ "Outros produtos",
    codigo == "Total" ~ "Total",
    TRUE ~ "Outros"
  )
}
adicionar_categoria_silvicultura <- function(tabela) {
  tabela |>
    dplyr::mutate(categoria_produto = dplyr::if_else(
      `Tipo de produto da silvicultura` == "Total",
      "Total",
      mapear_categoria_silvicultura(extrair_codigo_silvicultura(`Tipo de produto da silvicultura`))
    ))
}

# ============================================================================
# 4. LOOP PRINCIPAL: PROCESSAR CADA ANO E ESCREVER NO BANCO
# ============================================================================

# Conectar ao banco (uma vez, fora do loop)
source("X:/POWER BI/NOVOCAGED/conexao.R")   # deve criar objeto 'conexao'
schema_name <- "ibge"

# Criar schema se não existir
DBI::dbExecute(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

# Lista de municípios de MT
# Substitua a definição manual por:
municipios_mt <- {
  url <- "https://servicodados.ibge.gov.br/api/v1/localidades/estados/51/municipios"
  codigos <- jsonlite::fromJSON(url)$id
  paste0("N6[", paste(codigos, collapse = ","), "]")
}

# Loop sobre os anos
for (i in seq_along(anos)) {
  ano <- anos[i]
  primeiro_ano <- (i == 1)
  
  message("\n=========================================")
  message("Processando ano: ", ano)
  message("=========================================\n")
  
  periodo <- as.character(ano)
  
  # --- Buscar dados brutos para este ano (com verificação de NULL) ---
  extracao_vegetal_estadual <- busca_ibge(289, periodos = periodo, localidades = "N3[all]")
  if (is.null(extracao_vegetal_estadual)) {
    message("⚠️ Ano ", ano, " - sem dados para extração vegetal estadual. Pulando...")
    next
  }
  
  sivilcultura_estadual <- busca_ibge(291, periodos = periodo, localidades = "N3[all]")
  if (is.null(sivilcultura_estadual)) {
    message("⚠️ Ano ", ano, " - sem dados para silvicultura estadual. Pulando...")
    next
  }
  
  area_sivilcutura_estadual <- busca_ibge(5930, periodos = periodo, localidades = "N3[all]")
  if (is.null(area_sivilcutura_estadual)) {
    message("⚠️ Ano ", ano, " - sem dados para área silvicultura estadual. Pulando...")
    next
  }
  
  extracao_vegetal_municipal <- busca_ibge(289, periodos = periodo, localidades = municipios_mt)
  if (is.null(extracao_vegetal_municipal)) {
    message("⚠️ Ano ", ano, " - sem dados para extração vegetal municipal. Pulando...")
    next
  }
  
  sivilcultura_municipal <- busca_ibge(291, periodos = periodo, localidades = municipios_mt)
  if (is.null(sivilcultura_municipal)) {
    message("⚠️ Ano ", ano, " - sem dados para silvicultura municipal. Pulando...")
    next
  }
  
  area_sivilcutura_municipal <- busca_ibge(5930, periodos = periodo, localidades = municipios_mt)
  if (is.null(area_sivilcutura_municipal)) {
    message("⚠️ Ano ", ano, " - sem dados para área silvicultura municipal. Pulando...")
    next
  }
  
  # --- Pós-processamento básico (conversões) ---
  tabelas_brutas <- list(
    extracao_vegetal_estadual = extracao_vegetal_estadual,
    sivilcultura_estadual = sivilcultura_estadual,
    area_sivilcutura_estadual = area_sivilcutura_estadual,
    extracao_vegetal_municipal = extracao_vegetal_municipal,
    sivilcultura_municipal = sivilcultura_municipal,
    area_sivilcutura_municipal = area_sivilcutura_municipal
  )
  
  tabelas_processadas <- purrr::map(tabelas_brutas, processar_tabela_bruta)
  
  # --- Enriquecimento das tabelas municipais (territorialidade) ---
  tabelas_processadas$extracao_vegetal_municipal <- enriquecer_municipal(tabelas_processadas$extracao_vegetal_municipal)
  tabelas_processadas$sivilcultura_municipal <- enriquecer_municipal(tabelas_processadas$sivilcultura_municipal)
  tabelas_processadas$area_sivilcutura_municipal <- enriquecer_municipal(tabelas_processadas$area_sivilcutura_municipal)
  
  # --- Adicionar coluna categoria_produto ---
  tabelas_processadas$extracao_vegetal_estadual <- adicionar_categoria_extracao(tabelas_processadas$extracao_vegetal_estadual)
  tabelas_processadas$extracao_vegetal_municipal <- adicionar_categoria_extracao(tabelas_processadas$extracao_vegetal_municipal)
  tabelas_processadas$sivilcultura_estadual <- adicionar_categoria_silvicultura(tabelas_processadas$sivilcultura_estadual)
  tabelas_processadas$sivilcultura_municipal <- adicionar_categoria_silvicultura(tabelas_processadas$sivilcultura_municipal)
  
  # --- Adicionar unidade de medida (sem colunas extras) ---
  tabelas_processadas$extracao_vegetal_estadual <- adicionar_unidade_medida_extracao(tabelas_processadas$extracao_vegetal_estadual)
  tabelas_processadas$extracao_vegetal_municipal <- adicionar_unidade_medida_extracao(tabelas_processadas$extracao_vegetal_municipal)
  tabelas_processadas$sivilcultura_estadual <- adicionar_unidade_medida_silvicultura(tabelas_processadas$sivilcultura_estadual)
  tabelas_processadas$sivilcultura_municipal <- adicionar_unidade_medida_silvicultura(tabelas_processadas$sivilcultura_municipal)
  
  # --- Renomear os elementos da lista para os nomes finais ---
  names(tabelas_processadas) <- paste0("producao_extracao_vegetal_silvicultura__",
                                       names(tabelas_processadas))
  
  # --- Escrever cada tabela no banco ---
  for (table_name in names(tabelas_processadas)) {
    message("Escrevendo tabela: ", schema_name, ".", table_name, " (ano ", ano, ")")
    
    RPostgres::dbWriteTable(
      conn = conexao,
      name = DBI::Id(schema = schema_name, table = table_name),
      value = tabelas_processadas[[table_name]],
      row.names = FALSE,
      overwrite = primeiro_ano,
      append = !primeiro_ano
    )
  }
  
  message("✅ Ano ", ano, " concluído e escrito no banco.\n")
}

# Fechar conexão
DBI::dbDisconnect(conexao)
message("\n✅ Todos os anos processados com sucesso!")
