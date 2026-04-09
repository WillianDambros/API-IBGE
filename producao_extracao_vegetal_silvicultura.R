#source("X:/POWER BI/IBGE/ibge_pesquisas.R")

#json_agregados$nome |> unique()

#tabelas_ibge |>
#  dplyr::filter(pesquisa_nome == "Produção da Extração Vegetal e da Silvicultura")

# 10 anos de intervalo 
# A tabela extracao vegetal municipal esta muito pessada para pesquisa do IBGE vai ter que separar a extracao por ano a escrita no servidor
periodo_busca <- paste0(lubridate::year(lubridate::today()) - 5,
                        "-", lubridate::year(lubridate::today()))

source("X:/POWER BI/IBGE/ibge_funcao_3.R")

# Buscar dados
extracao_vegetal_estadual <- busca_ibge(289, periodos = periodo_busca, localidades = "N3[all]")
sivilcultura_estadual <- busca_ibge(291, periodos = periodo_busca, localidades = "N3[all]")
area_sivilcutura_estadual <- busca_ibge(5930, periodos = periodo_busca, localidades = "N3[all]")

municipios_mt <- paste0("N6[5100201,5100250,5100300,5100409,5100508,5100607,5101001,",
"5101209,5101258,5101308,5101704,5101803,5101852,5101902,5102504,5102603,",
"5102637,5102678,5102686,5102702,5102793,5103007,5103056,5103106,5103304,",
"5103403,5103437,5103502,5103601,5103700,5103858,5103957,5104104,5104203,",
"5104500,5104526,5104542,5104559,5104609,5104807,5104906,5105101,5105150,",
"5105200,5105234,5105259,5105580,5105606,5105622,5105903,5106000,5106109,",
"5106158,5106208,5106216,5106224,5106240,5106257,5106265,5106281,5106307,",
"5106372,5106422,5106455,5106653,5106828,5106851,5107008,5107040,5107065,",
"5107107,5107156,5107180,5107198,5107206,5107248,5107297,5107305,5107602,",
"5107701,5107750,5107768,5107792,5107800,5107859,5107875,5107909,5107925,",
"5107958,5108006,5108055,5108105,5108402,5108501,5108808,5108857,5108907,5108956]")

extracao_vegetal_municipal <- busca_ibge(289, periodos = periodo_busca, localidades = municipios_mt)
sivilcultura_municipal <- busca_ibge(291, periodos = periodo_busca, localidades = municipios_mt)
area_sivilcutura_municipal <- busca_ibge(5930, periodos = periodo_busca, localidades = municipios_mt)

extracao_vegetal_estadual |>dplyr::glimpse()
sivilcultura_estadual |> dplyr::glimpse()
area_sivilcutura_estadual |> dplyr::glimpse()

extracao_vegetal_municipal |>dplyr::glimpse()
sivilcultura_municipal |> dplyr::glimpse()
area_sivilcutura_municipal |> dplyr::glimpse()

# ============================================
# PÓS-PROCESSAMENTO
# ============================================

# Criar lista nomeada e processar
tabelas_processadas <- list(
  extracao_vegetal_estadual = extracao_vegetal_estadual,
  sivilcultura_estadual = sivilcultura_estadual,
  area_sivilcutura_estadual = area_sivilcutura_estadual,
  extracao_vegetal_municipal = extracao_vegetal_municipal,
  sivilcultura_municipal = sivilcultura_municipal,
  area_sivilcutura_municipal = area_sivilcutura_municipal
) |>
  purrr::map(~ .x |>
        dplyr::select(-dplyr::contains("percentual", ignore.case = TRUE)) |>
        dplyr::mutate(data_ano = as.Date(paste0(periodo, "-01-01"))) |>
        dplyr::mutate(dplyr::across(
          # Pega todas as colunas que são character EXCETO as de identificação
          -dplyr::any_of(c("localidade_id", "localidade_nome", "periodo", 
                           "data_ano", "Tipo de produto extrativo",
                           "Tipo de produto da silvicultura", 
                           "Espécie florestal")),
          ~ readr::parse_number(.)
        ))|>
          # Depois converter mil reais para reais e renomear
          dplyr::mutate(
            # Para cada coluna que contém "mil_reais", criar uma nova coluna multiplicada por 1000
            dplyr::across(
              dplyr::contains("mil_reais"),
              ~ . * 1000,
              .names = "{.col}_reais"
            ) |>
        # Renomear as colunas que termina com "_reais" (removendo o sufixo e o "_mil_reais" original)
        dplyr::rename_with(
          ~ stringr::str_replace(., "_mil_reais_reais", "_reais"),
          dplyr::ends_with("_reais")
        ) |>
        # Remover as colunas originais com "mil_reais"
        dplyr::select(-dplyr::ends_with("_mil_reais"))
  )
)
  
tabelas_processadas$extracao_vegetal_estadual |> dplyr::glimpse()
tabelas_processadas$sivilcultura_estadual |> dplyr::glimpse()
tabelas_processadas$area_sivilcutura_estadual |> dplyr::glimpse()

tabelas_processadas$extracao_vegetal_municipal |> dplyr::glimpse()
tabelas_processadas$sivilcultura_municipal |> dplyr::glimpse()
tabelas_processadas$area_sivilcutura_municipal |> dplyr::glimpse()

# ============================================================================
# CÓDIGO FINAL: FUZZYJOIN PARA TABELAS MUNICIPAIS (SEM DUPLICAÇÃO)
# ============================================================================
# Using the particular produce decoder to adding more information in the novocaged

compilado_decodificador_endereço <-
  paste0("https://github.com/WillianDambros/data_source/raw/",
         "refs/heads/main/compilado_decodificador.xlsx")

decodificador_endereco <- paste0(getwd(), "/compilado_decodificador.xlsx")


curl::curl_download(compilado_decodificador_endereço,
                    decodificador_endereco)

"compilado_decodificador.xlsx" |> readxl::excel_sheets()
# 1. Carregar e preparar o decodificador -------------------------------------
territorialidade_mt <- readxl::read_excel(
  decodificador_endereco,
  sheet = "territorialidade_municipios_mt",
  col_types = "text"
)

# Função de normalização (minúsculo, remove acentos, pontuação, espaços)
normalizar <- function(x) {
  x |>
    stringr::str_to_lower() |>
    stringr::str_replace_all(" \\(mt\\)", "") |>   # remove " (mt)"
    stringr::str_replace_all("[^a-z0-9 ]", "") |>  # remove acentos e pontuação
    stringr::str_squish()                          # remove espaços extras
}

# Preparar base territorial
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
      ~ readr::parse_number(., locale = readr::locale(decimal_mark = ","))
    )
  ) |>
  dplyr::filter(!is.na(municipio_norm))

# 2. Função para enriquecer uma tabela municipal (sem duplicar linhas) ------
enriquecer_municipal <- function(tabela_ibge) {
  
  # Preparar tabela IBGE
  tabela_ibge <- tabela_ibge |>
    dplyr::mutate(
      municipio_ibge_clean = stringr::str_remove(localidade_nome, " \\(MT\\)") |> stringr::str_trim(),
      municipio_ibge_norm = normalizar(municipio_ibge_clean)
    )
  
  # Fuzzyjoin: calcular todas as correspondências dentro da distância máxima
  join_result <- fuzzyjoin::stringdist_left_join(
    tabela_ibge,
    territorialidade_mt,
    by = c("municipio_ibge_norm" = "municipio_norm"),
    method = "jw",
    max_dist = 0.15,
    distance_col = "dist_match"
  )
  
  # Para cada linha original, manter apenas a melhor correspondência (menor distância)
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
  
  # Remover colunas auxiliares e colunas originais de "mil_reais"
  resultado <- melhor_correspondencia |>
    dplyr::select(-municipio_ibge_clean, -municipio_ibge_norm, -municipio_norm, -dist_match) |>
    dplyr::select(-dplyr::ends_with("mil_reais"))   # remove as colunas antigas (já temos _reais)
  
  return(resultado)
}

# 3. Aplicar a função a cada tabela municipal ---------------------------------
nomes_municipais <- c("extracao_vegetal_municipal", 
                      "sivilcultura_municipal", 
                      "area_sivilcutura_municipal")

tabelas_municipais_enriquecidas <- list()

for (nome in nomes_municipais) {
  if (nome %in% names(tabelas_processadas)) {
    tabelas_municipais_enriquecidas[[nome]] <- enriquecer_municipal(tabelas_processadas[[nome]])
    message("✅ Enriquecida: ", nome, " (", nrow(tabelas_municipais_enriquecidas[[nome]]), " linhas)")
  } else {
    message("⚠️ Tabela não encontrada: ", nome)
  }
}

# 5. (Opcional) Unir com as tabelas estaduais em um único objeto ------------
tabelas_finais <- tabelas_processadas
for (nome in names(tabelas_municipais_enriquecidas)) {
  tabelas_finais[[nome]] <- tabelas_municipais_enriquecidas[[nome]]
}

# Renomear os elementos da lista com separador "__"
names(tabelas_finais) <- paste0("producao_extracao_vegetal_silvicultura__",
                                names(tabelas_finais))

message("\n✅ Processamento finalizado! Tabelas disponíveis em 'tabelas_finais'.")


tabelas_finais |> dplyr::glimpse()
# 1. Carregar a conexão
source("X:/POWER BI/NOVOCAGED/conexao.R")

# 2. Schema alvo (ibge)
schema_name <- "ibge"

# 3. Criar o schema se não existir (opcional, mas seguro)
DBI::dbSendQuery(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

# 5. Loop para escrever cada tabela no schema "ibge"
for (table_name in names(tabelas_finais)) {
  message("Escrevendo tabela: ", schema_name, ".", table_name)
  
  RPostgres::dbWriteTable(
    conn = conexao,
    name = DBI::Id(schema = schema_name, table = table_name),
    value = tabelas_finais[[table_name]],
    row.names = FALSE,
    overwrite = TRUE
  )
}

# 6. Desconectar
DBI::dbDisconnect(conexao)

message("✅ Todas as 6 tabelas foram escritas com sucesso no schema '", 
        schema_name, "'.")
