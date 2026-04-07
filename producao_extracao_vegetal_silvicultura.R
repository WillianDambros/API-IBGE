#source("X:/POWER BI/IBGE/ibge_pesquisas.R")

#json_agregados$nome |> unique()

#tabelas_ibge |>
#  dplyr::filter(pesquisa_nome == "Produção da Extração Vegetal e da Silvicultura")

# 10 anos de intervalo 

periodo_busca <- paste0(lubridate::year(lubridate::today()) - 2,
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
# CORREÇÃO: FUZZYJOIN PARA MUNICÍPIOS DE MATO GROSSO (COM NORMALIZAÇÃO)
# ============================================================================

# 1. Recarregar o decodificador corretamente
territorialidade_mt <- readxl::read_excel(
  decodificador_endereco,
  sheet = "territorialidade_municipios_mt",
  col_types = "text"
)

# 2. Identificar a coluna que contém o nome do município (provavelmente "territorio_geo_munícipios")
colunas_decod <- names(territorialidade_mt)
print("Colunas disponíveis no decodificador:")
print(colunas_decod)

# Ajuste: usar a coluna correta (substitua "territorio_geo_munícipios" se necessário)
coluna_municipio_decod <- "territorio_geo_munícipios"  # ou "MUNICIPIO", verifique

# 3. Função de normalização (remove acentos, pontuação, espaços, coloca em minúsculo)
normalizar <- function(x) {
  x |>
    stringr::str_to_lower() |>
    stringr::str_replace_all(" \\(mt\\)", "") |>   # remove " (mt)" se existir
    stringr::str_replace_all("[^a-z0-9 ]", "") |>  # remove acentos e pontuação
    stringr::str_squish()                          # remove espaços extras
}

# 4. Preparar a base territorial com nome normalizado
territorialidade_mt <- territorialidade_mt |>
  dplyr::select(
    dplyr::all_of(coluna_municipio_decod),        # nome do município
    rpseplan10340_munícipio_polo_decodificado,
    rpseplan10340_regiao_decodificado,
    imeia_regiao,
    imeia_municipios_polo_economico,
    territorio_latitude,
    territorio_longitude
  ) |>
  dplyr::rename(MUNICIPIO_ORIGINAL = dplyr::all_of(coluna_municipio_decod)) |>
  dplyr::mutate(
    MUNICIPIO_NORM = normalizar(MUNICIPIO_ORIGINAL),
    # Converter coordenadas (vírgula como decimal)
    dplyr::across(
      c(territorio_latitude, territorio_longitude),
      ~ readr::parse_number(., locale = readr::locale(decimal_mark = ","))
    )
  ) |>
  dplyr::filter(!is.na(MUNICIPIO_NORM))  # remove linhas sem nome

# 5. Função para limpar o nome vindo do IBGE
limpar_municipio_ibge <- function(nome) {
  nome |>
    stringr::str_remove(" \\(MT\\)") |>   # remove " (MT)"
    stringr::str_trim()
}

# 6. Aplicar fuzzyjoin com normalização e tolerância maior
tabelas_municipais_enriquecidas <- list()
nomes_municipais <- c("extracao_vegetal_municipal", 
                      "sivilcultura_municipal", 
                      "area_sivilcutura_municipal")

for (nome in nomes_municipais) {
  if (nome %in% names(tabelas_processadas)) {
    tabela <- tabelas_processadas[[nome]]
    
    tabela_enriquecida <- tabela |>
      # Criar coluna com nome limpo e normalizado
      dplyr::mutate(
        municipio_clean = limpar_municipio_ibge(localidade_nome),
        municipio_norm = normalizar(municipio_clean)
      ) |>
      # Fuzzyjoin usando os nomes normalizados
      fuzzyjoin::stringdist_left_join(
        territorialidade_mt,
        by = c("municipio_norm" = "MUNICIPIO_NORM"),
        method = "jw",                # Jaro-Winkler
        max_dist = 0.15,              # tolerância maior (15% de diferença)
        distance_col = "dist_match"
      ) |>
      # Remover colunas auxiliares e manter apenas uma cópia do nome original
      dplyr::select(-municipio_clean, -municipio_norm, -dist_match, -MUNICIPIO_NORM) |>
      # Renomear a coluna do nome original do município (opcional)
      dplyr::rename_with(~ "municipio_decod", .cols = "MUNICIPIO_ORIGINAL")
    
    # Remover colunas "mil_reais" se existirem (já temos as versões "_reais")
    if (any(stringr::str_detect(names(tabela_enriquecida), "mil_reais"))) {
      tabela_enriquecida <- tabela_enriquecida |>
        dplyr::select(-dplyr::ends_with("mil_reais"))
    }
    
    tabelas_municipais_enriquecidas[[nome]] <- tabela_enriquecida
    message("✅ Enriquecida com sucesso: ", nome)
  } else {
    message("⚠️ Tabela não encontrada: ", nome)
  }
}

# 7. Verificar se o join funcionou (agora deve aparecer dados não-NA)
message("\n📊 Verificação de correspondências (sivilcultura_municipal):")
tabelas_municipais_enriquecidas$sivilcultura_municipal |>
  dplyr::select(localidade_nome, imeia_regiao, territorio_latitude, dist_match) |>
  dplyr::filter(!is.na(imeia_regiao)) |>
  head(10) |>
  print()

# 8. Juntar com as tabelas estaduais (se quiser manter tudo em um só objeto)
tabelas_finais <- tabelas_processadas
for (nome in names(tabelas_municipais_enriquecidas)) {
  tabelas_finais[[nome]] <- tabelas_municipais_enriquecidas[[nome]]
}

message("\n✅ Processamento finalizado! Tabelas enriquecidas disponíveis em 'tabelas_finais'.")

# ============================================
# SALVAR NO BANCO DE DADOS
# ============================================
source("X:/POWER BI/NOVOCAGED/conexao.R")

schema_name <- "ibge"
table_name <- "pesquisa_trimestral_abate_animais"

DBI::dbSendQuery(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

RPostgres::dbWriteTable(conexao,
                        name = DBI::Id(schema = schema_name,
                                       table = table_name),
                        value = abate_animais,
                        row.names = FALSE, 
                        overwrite = TRUE)

RPostgres::dbDisconnect(conexao)
