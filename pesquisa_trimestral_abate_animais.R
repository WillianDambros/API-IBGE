
#source("X:/POWER BI/IBGE/ibge_pesquisas.R")

#json_agregados$nome |> unique()

#tabelas_ibge |>
#  dplyr::filter(pesquisa_nome == "Pesquisa Trimestral do Abate de Animais") |>
#  dplyr::glimpse()


# 10 anos de intervalo 

periodo_busca <- paste0(lubridate::year(lubridate::today()) - 5,
                        "01-", lubridate::year(lubridate::today()), "04")

source("X:/POWER BI/IBGE/ibge_funcao.R")

# Buscar dados
bovinos <- busca_ibge(1092, periodos = periodo_busca, localidades = "N3[all]")
suinos <- busca_ibge(1093, periodos = periodo_busca, localidades = "N3[all]")
frangos <- busca_ibge(1094, periodos = periodo_busca, localidades = "N3[all]")

# ============================================
# SUBSTITUIR "Total" POR IDENTIFICADORES
# ============================================

# Bovinos: adicionar "Bovinos - " em todas as categorias
bovinos <- bovinos |> 
  dplyr::rename(tipo_conjunto = `Tipo de rebanho bovino`) |>
  dplyr::mutate(
    tipo_conjunto = paste0("Bovinos - ", tipo_conjunto),
    agregado = paste0("Bovinos Abatidos")
  )

# Suínos: criar coluna tipo_rebanho com "Suínos - Total"
suinos <- suinos |> 
  dplyr::mutate(
    tipo_conjunto = "Suínos - Total",
    agregado = paste0("Suínos Abatidos")
  )

# Frangos: criar coluna tipo_rebanho com "Frangos - Total"
frangos <- frangos |> 
  dplyr::mutate(
    tipo_conjunto = "Frangos - Total",
    agregado = paste0("Frangos Abatidos")
  )

# ============================================
# COMBINAR TUDO
# ============================================
abate_animais <- dplyr::bind_rows(bovinos, suinos, frangos)

# ============================================
# PÓS-PROCESSAMENTO
# ============================================

# Remover colunas de percentual
abate_animais <- abate_animais |>
  dplyr::select(-dplyr::contains("percentual", ignore.case = TRUE))

# Criar coluna de data
abate_animais <- abate_animais |>
  dplyr::mutate(
    data_trimestre = lubridate::yq(periodo_total_do_trimestre)
  )

# Converter colunas numéricas
abate_animais <- abate_animais |>
  dplyr::mutate(
    dplyr::across(
      c(n_mero_de_informantes_unidades, 
        animais_abatidos_cabe_as, 
        peso_total_das_carca_as_quilogramas),
      ~ as.numeric(.)
    )
  )


# Using the particular produce decoder to adding more information in the novocaged

#compilado_decodificador_endereço <-
#  paste0("https://github.com/WillianDambros/data_source/raw/",
#         "refs/heads/main/compilado_decodificador.xlsx")

#decodificador_endereco <- paste0(getwd(), "/compilado_decodificador.xlsx")


#curl::curl_download(compilado_decodificador_endereço,
#                    decodificador_endereco)

#"compilado_decodificador.xlsx" |> readxl::excel_sheets()

#territorialidade_sedec <- 
#  readxl::read_excel("compilado_decodificador.xlsx",
#                     sheet =  "geo_estados",
#                     col_types = "text") |>
#  dplyr::mutate(
#    latitude_estado =
#      readr::parse_number(latitude_estado,
#                          locale = readr::locale(decimal_mark = ",")),
#    longitude_estado =
#      readr::parse_number(longitude_estado,
#                          locale = readr::locale(decimal_mark = ",")))

#abate_animais <- abate_animais |> 
#  dplyr::left_join(territorialidade_sedec,
#                   by = dplyr::join_by(localidade_nome ==
#                                         ESTADO))


# Visualizar
abate_animais |> dplyr::glimpse()

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
