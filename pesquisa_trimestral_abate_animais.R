source("X:/POWER BI/IBGE/ibge_funcao.R")

# Buscar dados
bovinos <- busca_ibge(1092, periodos = "202401-202504", localidades = "N3[all]")
suinos <- busca_ibge(1093, periodos = "202401-202504", localidades = "N3[all]")
frangos <- busca_ibge(1094, periodos = "202401-202504", localidades = "N3[all]")

# ============================================
# SUBSTITUIR "Total" POR IDENTIFICADORES
# ============================================

# Bovinos: adicionar "Bovinos - " em todas as categorias
bovinos <- bovinos |> 
  dplyr::rename(tipo_rebanho = `Tipo de rebanho bovino`) |>
  dplyr::mutate(
    tipo_rebanho = paste0("Bovinos - ", tipo_rebanho),
    `Tipo de inspeção` = paste0("Bovinos - ", `Tipo de inspeção`)
  )

# Suínos: criar coluna tipo_rebanho com "Suínos - Total"
suinos <- suinos |> 
  dplyr::mutate(
    tipo_rebanho = "Suínos - Total",
    `Tipo de inspeção` = paste0("Suínos - ", `Tipo de inspeção`)
  )

# Frangos: criar coluna tipo_rebanho com "Frangos - Total"
frangos <- frangos |> 
  dplyr::mutate(
    tipo_rebanho = "Frangos - Total",
    `Tipo de inspeção` = paste0("Frangos - ", `Tipo de inspeção`)
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

# Visualizar
abate_animais |> dplyr::glimpse()

# Verificar os valores
abate_animais |>
  dplyr::count(tipo_rebanho, `Tipo de inspeção`) |>
  print(n = 30)

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