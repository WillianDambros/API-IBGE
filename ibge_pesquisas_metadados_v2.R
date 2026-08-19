
## API Metadados IBGE
# https://apimetadados.ibge.gov.br/

## API de metadados
# https://servicodados.ibge.gov.br/api/docs/metadados?versao=2

########################################### API METADADOS IBGE 2 # Funcionando

endereco_metadados_v2 <- "https://servicodados.ibge.gov.br/api/v2/metadados/pesquisas"

metadados_v2 <- httr::GET(endereco_metadados_v2)

endereco_metadados_v2 <- httr::content(metadados_v2, "text", encoding = "UTF-8") |> 
  jsonlite::fromJSON(simplifyDataFrame = FALSE)

metadados_v2 |> dplyr::glimpse()

metadados_v2$content

metadados_v2 <- httr::content(
  metadados_v2,
  "text",
  encoding = "UTF-8"
) |>
  jsonlite::fromJSON(
    simplifyDataFrame = FALSE
  )

metadados_v2 |> dplyr::glimpse()

pesquisas_ibge_v2 <- metadados_v2 |>
  dplyr::bind_rows() |>
  dplyr::select(
    codigo,
    nome,
    situacao
  )

pesquisas_ibge_v2 |> print(n = Inf)

############################################################ opção 2 sem select



# lista_filtrada <- purrr::keep(metadados_v2, ~ .x$codigo == "SR")

pesquisas_ibge_v2_2 <- metadados_v2 |>
  dplyr::bind_rows()

pesquisas_ibge_v2_2 |> print(n = Inf)

# pesquisas_ibge_v2_2 |> dplyr::select(nome) |> print(n = Inf)

#pesquisas_ibge_v2_teste |> dplyr::filter(nome == "Sistema de Contas Regionais Brasil") |> dplyr::glimpse()

#classificacoes_tematicas_exploracao <- pesquisas_ibge_v2_teste |> dplyr::filter(nome == "Sistema de Contas Regionais Brasil") |>
#  dplyr::select(classificacoes_tematicas)

#classificacoes_tematicas_exploracao
#classificacoes_tematicas_exploracao[[1]]
#classificacoes_tematicas_exploracao[[2]]

