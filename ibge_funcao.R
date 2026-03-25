# 1. Carregar os dados da API
base_url_ibge <- "https://servicodados.ibge.gov.br/api/v3/agregados"

url_ibge <- paste0("https://servicodados.ibge.gov.br/api/v3/agregados/1092/",
                   "periodos/202401-202504/variaveis/all?localidades=N3[all]&",
                   "classificacao=18[all]&classificacao=12529[all]&classificacao=12716[all]")

# 2. Fazer a requisição e parsear o JSON
json_ibge <- httr::GET(url_ibge) |> 
  httr::content("text", encoding = "UTF-8") |> 
  jsonlite::fromJSON()

# Função para processar um resultado específico (por índice)
processar_resultado_ibge <- function(json_ibge, indice) {
  # Extrair dados do resultado específico
  dados <- dplyr::bind_cols(
    json_ibge$resultados[[indice]]$series[[1]]$localidade |>
      dplyr::select(-nivel),
    json_ibge$resultados[[indice]]$series[[1]]$serie
  )
  
  # Renomear colunas de localidade
  dados <- dados |> 
    dplyr::rename(localidade_id = id, localidade_nome = nome)
  
  # Extrair informações das classificações
  classificacoes_info <- json_ibge$resultados[[indice]]$classificacoes[[1]]
  
  # Adicionar colunas de classificação
  dados <- dados |>
    dplyr::mutate(
      !!!setNames(
        lapply(1:length(classificacoes_info$id), function(i) {
          as.character(
            classificacoes_info$categoria[
              i,!is.na(classificacoes_info$categoria[i, ])])
        }),
        classificacoes_info$nome
      )
    )
  
  # Adicionar informações da variável
  dados <- dados |>
    dplyr::mutate(
      variavel_id = json_ibge$id[indice],
      variavel_nome = json_ibge$variavel[indice],
      variavel_unidade = json_ibge$unidade[indice]
    )
  
  # Transformar para formato longo (colunas com formato YYYYMM ou YYYYMMDD)
  dados_long <- dados |>
    tidyr::pivot_longer(
      cols = matches("^[0-9]{6}$|^[0-9]{8}$"),  # pega colunas com 6 ou 8 dígitos (YYYYMM ou YYYYMMDD)
      names_to = "periodo",
      values_to = "valor"
    )
  
  return(dados_long)
}

# Processar todos os resultados e criar uma lista
processar_todos_resultados <- function(json_ibge) {
  lista_resultados <- list()
  
  for(i in 1:length(json_ibge$id)) {
    cat("Processando variável", i, "de", length(json_ibge$id), ":",
        json_ibge$variavel[i], "\n")
    lista_resultados[[i]] <- processar_resultado_ibge(json_ibge, i)
  }
  
  names(lista_resultados) <- json_ibge$id
  
  return(lista_resultados)
}

# Executar
todos_resultados <- processar_todos_resultados(json_ibge)

# Ver a estrutura da lista
names(todos_resultados)
# [1] "151"     "1000151" "284"     "1000284" "285"     "1000285"

# Visualizar cada um
todos_resultados[["151"]] |> dplyr::glimpse()  # Número de informantes
todos_resultados[["284"]] |> dplyr::glimpse()  # Animais abatidos
todos_resultados[["285"]] |> dplyr::glimpse()  # Peso total das carcaças



todos_resultados |> dplyr::glimpse()
