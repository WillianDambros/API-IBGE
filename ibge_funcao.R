# Função principal para buscar dados do IBGE
busca_ibge <- function(agregado,
                       variaveis = "all",
                       localidades = "N3[all]",
                       periodos = "all") {
  
  base_url <- "https://servicodados.ibge.gov.br/api/v3/agregados"
  
  # Montar URL base
  url_base <- paste0(
    base_url, "/", agregado,
    "/periodos/", periodos,
    "/variaveis/", variaveis,
    "?localidades=", localidades
  )
  
  # Primeira chamada para descobrir as classificações disponíveis
  cat("🔍 Buscando classificações disponíveis...\n")
  cat("   URL: ", url_base, "\n")
  
  response <- httr::GET(url_base)
  json_base <- httr::content(response, "text", encoding = "UTF-8") |> 
    jsonlite::fromJSON(simplifyDataFrame = FALSE)
  
  # Verificar estrutura e extrair classificações
  if(length(json_base) == 0) {
    stop("Nenhum dado retornado pela API")
  }
  
  # Tentar extrair todas as classificações de forma segura
  class_str <- ""
  
  # Verificar se é uma lista e tem a estrutura esperada
  if(is.list(json_base)) {
    # Tentar acessar o primeiro elemento
    primeiro_elemento <- json_base[[1]]
    
    if(is.list(primeiro_elemento) && !is.null(primeiro_elemento$resultados)) {
      if(length(primeiro_elemento$resultados) > 0) {
        primeiro_resultado <- primeiro_elemento$resultados[[1]]
        if(is.list(primeiro_resultado) && !is.null(primeiro_resultado$classificacoes)) {
          # Extrair TODAS as classificações
          classificacoes_list <- primeiro_resultado$classificacoes
          
          # Montar string com todas as classificações
          class_parts <- c()
          for(j in 1:length(classificacoes_list)) {
            classificacao <- classificacoes_list[[j]]
            class_parts <- c(class_parts, paste0("&classificacao=", classificacao$id, "[all]"))
          }
          class_str <- paste(class_parts, collapse = "")
          
          cat("   Classificações encontradas:", 
              paste(sapply(classificacoes_list, function(x) x$nome), collapse = ", "), "\n")
        }
      }
    } else if(is.list(primeiro_elemento) && !is.null(primeiro_elemento$classificacoes)) {
      # Formato alternativo
      classificacoes_list <- primeiro_elemento$classificacoes
      class_parts <- c()
      for(j in 1:length(classificacoes_list)) {
        classificacao <- classificacoes_list[[j]]
        class_parts <- c(class_parts, paste0("&classificacao=", classificacao$id, "[all]"))
      }
      class_str <- paste(class_parts, collapse = "")
      cat("   Classificações encontradas:", 
          paste(sapply(classificacoes_list, function(x) x$nome), collapse = ", "), "\n")
    } else {
      cat("   Nenhuma classificação encontrada para este agregado.\n")
    }
  }
  
  # Montar URL final com as classificações (se houver)
  if(class_str != "") {
    url_final <- paste0(url_base, class_str)
  } else {
    url_final <- url_base
    cat("   Nenhuma classificação adicionada à URL.\n")
  }
  
  cat("📊 Buscando dados completos...\n")
  cat("   URL: ", url_final, "\n")
  
  # Chamada final para obter todos os dados
  json_ibge <- httr::GET(url_final) |>
    httr::content("text", encoding = "UTF-8") |>
    jsonlite::fromJSON()
  
  cat("✅ Dados carregados com sucesso!\n")
  
  # Função para processar um resultado específico (por índice)
  processar_resultado_ibge <- function(json_ibge, indice) {
    
    # Determinar quantas séries existem neste resultado
    num_series <- length(json_ibge$resultados[[indice]]$series)
    
    # Criar uma lista para armazenar os dados de cada série
    lista_dados <- list()
    
    # Iterar sobre todas as séries
    for(s in 1:num_series) {
      # Extrair dados da série específica
      dados_serie <- dplyr::bind_cols(
        json_ibge$resultados[[indice]]$series[[s]]$localidade |> 
          dplyr::select(-nivel),
        json_ibge$resultados[[indice]]$series[[s]]$serie
      )
      
      # Extrair informações das classificações correspondentes a esta série
      if(length(json_ibge$resultados[[indice]]$classificacoes) >= s) {
        classificacoes_info <- json_ibge$resultados[[indice]]$classificacoes[[s]]
        
        # Adicionar colunas de classificação
        dados_serie <- dados_serie |>
          dplyr::mutate(
            !!!setNames(
              lapply(1:length(classificacoes_info$id), function(i) {
                as.character(
                  classificacoes_info$categoria[
                    i, !is.na(classificacoes_info$categoria[i, ])])
              }),
              classificacoes_info$nome
            )
          )
      }
      
      lista_dados[[s]] <- dados_serie
    }
    
    # Combinar todos os dados das séries
    dados <- dplyr::bind_rows(lista_dados)
    
    # Renomear colunas de localidade
    dados <- dados |> 
      dplyr::rename(localidade_id = id, localidade_nome = nome)
    
    # Adicionar informações da variável
    dados <- dados |>
      dplyr::mutate(
        variavel_id = json_ibge$id[indice],
        variavel_nome = json_ibge$variavel[indice],
        variavel_unidade = json_ibge$unidade[indice]
      )
    
    # Transformar para formato longo
    dados_long <- dados |>
      tidyr::pivot_longer(
        cols = matches("^[0-9]{6}$|^[0-9]{8}$"),
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
  
  cat("✅ Processamento concluído!\n")
  
  return(todos_resultados)
}

# Testar
resultados_1092 <- busca_ibge(1092, periodos = "202401-202504", localidades = "N3[all]")

resultados_1092 |> dplyr::glimpse()

# Verificar todas as classificações
cat("\n=== Verificando classificações ===\n")
cat("Tipo de rebanho bovino:\n")
print(resultados_1092[[1]]$`Tipo de rebanho bovino` |> unique())

cat("\nTipo de inspeção:\n")
print(resultados_1092[[1]]$`Tipo de inspeção` |> unique())

cat("\nReferência temporal:\n")
print(resultados_1092[[1]]$`Referência temporal` |> unique())
