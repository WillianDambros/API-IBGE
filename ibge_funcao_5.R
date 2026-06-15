# Função principal para buscar dados do IBGE (VERSÃO FINAL CORRIGIDA)
busca_ibge <- function(agregado,
                       variaveis = "all",
                       localidades = "N3[all]",
                       periodos = "all",
                       combinar_tabelas = TRUE) {
  
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
  
  # Verificar se a primeira chamada foi bem-sucedida
  if (httr::http_error(response)) {
    cat("❌ Erro na primeira chamada. Status:", httr::status_code(response), 
        "\n")
    cat("   A API não retornou dados para esta combinação.\n")
    return(NULL)
  }
  
  json_base <- httr::content(response, "text", encoding = "UTF-8") |> 
    jsonlite::fromJSON(simplifyDataFrame = FALSE)
  
  # Verificar estrutura e extrair classificações
  if(length(json_base) == 0) {
    cat("⚠️ Nenhum dado retornado pela API na primeira chamada.\n")
    return(NULL)
  }
  
  # Extração robusta de classificações
  class_str <- ""
  
  if(is.list(json_base) && length(json_base) > 0) {
    primeiro_elemento <- json_base[[1]]
    
    lista_resultados <- NULL
    if(is.list(primeiro_elemento) && !is.null(primeiro_elemento$resultados)) {
      lista_resultados <- primeiro_elemento$resultados
    } else if(is.list(primeiro_elemento) && !is.null(primeiro_elemento$classificacoes)) {
      lista_resultados <- list(primeiro_elemento)
    }
    
    if(!is.null(lista_resultados) && length(lista_resultados) > 0) {
      primeiro_resultado <- lista_resultados[[1]]
      
      if(is.list(primeiro_resultado) && !is.null(primeiro_resultado$classificacoes)) {
        classificacoes_list <- primeiro_resultado$classificacoes
        
        if(is.list(classificacoes_list) && length(classificacoes_list) > 0) {
          for(j in seq_along(classificacoes_list)) {
            classificacao <- classificacoes_list[[j]]
            if(!is.null(classificacao$id)) {
              class_str <- paste0(class_str, "&classificacao=", classificacao$id, "[all]")
            }
          }
          cat("   Classificações encontradas:", 
              paste(sapply(classificacoes_list, function(x) x$nome), collapse = ", "), "\n")
        } else {
          cat("   Nenhuma classificação disponível para este agregado.\n")
        }
      } else {
        cat("   Estrutura de classificação não encontrada.\n")
      }
    } else {
      cat("   Nenhum resultado encontrado na primeira chamada.\n")
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
  response_final <- httr::GET(url_final)
  
  if (httr::http_error(response_final)) {
    cat("❌ Erro na requisição final. Status:", 
        httr::status_code(response_final), "\n")
    cat("   A API não retornou dados para esta combinação de agregado/período/localidade.\n")
    return(NULL)
  }
  
  json_ibge <- httr::content(response_final, "text", encoding = "UTF-8") |> 
    jsonlite::fromJSON()
  
  if(is.null(json_ibge) || length(json_ibge) == 0) {
    cat("⚠️ A API retornou uma estrutura vazia. Nenhum dado disponível.\n")
    return(NULL)
  }
  
  if(is.null(json_ibge$resultados) || length(json_ibge$resultados) == 0) {
    cat("⚠️ Nenhum resultado encontrado para esta consulta.\n")
    return(NULL)
  }
  
  if(is.null(json_ibge$id) || length(json_ibge$id) == 0) {
    cat("⚠️ Nenhuma variável encontrada para este agregado.\n")
    return(NULL)
  }
  
  cat("✅ Dados carregados com sucesso!\n")
  cat("   Total de variáveis encontradas:", length(json_ibge$id), "\n")
  
  # Função para processar um resultado específico - VERSÃO CORRIGIDA (com tratamento de classificações)
  processar_resultado_ibge <- function(json_ibge, indice) {
    
    if(indice > length(json_ibge$resultados)) {
      return(NULL)
    }
    
    num_series <- length(json_ibge$resultados[[indice]]$series)
    if(num_series == 0) return(NULL)
    
    lista_dados <- list()
    
    for(s in 1:num_series) {
      if(is.null(json_ibge$resultados[[indice]]$series[[s]]$localidade) ||
         is.null(json_ibge$resultados[[indice]]$series[[s]]$serie)) {
        next
      }
      
      dados_serie <- dplyr::bind_cols(
        json_ibge$resultados[[indice]]$series[[s]]$localidade |> 
          dplyr::select(-nivel),
        json_ibge$resultados[[indice]]$series[[s]]$serie
      )
      
      # ============================================================
      # CORREÇÃO: Extração segura das classificações (evita character(0))
      # ============================================================
      if(length(json_ibge$resultados[[indice]]$classificacoes) >= s) {
        classificacoes_info <- json_ibge$resultados[[indice]]$classificacoes[[s]]
        
        if(!is.null(classificacoes_info$id) && length(classificacoes_info$id) > 0) {
          for(i in seq_along(classificacoes_info$id)) {
            nome_class <- classificacoes_info$nome[i]
            
            # Inicializa como NA
            valor_class <- NA_character_
            
            # Tenta extrair o valor da categoria com segurança
            if(!is.null(classificacoes_info$categoria)) {
              # Se for matriz ou data.frame
              if(is.matrix(classificacoes_info$categoria) || is.data.frame(classificacoes_info$categoria)) {
                if(nrow(classificacoes_info$categoria) >= i && ncol(classificacoes_info$categoria) >= 1) {
                  tmp <- classificacoes_info$categoria[i, 1]
                  if(length(tmp) == 1 && !is.na(tmp)) {
                    valor_class <- as.character(tmp)
                  }
                }
              } else if(is.vector(classificacoes_info$categoria)) {
                # Se for vetor
                if(length(classificacoes_info$categoria) >= i) {
                  tmp <- classificacoes_info$categoria[i]
                  if(length(tmp) == 1 && !is.na(tmp)) {
                    valor_class <- as.character(tmp)
                  }
                }
              }
            }
            
            # Atribui a coluna (valor_class tem comprimento 1, será reciclado)
            dados_serie[[nome_class]] <- valor_class
          }
        }
      }
      
      lista_dados[[s]] <- dados_serie
    }
    
    dados <- dplyr::bind_rows(lista_dados)
    if(is.null(dados) || nrow(dados) == 0) return(NULL)
    
    dados <- dados |> 
      dplyr::rename(localidade_id = id, localidade_nome = nome) |>
      dplyr::mutate(
        variavel_id = json_ibge$id[indice],
        variavel_nome = json_ibge$variavel[indice],
        variavel_unidade = json_ibge$unidade[indice]
      )
    
    # Identificar colunas de período
    colunas_periodo <- names(dados)[grepl("^[0-9]{4}$", names(dados))]
    if(length(colunas_periodo) == 0) {
      colunas_periodo <- names(dados)[grepl("^[0-9]{6}$|^[0-9]{8}$", names(dados))]
    }
    if(length(colunas_periodo) == 0) {
      colunas_identificacao <- c("localidade_id", "localidade_nome", 
                                 "variavel_id", "variavel_nome", "variavel_unidade")
      colunas_periodo <- setdiff(names(dados), colunas_identificacao)
    }
    if(length(colunas_periodo) == 0) {
      cat("   ⚠️ Nenhuma coluna de período encontrada para variável", indice, "\n")
      return(NULL)
    }
    
    dados_long <- dados |>
      tidyr::pivot_longer(
        cols = tidyselect::all_of(colunas_periodo),
        names_to = "periodo",
        values_to = "valor"
      )
    
    return(dados_long)
  }
  
  processar_todos_resultados <- function(json_ibge) {
    lista_resultados <- list()
    total_variaveis <- length(json_ibge$id)
    
    for(i in 1:total_variaveis) {
      cat("Processando variável", i, "de", total_variaveis, ":",
          json_ibge$variavel[i], "\n")
      resultado <- processar_resultado_ibge(json_ibge, i)
      if(!is.null(resultado)) {
        lista_resultados[[i]] <- resultado
      } else {
        cat("   ⚠️ Variável sem dados disponíveis\n")
      }
    }
    
    if(length(lista_resultados) == 0) {
      cat("⚠️ Nenhuma variável com dados foi processada.\n")
      return(NULL)
    }
    
    names(lista_resultados) <- json_ibge$id[1:length(lista_resultados)]
    return(lista_resultados)
  }
  
  todos_resultados <- processar_todos_resultados(json_ibge)
  if(is.null(todos_resultados)) {
    cat("❌ Processamento interrompido: nenhum dado válido foi encontrado.\n")
    return(NULL)
  }
  
  cat("\n🔄 Aplicando pós-processamento com tidyverse...\n")
  
  todos_resultados <- purrr::map(todos_resultados, function(tabela) {
    if(is.null(tabela)) return(NULL)
    
    if("Referência temporal" %in% names(tabela)) {
      ref_temp_unique <- tabela |> dplyr::pull(`Referência temporal`) |> unique()
      if(length(ref_temp_unique) == 1 && !is.na(ref_temp_unique)) {
        ref_temp_clean <- ref_temp_unique |>
          stringr::str_to_lower() |>
          stringr::str_replace_all(" ", "_") |>
          stringr::str_replace_all("[^a-z0-9_]", "")
        tabela <- tabela |>
          dplyr::rename_with(~ paste0("periodo_", ref_temp_clean), .cols = "periodo") |>
          dplyr::select(-`Referência temporal`)
      }
    }
    
    if(all(c("variavel_nome", "variavel_unidade") %in% names(tabela))) {
      nome_unique <- tabela |> dplyr::pull(variavel_nome) |> unique()
      unidade_unique <- tabela |> dplyr::pull(variavel_unidade) |> unique()
      if(length(nome_unique) == 1 && length(unidade_unique) == 1) {
        nome_clean <- nome_unique |>
          stringr::str_to_lower() |>
          stringr::str_replace_all("[^a-z0-9]", "_") |>
          stringr::str_replace_all("_+", "_") |>
          stringr::str_remove_all("^_|_$")
        unidade_clean <- unidade_unique |>
          stringr::str_to_lower() |>
          stringr::str_replace_all("[^a-z0-9]", "_") |>
          stringr::str_replace_all("_+", "_") |>
          stringr::str_remove_all("^_|_$")
        novo_nome_valor <- paste0(nome_clean, "_", unidade_clean)
        tabela <- tabela |>
          dplyr::rename_with(~ novo_nome_valor, .cols = "valor") |>
          dplyr::select(-variavel_nome, -variavel_unidade)
      }
    }
    
    if("variavel_id" %in% names(tabela)) {
      tabela <- tabela |> dplyr::select(-variavel_id)
    }
    
    return(tabela)
  })
  
  todos_resultados <- purrr::compact(todos_resultados)
  
  if(length(todos_resultados) == 0) {
    cat("❌ Nenhuma tabela válida após o pós-processamento.\n")
    return(NULL)
  }
  
  cat("✅ Pós-processamento concluído!\n")
  cat("   Total de tabelas processadas:", length(todos_resultados), "\n")
  
  if(combinar_tabelas && length(todos_resultados) > 0) {
    cat("\n🔄 Combinando todas as tabelas em um único dataframe...\n")
    
    todas_colunas <- purrr::map(todos_resultados, names)
    colunas_comuns <- Reduce(intersect, todas_colunas)
    
    if(length(colunas_comuns) == 0) {
      cat("⚠️ Não foi possível identificar colunas comuns entre as tabelas.\n")
      cat("   Retornando a lista de tabelas separadas.\n")
      return(todos_resultados)
    }
    
    cat("   Colunas comuns identificadas:\n")
    cat("     ", paste(colunas_comuns, collapse = ", "), "\n")
    
    colunas_localidade <- c("localidade_id", "localidade_nome")
    colunas_chave <- colunas_localidade
    colunas_classificacao <- setdiff(colunas_comuns, c("localidade_id", "localidade_nome"))
    
    coluna_periodo <- colunas_classificacao[stringr::str_starts(colunas_classificacao, "periodo_")]
    if(length(coluna_periodo) > 0) {
      colunas_chave <- c(colunas_chave, coluna_periodo)
      colunas_classificacao <- setdiff(colunas_classificacao, coluna_periodo)
    }
    if(length(colunas_classificacao) > 0) {
      colunas_chave <- c(colunas_chave, colunas_classificacao)
    }
    
    cat("   Colunas chave para combinação:\n")
    cat("     ", paste(colunas_chave, collapse = ", "), "\n")
    
    tabela_combinada <- todos_resultados[[1]] |> dplyr::select(dplyr::all_of(colunas_chave))
    
    for(nome_tabela in names(todos_resultados)) {
      tabela <- todos_resultados[[nome_tabela]]
      colunas_especificas <- setdiff(names(tabela), colunas_chave)
      if(length(colunas_especificas) > 0) {
        for(coluna_espec in colunas_especificas) {
          cat("     Adicionando coluna:", coluna_espec, "\n")
          tabela_combinada <- tabela_combinada |>
            dplyr::left_join(
              tabela |> dplyr::select(dplyr::all_of(c(colunas_chave, coluna_espec))),
              by = colunas_chave
            )
        }
      }
    }
    
    cat("✅ Tabelas combinadas com sucesso!\n")
    cat("   Total de linhas:", nrow(tabela_combinada), "\n")
    cat("   Total de colunas:", ncol(tabela_combinada), "\n")
    
    return(tabela_combinada)
  }
  
  cat("✅ Processamento concluído!\n")
  return(todos_resultados)
}
