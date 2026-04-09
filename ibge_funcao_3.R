# Função principal para buscar dados do IBGE
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
  
  # Tentar extrair todas as classificações de forma segura
  class_str <- ""
  
  # Verificar se é uma lista e tem a estrutura esperada
  if(is.list(json_base)) {
    # Tentar acessar o primeiro elemento
    primeiro_elemento <- json_base[[1]]
    
    if(is.list(primeiro_elemento) && !is.null(primeiro_elemento$resultados)) {
      if(length(primeiro_elemento$resultados) > 0) {
        primeiro_resultado <- primeiro_elemento$resultados[[1]]
        if(is.list(primeiro_resultado) && !is.null(
          primeiro_resultado$classificacoes)) {
          # Extrair TODAS as classificações
          classificacoes_list <- primeiro_resultado$classificacoes
          
          # Montar string com todas as classificações
          class_parts <- c()
          for(j in 1:length(classificacoes_list)) {
            classificacao <- classificacoes_list[[j]]
            class_parts <- c(class_parts, paste0("&classificacao=",
                                                 classificacao$id, "[all]"))
          }
          class_str <- paste(class_parts, collapse = "")
          
          cat("   Classificações encontradas:", 
              paste(sapply(classificacoes_list, function(x) x$nome),
                    collapse = ", "), "\n")
        }
      }
    } else if(is.list(primeiro_elemento) && !is.null(
      primeiro_elemento$classificacoes)) {
      # Formato alternativo
      classificacoes_list <- primeiro_elemento$classificacoes
      class_parts <- c()
      for(j in 1:length(classificacoes_list)) {
        classificacao <- classificacoes_list[[j]]
        class_parts <- c(class_parts, paste0("&classificacao=",
                                             classificacao$id, "[all]"))
      }
      class_str <- paste(class_parts, collapse = "")
      cat("   Classificações encontradas:", 
          paste(sapply(classificacoes_list, function(x) x$nome),
                collapse = ", "), "\n")
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
  response_final <- httr::GET(url_final)
  
  # Verificar se a requisição final foi bem-sucedida
  if (httr::http_error(response_final)) {
    cat("❌ Erro na requisição final. Status:", 
        httr::status_code(response_final), "\n")
    cat("   A API não retornou dados para esta combinação de agregado/período/localidade.\n")
    cat("   Possíveis causas:\n")
    cat("     - O agregado não possui dados para o período solicitado\n")
    cat("     - O agregado foi descontinuado\n")
    cat("     - Problema temporário na API do IBGE\n")
    return(NULL)
  }
  
  json_ibge <- httr::content(response_final, "text", encoding = "UTF-8") |> 
    jsonlite::fromJSON()
  
  # Verificar se o JSON tem a estrutura esperada
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
  
  # Função para processar um resultado específico (por índice) - VERSÃO CORRIGIDA
  processar_resultado_ibge <- function(json_ibge, indice) {
    
    # Verificar se o índice existe
    if(indice > length(json_ibge$resultados)) {
      return(NULL)
    }
    
    # Determinar quantas séries existem neste resultado
    num_series <- length(json_ibge$resultados[[indice]]$series)
    
    if(num_series == 0) {
      return(NULL)
    }
    
    # Criar uma lista para armazenar os dados de cada série
    lista_dados <- list()
    
    # Iterar sobre todas as séries
    for(s in 1:num_series) {
      # Verificar se a série tem a estrutura esperada
      if(is.null(json_ibge$resultados[[indice]]$series[[s]]$localidade) ||
         is.null(json_ibge$resultados[[indice]]$series[[s]]$serie)) {
        next
      }
      
      # Extrair dados da série específica
      dados_serie <- dplyr::bind_cols(
        json_ibge$resultados[[indice]]$series[[s]]$localidade |> 
          dplyr::select(-nivel),
        json_ibge$resultados[[indice]]$series[[s]]$serie
      )
      
      # Extrair informações das classificações correspondentes a esta série
      if(length(json_ibge$resultados[[indice]]$classificacoes) >= s) {
        classificacoes_info <- json_ibge$resultados[[indice]]$classificacoes[[s]]
        
        # Adicionar colunas de classificação de forma segura
        for(i in 1:length(classificacoes_info$id)) {
          nome_class <- classificacoes_info$nome[i]
          # Pegar o primeiro valor não-NA da categoria
          valor_class <- as.character(classificacoes_info$categoria[i, 1])
          dados_serie[[nome_class]] <- valor_class
        }
      }
      
      lista_dados[[s]] <- dados_serie
    }
    
    # Combinar todos os dados das séries
    dados <- dplyr::bind_rows(lista_dados)
    
    if(is.null(dados) || nrow(dados) == 0) {
      return(NULL)
    }
    
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
    
    # CORREÇÃO: Identificar colunas que são períodos (anos)
    # Baseado na estrutura dos dados, as colunas de período são as que têm nomes com 4 dígitos
    colunas_periodo <- names(dados)[grepl("^[0-9]{4}$", names(dados))]
    
    # Se não encontrou com 4 dígitos, tentar outros padrões
    if(length(colunas_periodo) == 0) {
      colunas_periodo <- names(dados)[grepl("^[0-9]{6}$|^[0-9]{8}$", names(dados))]
    }
    
    # Se ainda não encontrou, usar todas as colunas que não são as de identificação
    if(length(colunas_periodo) == 0) {
      colunas_identificacao <- c("localidade_id", "localidade_nome", 
                                 "variavel_id", "variavel_nome", "variavel_unidade")
      colunas_periodo <- setdiff(names(dados), colunas_identificacao)
    }
    
    if(length(colunas_periodo) == 0) {
      cat("   ⚠️ Nenhuma coluna de período encontrada para variável", indice, "\n")
      return(NULL)
    }
    
    # Transformar para formato longo
    dados_long <- dados |>
      tidyr::pivot_longer(
        cols = tidyselect::all_of(colunas_periodo),
        names_to = "periodo",
        values_to = "valor"
      )
    
    return(dados_long)
  }
  
  # Processar todos os resultados e criar uma lista
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
  
  # Executar
  todos_resultados <- processar_todos_resultados(json_ibge)
  
  if(is.null(todos_resultados)) {
    cat("❌ Processamento interrompido: nenhum dado válido foi encontrado.\n")
    return(NULL)
  }
  
  # ============================================
  # PÓS-PROCESSAMENTO COM TIDYVERSE
  # ============================================
  cat("\n🔄 Aplicando pós-processamento com tidyverse...\n")
  
  todos_resultados <- purrr::map(todos_resultados, function(tabela) {
    
    if(is.null(tabela)) return(NULL)
    
    # 1. Tratamento da coluna periodo com base na Referência temporal
    if("Referência temporal" %in% names(tabela)) {
      ref_temp_unique <- tabela |> 
        dplyr::pull(`Referência temporal`) |> 
        unique()
      
      if(length(ref_temp_unique) == 1 && !is.na(ref_temp_unique)) {
        # Converter para snake_case
        ref_temp_clean <- ref_temp_unique |>
          stringr::str_to_lower() |>
          stringr::str_replace_all(" ", "_") |>
          stringr::str_replace_all("[^a-z0-9_]", "")
        
        # Renomear coluna periodo e remover Referência temporal
        tabela <- tabela |>
          dplyr::rename_with(
            ~ paste0("periodo_", ref_temp_clean),
            .cols = "periodo"
          ) |>
          dplyr::select(-`Referência temporal`)
      }
    }
    
    # 2. Tratamento da coluna valor combinando variavel_nome + variavel_unidade
    if(all(c("variavel_nome", "variavel_unidade") %in% names(tabela))) {
      
      nome_unique <- tabela |> dplyr::pull(variavel_nome) |> unique()
      unidade_unique <- tabela |> dplyr::pull(variavel_unidade) |> unique()
      
      if(length(nome_unique) == 1 && length(unidade_unique) == 1) {
        # Criar nome da coluna no formato snake_case
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
        
        # Renomear coluna valor e remover colunas originais
        tabela <- tabela |>
          dplyr::rename_with(
            ~ novo_nome_valor,
            .cols = "valor"
          ) |>
          dplyr::select(-variavel_nome, -variavel_unidade)
      }
    }
    
    # 3. Remover a coluna variavel_id
    if("variavel_id" %in% names(tabela)) {
      tabela <- tabela |>
        dplyr::select(-variavel_id)
    }
    
    return(tabela)
  })
  
  # Remover elementos NULL da lista
  todos_resultados <- purrr::compact(todos_resultados)
  
  if(length(todos_resultados) == 0) {
    cat("❌ Nenhuma tabela válida após o pós-processamento.\n")
    return(NULL)
  }
  
  cat("✅ Pós-processamento concluído!\n")
  cat("   Total de tabelas processadas:", length(todos_resultados), "\n")
  
  # ============================================
  # COMBINAR TABELAS EM UM ÚNICO DATAFRAME (VERSÃO GENÉRICA)
  # ============================================
  
  if(combinar_tabelas && length(todos_resultados) > 0) {
    cat("\n🔄 Combinando todas as tabelas em um único dataframe...\n")
    
    # 1. Identificar dinamicamente as colunas comuns (presentes em TODAS as tabelas)
    todas_colunas <- purrr::map(todos_resultados, names)
    colunas_comuns <- Reduce(intersect, todas_colunas)
    
    if(length(colunas_comuns) == 0) {
      cat("⚠️ Não foi possível identificar colunas comuns entre as tabelas.\n")
      cat("   Retornando a lista de tabelas separadas.\n")
      return(todos_resultados)
    }
    
    cat("   Colunas comuns identificadas:\n")
    cat("     ", paste(colunas_comuns, collapse = ", "), "\n")
    
    # 2. Identificar colunas que são chave (que queremos manter como identificadores)
    # As colunas de localidade sempre serão chave
    colunas_localidade <- c("localidade_id", "localidade_nome")
    colunas_chave <- colunas_localidade
    
    # Adicionar colunas de classificação (qualquer coluna que não seja período e não seja a coluna de valor)
    colunas_classificacao <- setdiff(colunas_comuns, c("localidade_id", "localidade_nome"))
    
    # Identificar a coluna de período (começa com "periodo_")
    coluna_periodo <- colunas_classificacao[
      stringr::str_starts(colunas_classificacao, "periodo_")]
    if(length(coluna_periodo) > 0) {
      colunas_chave <- c(colunas_chave, coluna_periodo)
      colunas_classificacao <- setdiff(colunas_classificacao, coluna_periodo)
    }
    
    # Adicionar as colunas de classificação restantes como chave
    if(length(colunas_classificacao) > 0) {
      colunas_chave <- c(colunas_chave, colunas_classificacao)
    }
    
    cat("   Colunas chave para combinação:\n")
    cat("     ", paste(colunas_chave, collapse = ", "), "\n")
    
    # 3. Iniciar com a primeira tabela (apenas as colunas chave)
    tabela_combinada <- todos_resultados[[1]] |>
      dplyr::select(dplyr::all_of(colunas_chave))
    
    # 4. Adicionar as colunas específicas de cada tabela
    for(nome_tabela in names(todos_resultados)) {
      tabela <- todos_resultados[[nome_tabela]]
      
      # Identificar colunas que NÃO são chave (são as variáveis específicas)
      colunas_especificas <- setdiff(names(tabela), colunas_chave)
      
      if(length(colunas_especificas) > 0) {
        # Adicionar cada coluna específica
        for(coluna_espec in colunas_especificas) {
          cat("     Adicionando coluna:", coluna_espec, "\n")
          
          tabela_combinada <- tabela_combinada |>
            dplyr::left_join(
              tabela |>
                dplyr::select(dplyr::all_of(c(colunas_chave, coluna_espec))),
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