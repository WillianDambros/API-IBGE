# Função que busca TODOS os agregados de uma pesquisa
buscar_todos_agregados_pesquisa <- function(codigo_pesquisa, 
                                            periodos = "all", 
                                            localidades = "N3[all]") {
  
  # URL para obter a lista de agregados da pesquisa
  url_pesquisa <- paste0("https://servicodados.ibge.gov.br/api/v3/agregados/", codigo_pesquisa)
  
  cat("🔍 Buscando lista de agregados da pesquisa", codigo_pesquisa, "...\n")
  
  json_pesquisa <- httr::GET(url_pesquisa) |>
    httr::content("text", encoding = "UTF-8") |>
    jsonlite::fromJSON()
  
  if(is.null(json_pesquisa$agregados)) {
    stop("Nenhum agregado encontrado para esta pesquisa")
  }
  
  agregados <- json_pesquisa$agregados
  cat("✅ Encontrados", nrow(agregados), "agregados\n\n")
  
  # Lista para armazenar os resultados
  resultados <- list()
  
  for(i in 1:nrow(agregados)) {
    agregado_id <- agregados$id[i]
    agregado_nome <- agregados$nome[i]
    
    cat("📊 [", i, "/", nrow(agregados), "] Processando agregado:", 
        agregado_id, "-", substr(agregado_nome, 1, 50), "...\n")
    
    tryCatch({
      # Usar sua função busca_ibge existente
      resultados[[agregado_id]] <- busca_ibge(
        agregado = agregado_id,
        periodos = periodos,
        localidades = localidades
      )
      cat("   ✅ OK\n")
    }, error = function(e) {
      cat("   ❌ Erro:", e$message, "\n")
    })
  }
  
  # Adicionar nomes dos agregados como atributo
  attr(resultados, "agregados_nomes") <- setNames(agregados$nome, agregados$id)
  
  cat("\n✅ Processamento concluído!\n")
  cat("   Total de agregados processados:", length(resultados), "\n")
  
  return(resultados)
}

# Usar a função
todos_agregados_1092 <- buscar_todos_agregados_pesquisa(
  codigo_pesquisa = 1092,
  periodos = "202401-202504",
  localidades = "N3[all]"
)

# Ver quais agregados foram carregados
names(todos_agregados_1092)

# Acessar um agregado específico
todos_agregados_1092[["1092"]]  # Efetivo dos rebanhos
todos_agregados_1092[["1094"]]  # Abate de animais (se existir)
