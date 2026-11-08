suppressPackageStartupMessages(library(tsfeatures))

input <- jsonlite::fromJSON(paste(readLines(file("stdin"), warn = FALSE), collapse = "\n"),
                            simplifyVector = FALSE)
metric_id <- input$metric_id
windows <- input$windows

evaluate_metric <- function(metric_id, x) {
  x <- as.numeric(unlist(x))
  warnings <- character()
  started <- proc.time()[["elapsed"]]
  result <- withCallingHandlers(
    tryCatch({
      value <- switch(
        metric_id,
        "tsf.x_acf1" = unname(acf_features(x)[["x_acf1"]]),
        "tsf.x_acf10" = unname(acf_features(x)[["x_acf10"]]),
        "tsf.x_pacf5" = unname(pacf_features(x)[["x_pacf5"]]),
        "tsf.arch_r2_l12" = unname(arch_stat(x, lags = 12, demean = TRUE)),
        "tsf.spectral_entropy" = unname(entropy(x)),
        "tsf.stability_w60" = unname(stability(x, width = 60)),
        "tsf.lumpiness_w60" = unname(lumpiness(x, width = 60)),
        "tsf.crossing_points" = unname(crossing_points(x)),
        stop("unsupported metric")
      )
      if (length(value) != 1L || !is.numeric(value)) {
        list(status = "NONFINITE", value = NULL, warnings = warnings)
      } else if (!is.finite(value)) {
        list(status = "NONFINITE", value = NULL, warnings = warnings)
      } else {
        list(status = "OK", value = as.numeric(value), warnings = warnings)
      }
    }, error = function(exc) {
      list(status = "ERROR", value = NULL, error = conditionMessage(exc),
           warnings = warnings)
    }),
    warning = function(w) {
      warnings <<- c(warnings, conditionMessage(w))
      invokeRestart("muffleWarning")
    }
  )
  result$elapsed_ms <- (proc.time()[["elapsed"]] - started) * 1000
  result
}

results <- lapply(windows, function(x) evaluate_metric(metric_id, x))
writeLines(jsonlite::toJSON(list(
  schema_version = "tsfeatures_backend_output_v1",
  metric_id = metric_id,
  r_version = R.version.string,
  tsfeatures_version = as.character(packageVersion("tsfeatures")),
  results = results
), auto_unbox = TRUE, null = "null", digits = NA))
