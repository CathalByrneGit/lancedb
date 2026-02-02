.ensure_pyarrow <- function() {
  .ensure_python()
  if (!reticulate::py_module_available("pyarrow")) {
    reticulate::py_install("pyarrow", pip = TRUE)
  }
  invisible(TRUE)
}

.df_to_pyarrow_table <- function(df, vector_column = "vector") {
  stopifnot(is.data.frame(df))
  .ensure_pyarrow()

  if (!vector_column %in% names(df)) {
    stop("Vector column not found: ", vector_column, call. = FALSE)
  }
  v <- df[[vector_column]]
  if (!is.list(v) || length(v) < 1L) {
    stop("`", vector_column, "` must be a non-empty list-column of numeric vectors.", call. = FALSE)
  }

  lens <- vapply(v, length, integer(1))
  dim <- lens[[1]]
  if (dim < 1L || any(lens != dim)) {
    stop("All vectors in `", vector_column, "` must have the same positive length.", call. = FALSE)
  }

  pa <- reticulate::import("pyarrow", delay_load = FALSE)

  cols <- list()
  for (nm in names(df)) {
    if (nm == vector_column) {
      vec_type <- pa$list_(pa$float32(), dim)   # FixedSizeList<float32>[dim]
      cols[[nm]] <- pa$array(df[[nm]], type = vec_type)
    } else {
      cols[[nm]] <- pa$array(df[[nm]])
    }
  }

  pa$table(cols)
}
