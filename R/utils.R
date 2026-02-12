#' Convert R data to Arrow IPC bytes for transfer to Rust
#'
#' Accepts a data.frame or arrow::Table and serializes it to Arrow IPC
#' streaming format as a raw vector.
#'
#' @param data A `data.frame` or `arrow::Table`.
#' @return A raw vector of Arrow IPC stream bytes.
#' @noRd
data_to_ipc <- function(data) {
  if (is.data.frame(data) && !inherits(data, "ArrowTabular")) {
    tbl <- arrow::as_arrow_table(data)
  } else if (inherits(data, "ArrowTabular")) {
    tbl <- data
  } else {
    rlang::abort("data must be a data.frame or arrow::Table")
  }

  # Serialize to IPC stream bytes
  buf <- arrow::BufferOutputStream$create()
  writer <- arrow::RecordBatchStreamWriter$create(buf, tbl$schema)
  for (batch in tbl$to_batches()) {
    writer$write(batch)
  }
  writer$close()
  buf$finish()$data()
}

#' Minimal JSON parser for schema metadata
#'
#' Parses a simple JSON array of objects. Only handles the specific format
#' returned by `rust_table_schema_json()`.
#'
#' @param json_str A JSON string.
#' @return A list of named lists.
#' @noRd
jsonlite_parse <- function(json_str) {
  # Try to use jsonlite if available, otherwise basic parsing
  if (requireNamespace("jsonlite", quietly = TRUE)) {
    return(jsonlite::fromJSON(json_str, simplifyVector = FALSE))
  }

  # Fallback: very basic parser for our known format
  # [{"name":"x","type":"Int32","nullable":true}, ...]
  tryCatch({
    eval(parse(text = gsub("null", "NULL",
      gsub("false", "FALSE",
        gsub("true", "TRUE",
          gsub(":", "=",
            gsub("\\{", "list(",
              gsub("\\}", ")",
                gsub("\\[", "list(",
                  gsub("\\]", ")", json_str)
                )
              )
            )
          )
        )
      )
    )))
  }, error = function(e) list())
}

#' Convert an R list to a JSON string
#'
#' Serializes a named list to a JSON object string. Uses jsonlite if
#' available, otherwise a simple manual serializer for flat lists.
#'
#' @param x A named list (can contain scalars, character vectors, logicals).
#' @return A JSON string.
#' @noRd
jsonlite_to_json <- function(x) {
  if (length(x) == 0) return("{}")

  if (requireNamespace("jsonlite", quietly = TRUE)) {
    return(jsonlite::toJSON(x, auto_unbox = TRUE))
  }

  # Manual fallback for flat lists
  parts <- vapply(names(x), function(nm) {
    val <- x[[nm]]
    if (is.logical(val)) {
      json_val <- if (val) "true" else "false"
    } else if (is.numeric(val)) {
      json_val <- as.character(val)
    } else if (is.character(val) && length(val) == 1) {
      json_val <- paste0('"', gsub('"', '\\\\"', val), '"')
    } else if (is.character(val) && length(val) > 1) {
      json_val <- paste0("[", paste0('"', gsub('"', '\\\\"', val), '"', collapse = ","), "]")
    } else {
      json_val <- paste0('"', as.character(val), '"')
    }
    paste0('"', nm, '":', json_val)
  }, character(1))

  paste0("{", paste(parts, collapse = ","), "}")
}
