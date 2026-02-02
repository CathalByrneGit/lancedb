#' Connect to a LanceDB database
#'
#' Creates a connection to a LanceDB database at the specified URI.
#' This is a backend-agnostic wrapper that dispatches to the active
#' backend implementation (currently \code{"reticulate"}).
#'
#' @param uri Character scalar giving the database location.
#'   This is typically a filesystem path (e.g., \code{"~/mydb"}).
#'
#' @return A \code{lancedb_connection} object.
#'
#' @export
lancedb_connect <- function(uri) {
  .backend_call("connect", uri = uri)
}

#' Open an existing LanceDB table
#'
#' Opens an existing table within a LanceDB database connection.
#' This function does not create the table if it does not exist.
#'
#' @param con A \code{lancedb_connection} object.
#' @param name Character scalar giving the table name.
#'
#' @return A \code{lancedb_table} object.
#'
#' @export
lancedb_open_table <- function(con, name) {
  .backend_call("open_table", con = con, name = name)
}


#' Create a LanceDB table
#'
#' Creates a new LanceDB table from the provided data.
#' This function dispatches to the active backend implementation.
#'
#' @param con A \code{lancedb_connection} object.
#' @param name Character scalar giving the table name.
#' @param data Data used to create the table. Typically a
#'   \code{data.frame} or Arrow-compatible object.
#' @param mode Creation mode. Either \code{"overwrite"} (replace
#'   any existing table) or \code{"create"} (fail if the table exists).
#'
#' @return A \code{lancedb_table} object.
#'
#' @export
lancedb_create_table <- function(con, name, data, mode = c("overwrite", "create")) {
  mode <- match.arg(mode)
  .backend_call("create_table", con = con, name = name, data = data, mode = mode)
}


#' Reticulate backend: connect to a LanceDB database
#'
#' Backend-specific implementation of \code{lancedb_connect()} using
#' the Python LanceDB client.
#'
#' @param uri Character scalar giving the database location.
#'
#' @return A \code{lancedb_connection} object.
#'
#' @keywords internal
.rt_connect <- function(uri) {
  stopifnot(is.character(uri), length(uri) == 1)

  lancedb <- .py_lancedb()
  conn <- lancedb$connect(uri)

  structure(list(uri = uri, .py = conn), class = "lancedb_connection")
}


#' Reticulate backend: open an existing table
#'
#' Backend-specific implementation of \code{lancedb_open_table()}
#' using the Python LanceDB client.
#'
#' @param con A \code{lancedb_connection} object.
#' @param name Character scalar giving the table name.
#'
#' @return A \code{lancedb_table} object.
#'
#' @keywords internal
.rt_open_table <- function(con, name) {
  stopifnot(inherits(con, "lancedb_connection"))
  stopifnot(is.character(name), length(name) == 1)

  tbl <- con$.py$open_table(name)
  structure(list(name = name, con = con, .py = tbl), class = "lancedb_table")
}


# internal: convert an R data.frame to list-of-row dictionaries for Python
.df_to_row_dicts <- function(df) {
  stopifnot(is.data.frame(df))

  # Ensure we don't accidentally drop list columns
  rows <- lapply(seq_len(nrow(df)), function(i) {
    as.list(df[i, , drop = FALSE])
  })

  # Unwrap 1-length atomic vectors produced by data.frame slicing
  rows <- lapply(rows, function(r) {
    lapply(r, function(v) {
      if (is.list(v)) return(v)  # list column stays as list
      if (length(v) == 1) return(v[[1]])
      v
    })
  })

  rows
}


#' Reticulate backend: create a table
#'
#' Backend-specific implementation of \code{lancedb_create_table()}
#' using the Python LanceDB client.
#'
#' @param con A \code{lancedb_connection} object.
#' @param name Character scalar giving the table name.
#' @param data Data used to create the table.
#' @param mode Creation mode (\code{"overwrite"} or \code{"create"}).
#'
#' @return A \code{lancedb_table} object.
#'
#' @keywords internal
.rt_create_table <- function(con, name, data, mode) {
  stopifnot(inherits(con, "lancedb_connection"))
  stopifnot(is.character(name), length(name) == 1)

  payload <- if (is.data.frame(data) && "vector" %in% names(data)) {
    .df_to_pyarrow_table(data, vector_column = "vector")
  } else {
    data
  }
  tbl <- con$.py$create_table(name, payload, mode = mode)
  structure(list(name = name, con = con, .py = tbl), class = "lancedb_table")
}


