#' Connect to a LanceDB Database
#'
#' Opens a connection to a LanceDB database at the given URI. The URI can be
#' a local directory path (embedded mode) or a cloud storage path.
#'
#' @param uri Character string. Path to the database directory (e.g.,
#'   `"/tmp/my_lancedb"`) or a cloud storage URI.
#'
#' @return A `lancedb_connection` object.
#'
#' @examples
#' \dontrun{
#' con <- lancedb_connect("/tmp/my_lancedb")
#' print(con)
#' }
#'
#' @export
lancedb_connect <- function(uri) {
  stopifnot(is.character(uri), length(uri) == 1)
  uri <- normalizePath(uri, mustWork = FALSE)

  ptr <- rust_connect(uri)

  structure(
    list(
      ptr = ptr,
      uri = uri
    ),
    class = "lancedb_connection"
  )
}

#' Print a LanceDB Connection
#' @param x A `lancedb_connection` object.
#' @param ... Ignored.
#' @export
print.lancedb_connection <- function(x, ...) {
  cat("<lancedb_connection>\n")
  cat("  URI:", x$uri, "\n")
  tables <- tryCatch(
    rust_table_names(x$ptr),
    error = function(e) character(0)
  )
  cat("  Tables:", if (length(tables) == 0) "(none)" else paste(tables, collapse = ", "), "\n")
  invisible(x)
}
