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

#' List Tables in a Database
#'
#' Returns the names of all tables in the connected database.
#'
#' @param con A `lancedb_connection` object.
#'
#' @return A character vector of table names.
#'
#' @examples
#' \dontrun{
#' con <- lancedb_connect("/tmp/my_lancedb")
#' lancedb_list_tables(con)
#' }
#'
#' @export
lancedb_list_tables <- function(con) {
  stopifnot(inherits(con, "lancedb_connection"))
  rust_table_names(con$ptr)
}

#' Drop a Table from the Database
#'
#' Permanently removes a table and its data from the database.
#'
#' @param con A `lancedb_connection` object.
#' @param name Character string. Name of the table to drop.
#'
#' @return The connection (invisibly).
#'
#' @examples
#' \dontrun{
#' con <- lancedb_connect("/tmp/my_lancedb")
#' lancedb_drop_table(con, "old_table")
#' }
#'
#' @export
lancedb_drop_table <- function(con, name) {
  stopifnot(inherits(con, "lancedb_connection"))
  stopifnot(is.character(name), length(name) == 1)
  rust_drop_table(con$ptr, name)
  invisible(con)
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
