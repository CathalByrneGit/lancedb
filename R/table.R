#' Open an Existing LanceDB Table
#'
#' Opens a table by name from an existing LanceDB connection.
#'
#' @param con A `lancedb_connection` object.
#' @param name Character string. Name of the table to open.
#'
#' @return A `lancedb_table` object.
#'
#' @examples
#' \dontrun{
#' con <- lancedb_connect("/tmp/my_lancedb")
#' tbl <- lancedb_open_table(con, "my_table")
#' }
#'
#' @export
lancedb_open_table <- function(con, name) {
  stopifnot(inherits(con, "lancedb_connection"))
  stopifnot(is.character(name), length(name) == 1)

  ptr <- rust_open_table(con$ptr, name)

  structure(
    list(
      ptr = ptr,
      name = name,
      connection = con
    ),
    class = "lancedb_table"
  )
}

#' Create a New LanceDB Table
#'
#' Creates a table in the database from a data.frame or Arrow Table.
#'
#' @param con A `lancedb_connection` object.
#' @param name Character string. Name for the new table.
#' @param data A `data.frame` or `arrow::Table` to use as initial data.
#' @param mode Character string. One of `"create"` (error if exists) or
#'   `"overwrite"` (replace if exists). Default is `"create"`.
#'
#' @return A `lancedb_table` object.
#'
#' @examples
#' \dontrun{
#' con <- lancedb_connect("/tmp/my_lancedb")
#' df <- data.frame(id = 1:3, text = c("a", "b", "c"))
#' tbl <- lancedb_create_table(con, "my_table", df)
#' }
#'
#' @export
lancedb_create_table <- function(con, name, data, mode = c("create", "overwrite")) {

  stopifnot(inherits(con, "lancedb_connection"))
  stopifnot(is.character(name), length(name) == 1)
  mode <- match.arg(mode)

  ipc_bytes <- data_to_ipc(data)
  ptr <- rust_create_table(con$ptr, name, ipc_bytes, mode)

  structure(
    list(
      ptr = ptr,
      name = name,
      connection = con
    ),
    class = "lancedb_table"
  )
}

#' Print a LanceDB Table
#' @param x A `lancedb_table` object.
#' @param ... Ignored.
#' @export
print.lancedb_table <- function(x, ...) {
  cat("<lancedb_table>\n")
  cat("  Name:", x$name, "\n")
  nrows <- tryCatch(
    rust_count_rows(x$ptr, NULL),
    error = function(e) NA
  )
  cat("  Rows:", if (is.na(nrows)) "unknown" else format(nrows, big.mark = ","), "\n")
  schema_json <- tryCatch(
    rust_table_schema_json(x$ptr),
    error = function(e) "[]"
  )
  fields <- jsonlite_parse(schema_json)
  if (length(fields) > 0) {
    cat("  Columns:\n")
    for (f in fields) {
      cat("    -", f$name, paste0("(", f$type, ")"), "\n")
    }
  }
  invisible(x)
}

#' Count Rows in a Table
#'
#' @param table A `lancedb_table` object.
#' @param filter Optional filter expression string.
#'
#' @return Integer count of rows.
#' @export
lancedb_count_rows <- function(table, filter = NULL) {
  stopifnot(inherits(table, "lancedb_table"))
  rust_count_rows(table$ptr, filter)
}

#' Add Data to a Table
#'
#' @param table A `lancedb_table` object.
#' @param data A `data.frame` or `arrow::Table`.
#' @param mode One of `"append"` or `"overwrite"`.
#'
#' @return The table (invisibly).
#' @export
lancedb_add <- function(table, data, mode = c("append", "overwrite")) {
  stopifnot(inherits(table, "lancedb_table"))
  mode <- match.arg(mode)
  ipc_bytes <- data_to_ipc(data)
  rust_add_data(table$ptr, ipc_bytes, mode)
  invisible(table)
}

#' Delete Rows from a Table
#'
#' @param table A `lancedb_table` object.
#' @param predicate SQL predicate string for rows to delete.
#'
#' @return The table (invisibly).
#' @export
lancedb_delete <- function(table, predicate) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.character(predicate), length(predicate) == 1)
  rust_delete_rows(table$ptr, predicate)
  invisible(table)
}
