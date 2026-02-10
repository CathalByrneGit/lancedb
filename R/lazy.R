#' Create a Vector Search Query
#'
#' Begins a lazy vector similarity search on a LanceDB table. The query is not
#' executed until [collect()] is called. Use dplyr verbs (`filter`, `select`,
#' `slice_head`) to refine the query.
#'
#' @param table A `lancedb_table` object.
#' @param query_vector A numeric vector to use as the search query.
#'
#' @return A `lancedb_lazy` object.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' con <- lancedb_connect("/tmp/my_lancedb")
#' tbl <- lancedb_open_table(con, "embeddings")
#'
#' results <- lancedb_search(tbl, runif(128)) %>%
#'   filter(category == "science") %>%
#'   select(title, category) %>%
#'   slice_head(n = 10) %>%
#'   collect()
#' }
#'
#' @export
lancedb_search <- function(table, query_vector) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.numeric(query_vector), length(query_vector) > 0)

  new_lancedb_lazy(
    table = table,
    mode = "search",
    qvec = as.double(query_vector),
    ops = list()
  )
}

#' Create a Table Scan Query
#'
#' Begins a lazy full table scan on a LanceDB table. The query is not executed
#' until [collect()] is called. Use dplyr verbs (`filter`, `select`,
#' `slice_head`) to refine the scan.
#'
#' @param table A `lancedb_table` object.
#'
#' @return A `lancedb_lazy` object.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' con <- lancedb_connect("/tmp/my_lancedb")
#' tbl <- lancedb_open_table(con, "my_table")
#'
#' results <- lancedb_scan(tbl) %>%
#'   filter(age > 30) %>%
#'   select(name, age) %>%
#'   slice_head(n = 100) %>%
#'   collect()
#' }
#'
#' @export
lancedb_scan <- function(table) {
  stopifnot(inherits(table, "lancedb_table"))

  new_lancedb_lazy(
    table = table,
    mode = "scan",
    qvec = NULL,
    ops = list()
  )
}

#' Create a new lancedb_lazy object (internal constructor)
#' @noRd
new_lancedb_lazy <- function(table, mode, qvec, ops) {
  structure(
    list(
      table = table,
      mode = mode,
      qvec = qvec,
      ops = ops
    ),
    class = "lancedb_lazy"
  )
}

#' Append an operation to a lazy query (immutably)
#' @noRd
append_op <- function(.data, op) {
  new_lancedb_lazy(
    table = .data$table,
    mode = .data$mode,
    qvec = .data$qvec,
    ops = c(.data$ops, list(op))
  )
}

#' Print a Lazy Query
#' @param x A `lancedb_lazy` object.
#' @param ... Ignored.
#' @export
print.lancedb_lazy <- function(x, ...) {
  cat("<lancedb_lazy>\n")
  cat("  Table:", x$table$name, "\n")
  cat("  Mode:", x$mode, "\n")
  if (!is.null(x$qvec)) {
    cat("  Query vector: [", length(x$qvec), "dimensions ]\n")
  }
  if (length(x$ops) == 0) {
    cat("  Ops: (none)\n")
  } else {
    cat("  Ops:\n")
    for (i in seq_along(x$ops)) {
      op <- x$ops[[i]]
      desc <- switch(op$op,
        "where" = paste0("filter: ", op$expr),
        "select" = paste0("select: ", paste(op$cols, collapse = ", ")),
        "limit" = paste0("limit: ", op$n),
        "order_by" = paste0("arrange: ", paste(op$cols, collapse = ", ")),
        paste0(op$op, ": ...")
      )
      cat("    ", i, ". ", desc, "\n", sep = "")
    }
  }
  cat("
Call collect() to execute this query.\n")
  invisible(x)
}

#' Show the Query Plan
#'
#' Displays a human-readable representation of the lazy query plan, similar
#' to dbplyr's `show_query()`.
#'
#' @param x A `lancedb_lazy` object.
#'
#' @return The query object (invisibly).
#'
#' @examples
#' \dontrun{
#' lancedb_scan(tbl) %>%
#'   filter(age > 30) %>%
#'   select(name, age) %>%
#'   show_query()
#' }
#'
#' @export
show_query <- function(x) {
  stopifnot(inherits(x, "lancedb_lazy"))

  cat("== LanceDB Query Plan ==\n")
  cat("Table:", x$table$name, "\n")

  if (x$mode == "search") {
    cat("Type: Vector Search\n")
    cat("Query vector:", length(x$qvec), "dimensions\n")
  } else {
    cat("Type: Table Scan\n")
  }

  if (length(x$ops) > 0) {
    cat("\nOperations (applied in order):\n")
    for (i in seq_along(x$ops)) {
      op <- x$ops[[i]]
      switch(op$op,
        "where" = cat("  WHERE", op$expr, "\n"),
        "select" = cat("  SELECT", paste(op$cols, collapse = ", "), "\n"),
        "limit" = cat("  LIMIT", op$n, "\n"),
        "order_by" = {
          cols_str <- paste0(
            op$cols,
            ifelse(op$desc, " DESC", " ASC"),
            collapse = ", "
          )
          cat("  ORDER BY", cols_str, "\n")
        }
      )
    }
  }
  cat("========================\n")
  invisible(x)
}
