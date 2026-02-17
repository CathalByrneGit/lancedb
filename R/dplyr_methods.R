#' @importFrom dplyr filter select slice_head arrange collect pull
#' @importFrom rlang enquos exprs quo_get_expr is_string abort

# ---------------------------------------------------------------------------
# filter.lancedb_lazy
# ---------------------------------------------------------------------------

#' Filter a LanceDB Lazy Query
#'
#' Adds a filter (WHERE) clause to the lazy query plan. Predicates can be
#' R expressions or string expressions.
#'
#' @param .data A `lancedb_lazy` object.
#' @param ... Filter predicates. Can be R expressions (e.g., `age > 30`) or
#'   string expressions (e.g., `"age > 30"`).
#' @param .preserve Ignored (present for dplyr compatibility).
#'
#' @return A new `lancedb_lazy` object with the filter appended.
#'
#' @examples
#' \dontrun{
#' # R expression filter
#' lancedb_scan(tbl) %>% filter(age > 30, name == "Alice")
#'
#' # String filter (passed through directly)
#' lancedb_scan(tbl) %>% filter("age > 30 AND name = 'Alice'")
#' }
filter.lancedb_lazy <- function(.data, ..., .preserve = FALSE) {
  dots <- rlang::enquos(...)

  if (length(dots) == 0) {
    return(.data)
  }

  # Each quosure becomes a filter expression; multiple are ANDed
  exprs_str <- vapply(dots, function(q) {
    expr <- rlang::quo_get_expr(q)

    # If the user passed a string literal, use it directly
    if (is.character(expr)) {
      return(expr)
    }

    # Translate R expression to LanceDB filter string
    translate_expr(expr)
  }, character(1))

  # Combine multiple predicates with AND
  combined <- paste(exprs_str, collapse = " AND ")

  append_op(.data, list(op = "where", expr = combined))
}

# ---------------------------------------------------------------------------
# select.lancedb_lazy
# ---------------------------------------------------------------------------

#' Select Columns from a LanceDB Lazy Query
#'
#' Specifies which columns to return from the query.
#'
#' @param .data A `lancedb_lazy` object.
#' @param ... Column names (unquoted or character strings).
#'
#' @return A new `lancedb_lazy` object with the selection appended.
#'
#' @examples
#' \dontrun{
#' lancedb_scan(tbl) %>% select(name, age, score)
#' }
select.lancedb_lazy <- function(.data, ...) {
  exprs <- rlang::exprs(...)

  if (length(exprs) == 0) {
    rlang::abort("select() requires at least one column.")
  }

  cols <- vapply(exprs, function(e) {
    if (is.character(e)) {
      return(e)
    }
    if (is.symbol(e)) {
      return(as.character(e))
    }
    rlang::abort(paste0(
      "select() currently supports bare column names only, got: ",
      deparse(e)
    ))
  }, character(1))

  append_op(.data, list(op = "select", cols = cols))
}

# ---------------------------------------------------------------------------
# slice_head.lancedb_lazy
# ---------------------------------------------------------------------------

#' Limit Results from a LanceDB Lazy Query
#'
#' Equivalent to SQL `LIMIT`. Returns the first `n` rows.
#'
#' @param .data A `lancedb_lazy` object.
#' @param n Integer. Number of rows to return.
#' @param prop Not supported (present for dplyr compatibility).
#' @param by Not supported (present for dplyr compatibility).
#'
#' @return A new `lancedb_lazy` object with the limit appended.
#'
#' @examples
#' \dontrun{
#' lancedb_scan(tbl) %>% slice_head(n = 10)
#' }
slice_head.lancedb_lazy <- function(.data, n, prop = NULL, by = NULL) {
  if (!is.null(prop)) {
    rlang::abort("slice_head() with `prop` is not supported for LanceDB queries.")
  }
  if (!is.null(by)) {
    rlang::abort("slice_head() with `by` is not supported for LanceDB queries.")
  }
  stopifnot(is.numeric(n), length(n) == 1, n > 0)

  append_op(.data, list(op = "limit", n = as.integer(n)))
}

# ---------------------------------------------------------------------------
# head.lancedb_lazy
# ---------------------------------------------------------------------------

#' @export
head.lancedb_lazy <- function(x, n = 6L, ...) {
  append_op(x, list(op = "limit", n = as.integer(n)))
}

# ---------------------------------------------------------------------------
# arrange.lancedb_lazy
# ---------------------------------------------------------------------------

#' Arrange (Sort) a LanceDB Lazy Query
#'
#' Adds an ORDER BY clause. Note: ordering support depends on the query mode
#' and backend capabilities.
#'
#' @param .data A `lancedb_lazy` object.
#' @param ... Column expressions to sort by. Use `desc(col)` for descending.
#'
#' @return A new `lancedb_lazy` object with the order appended.
#'
#' @examples
#' \dontrun{
#' lancedb_scan(tbl) %>% arrange(age, desc(name))
#' }
arrange.lancedb_lazy <- function(.data, ...) {
  rlang::abort(
    paste0(
      "arrange() is not currently supported by the LanceDB backend.\n",
      "Vector search results are automatically ordered by distance.\n",
      "For scan queries, retrieve results with collect() and sort in R:\n",
      "  collect() |> dplyr::arrange(...)"
    ),
    class = "lancedb_unsupported_op"
  )
}

# ---------------------------------------------------------------------------
# collect.lancedb_lazy
# ---------------------------------------------------------------------------

#' Execute a LanceDB Lazy Query
#'
#' Terminal method that executes the accumulated query plan against the
#' LanceDB backend and returns the results.
#'
#' @param x A `lancedb_lazy` object.
#' @param ... Additional arguments.
#' @param as Output format: `"data.frame"` (default) or `"arrow"`.
#'
#' @return A `data.frame` or `arrow::Table` depending on `as`.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' results <- lancedb_scan(tbl) %>%
#'   filter(age > 30) %>%
#'   select(name, age) %>%
#'   slice_head(n = 10) %>%
#'   collect()
#'
#' # Return as Arrow Table
#' arrow_results <- lancedb_scan(tbl) %>%
#'   filter(age > 30) %>%
#'   collect(as = "arrow")
#' }
collect.lancedb_lazy <- function(x, ..., as = c("data.frame", "arrow")) {
  as <- match.arg(as)

  # Serialize ops to JSON
  ops_json <- ops_to_json(x$ops)

  # Serialize search config to JSON
  search_config_json <- jsonlite_to_json(x$search_config)

  # Execute via Rust
  ipc_bytes <- rust_execute_query(
    x$table$ptr,
    x$mode,
    x$qvec,
    ops_json,
    search_config_json
  )

  # Handle empty result
  if (length(ipc_bytes) == 0) {
    if (as == "arrow") {
      return(arrow::arrow_table())
    }
    return(data.frame())
  }

  # Decode IPC bytes via Arrow
  reader <- arrow::RecordBatchStreamReader$create(ipc_bytes)
  arrow_table <- reader$read_table()

  if (as == "arrow") {
    return(arrow_table)
  }

  as.data.frame(arrow_table)
}

# ---------------------------------------------------------------------------
# pull.lancedb_lazy
# ---------------------------------------------------------------------------

#' Pull a Single Column from a LanceDB Lazy Query
#'
#' Convenience terminal that collects the query results and extracts a single
#' column as a vector. Equivalent to `collect(.data) |> dplyr::pull(var)`.
#'
#' @param .data A `lancedb_lazy` object.
#' @param var Column to extract. Can be an unquoted column name, a string, or
#'   an integer position (negative counts from the right).
#' @param name Not supported (present for dplyr compatibility).
#' @param ... Ignored.
#'
#' @return A vector containing the values of the requested column.
#'
#' @examples
#' \dontrun{
#' # Pull by name
#' lancedb_scan(tbl) %>% pull(name)
#'
#' # Pull last column
#' lancedb_scan(tbl) %>% pull(-1)
#' }
#'
#' @export
pull.lancedb_lazy <- function(.data, var = -1, name = NULL, ...) {
  result <- collect.lancedb_lazy(.data)
  if (is.numeric(var)) {
    if (var < 0) var <- ncol(result) + var + 1
    return(result[[var]])
  }
  var_name <- as.character(rlang::ensym(var))
  result[[var_name]]
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#' Convert ops list to JSON string
#' @noRd
ops_to_json <- function(ops) {
  # Manual JSON serialisation to avoid hard dep on jsonlite
  if (length(ops) == 0) return("[]")

  parts <- vapply(ops, function(op) {
    switch(op$op,
      "where" = {
        sprintf('{"op":"where","expr":"%s"}', gsub('"', '\\\\"', op$expr))
      },
      "select" = {
        cols_json <- paste0('"', op$cols, '"', collapse = ",")
        sprintf('{"op":"select","cols":[%s]}', cols_json)
      },
      "limit" = {
        sprintf('{"op":"limit","n":%d}', op$n)
      },
      "offset" = {
        sprintf('{"op":"offset","n":%d}', op$n)
      },
      "postfilter" = '{"op":"postfilter"}',
      "fast_search" = '{"op":"fast_search"}',
      "with_row_id" = '{"op":"with_row_id"}',
      "select_expr" = {
        # Serialize as {"op":"select_expr","exprs":{"col":"expr",...}}
        pairs <- op$exprs
        kv <- paste0(
          '"', names(pairs), '":"',
          gsub('"', '\\\\"', unlist(pairs, use.names = FALSE)),
          '"',
          collapse = ","
        )
        sprintf('{"op":"select_expr","exprs":{%s}}', kv)
      },
      sprintf('{"op":"%s"}', op$op)
    )
  }, character(1))

  paste0("[", paste(parts, collapse = ","), "]")
}
