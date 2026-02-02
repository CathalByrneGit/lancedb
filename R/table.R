#' Add data to a LanceDB table
#'
#' Appends rows to an existing LanceDB table.
#' This is a backend-agnostic wrapper that dispatches to the active
#' backend implementation (currently \code{"reticulate"}).
#'
#' @param tbl A \code{lancedb_table} object.
#' @param data Data to add. Typically a \code{data.frame} or Arrow-compatible
#'   object that can be converted by the backend.
#'
#' @return Invisibly returns \code{NULL}.
#'
#' @export
tbl_add <- function(tbl, data) {
  if (!inherits(tbl, "lancedb_table")) {
    stop("`tbl` must be a lancedb_table.", call. = FALSE)
  }
  .backend_call("add", tbl = tbl, data = data)
}

#' Search a LanceDB table
#'
#' Performs a vector similarity search against a LanceDB table using a
#' numeric query vector. This function provides a stable, backend-agnostic
#' R interface and dispatches to the active backend implementation.
#'
#' @param tbl A \code{lancedb_table} object.
#' @param query Numeric vector representing the query embedding.
#' @param k Integer. Number of nearest neighbors to return.
#' @param filter Optional character string specifying a filter expression
#'   (passed through to the backend).
#' @param select Optional character vector of column names to return.
#' @param as Output format. Either \code{"data.frame"} (default) or
#'   \code{"arrow"} to return an Arrow Table.
#' @param vector_column Character scalar giving the name of the vector
#'   column in the table. Defaults to \code{"vector"}.
#'
#' @return
#' If \code{as = "data.frame"}, returns a data frame of results.
#' If \code{as = "arrow"}, returns an \code{arrow::Table}.
#'
#' @export
tbl_search <- function(tbl, query, k = 10, filter = NULL, select = NULL,
                       as = c("data.frame", "arrow"),
                       vector_column = "vector") {
  if (!inherits(tbl, "lancedb_table")) {
    stop("`tbl` must be a lancedb_table.", call. = FALSE)
  }
  if (!is.numeric(query) || length(query) < 1L) {
    stop("`query` must be a non-empty numeric vector.", call. = FALSE)
  }
  if (!is.numeric(k) || length(k) != 1L || is.na(k) || k < 1) {
    stop("`k` must be a single positive integer.", call. = FALSE)
  }
  if (!is.character(vector_column) || length(vector_column) != 1L || !nzchar(vector_column)) {
    stop("`vector_column` must be a non-empty character scalar.", call. = FALSE)
  }

  as <- match.arg(as)

  .backend_call(
    "search",
    tbl = tbl,
    query = query,
    k = k,
    filter = filter,
    select = select,
    as = as,
    vector_column = vector_column
  )
}



#' Reticulate backend: add rows to table
#'
#' Backend-specific implementation for adding data using the
#' Python LanceDB client.
#'
#' @param tbl A \code{lancedb_table} object.
#' @param data Data to add.
#'
#' @return Invisibly returns \code{NULL}.
#'
#' @keywords internal
.rt_add <- function(tbl, data) {
  stopifnot(inherits(tbl, "lancedb_table"))

  payload <- if (is.data.frame(data) && "vector" %in% names(data)) {
    .df_to_pyarrow_table(data, vector_column = "vector")
  } else {
    data
  }
  invisible(tbl$.py$add(payload))
}




#' Try calling a Python method if it exists
#'
#' Internal helper that attempts to call a named method on a Python
#' object. Returns \code{NULL} if the method does not exist.
#'
#' @param obj A Python object.
#' @param method Character scalar giving the method name.
#' @param ... Arguments passed to the method.
#'
#' @return The result of the method call, or \code{NULL}.
#'
#' @keywords internal
.py_try_method <- function(obj, method, ...) {
  if (reticulate::py_has_attr(obj, method)) {
    return(obj[[method]](...))
  }
  NULL
}


#' Build a LanceDB search query (reticulate backend)
#'
#' Internal helper that constructs a Python LanceDB query object using
#' the reticulate backend. This function attempts to explicitly specify
#' the vector column name, which is required by some versions of the
#' Python LanceDB API.
#'
#' @param tbl A \code{lancedb_table} object.
#' @param query Numeric query vector.
#' @param k Integer number of nearest neighbors.
#' @param filter Optional filter expression.
#' @param select Optional column selection.
#' @param vector_column Character scalar giving the vector column name.
#'
#' @return A Python LanceDB query object.
#'
#' @keywords internal
.rt_build_query <- function(tbl, query, k, filter, select, vector_column) {
  stopifnot(inherits(tbl, "lancedb_table"))
  stopifnot(is.numeric(query), length(query) > 0)

  q <- tryCatch(
    tbl$.py$search(as.numeric(query), vector_column_name = vector_column),
    error = function(e) NULL
  )
  if (is.null(q)) {
    q <- tbl$.py$search(as.numeric(query))
  }

  q <- q$limit(as.integer(k))
  if (!is.null(filter)) q <- q$where(as.character(filter))
  if (!is.null(select)) q <- q$select(select)
  q
}




#' Reticulate backend: search implementation
#'
#' Backend-specific implementation of \code{tbl_search()} using the
#' Python LanceDB client. Supports returning results as either a
#' data frame or an Arrow Table.
#'
#' @param tbl A \code{lancedb_table} object.
#' @param query Numeric query vector.
#' @param k Integer number of nearest neighbors.
#' @param filter Optional filter expression.
#' @param select Optional column selection.
#' @param as Output format (\code{"data.frame"} or \code{"arrow"}).
#' @param vector_column Character scalar giving the vector column name.
#'
#' @return A data frame or \code{arrow::Table}.
#'
#' @keywords internal
.rt_search <- function(tbl, query, k, filter, select, as, vector_column) {
  q <- .rt_build_query(tbl, query, k, filter, select, vector_column)

  if (as == "data.frame") {
    res <- q$to_list()
    return(tryCatch(as.data.frame(res), error = function(e) res))
  }

  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop(
      "Package 'arrow' is required for as='arrow'. Install it first.",
      call. = FALSE
    )
  }

  # Try common Arrow conversion methods (API varies by LanceDB version)
  py_tbl <-
    .py_try_method(q, "to_arrow") %||%
    .py_try_method(q, "to_arrow_table") %||%
    .py_try_method(q, "to_table")

  if (is.null(py_tbl)) {
    stop(
      "Could not convert results to Arrow via Python ",
      "(no to_arrow/to_table found).",
      call. = FALSE
    )
  }

  # Fast path: let reticulate convert pyarrow -> arrow
  out <- tryCatch(
    reticulate::py_to_r(py_tbl),
    error = function(e) NULL
  )
  if (!is.null(out)) return(out)

  # Fallback: serialize Arrow IPC bytes and read in R
  pa <- tryCatch(
    reticulate::import("pyarrow", delay_load = FALSE),
    error = function(e) NULL
  )
  if (is.null(pa)) {
    stop(
      "Python package 'pyarrow' is required for Arrow output but is unavailable.",
      call. = FALSE
    )
  }

  sink <- pa$BufferOutputStream()
  writer <- pa$ipc$new_stream(sink, py_tbl$schema)
  writer$write_table(py_tbl)
  writer$close()

  buf <- sink$getvalue()
  raw <- reticulate::py_to_r(buf$to_pybytes())

  arrow::read_ipc_stream(raw)
}



#' Null-coalescing helper
#'
#' Internal utility that returns the left-hand side if it is not
#' \code{NULL}, otherwise returns the right-hand side.
#'
#' @param x First value.
#' @param y Fallback value.
#'
#' @return \code{x} if not \code{NULL}, otherwise \code{y}.
#'
#' @keywords internal
`%||%` <- function(x, y) if (!is.null(x)) x else y
