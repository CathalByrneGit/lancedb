#' Create a Vector Search Query
#'
#' Begins a lazy vector similarity search on a LanceDB table. The query is not
#' executed until [collect()] is called. Use dplyr verbs (`filter`, `select`,
#' `slice_head`) to refine the query.
#'
#' @param table A `lancedb_table` object.
#' @param query_vector A numeric vector to use as the search query.
#' @param nprobes Integer. Number of IVF partitions to search (default: 20).
#'   Higher values increase recall but reduce speed.
#' @param refine_factor Integer. Multiplier for refining PQ results. Higher
#'   values increase accuracy after quantization. Default `NULL` (no refine).
#' @param ef Integer. Number of candidates for HNSW search. Higher values
#'   increase recall. Only used with HNSW indices. Default `NULL`.
#' @param column Character. Name of the vector column to search. Required if
#'   the table has multiple vector columns. Default `NULL` (auto-detect).
#' @param distance_type Character. Override distance metric at query time.
#'   One of `"l2"`, `"cosine"`, `"dot"`. Default `NULL` (use index metric).
#' @param bypass_vector_index Logical. If `TRUE`, perform exhaustive flat search
#'   instead of using the index. Default `FALSE`.
#' @param distance_range Numeric vector of length 2 `c(lower, upper)`. Only
#'   return results within this distance range. Default `NULL` (no range filter).
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
#' # Basic vector search
#' results <- lancedb_search(tbl, runif(128)) %>%
#'   filter(category == "science") %>%
#'   select(title, category) %>%
#'   slice_head(n = 10) %>%
#'   collect()
#'
#' # Tuned vector search with more probes and refine
#' results <- lancedb_search(tbl, runif(128),
#'                           nprobes = 50,
#'                           refine_factor = 10) %>%
#'   slice_head(n = 20) %>%
#'   collect()
#'
#' # Search a specific vector column
#' results <- lancedb_search(tbl, runif(128), column = "embedding") %>%
#'   collect()
#' }
#'
#' @export
lancedb_search <- function(table, query_vector,
                            nprobes = NULL,
                            refine_factor = NULL,
                            ef = NULL,
                            column = NULL,
                            distance_type = NULL,
                            bypass_vector_index = FALSE,
                            distance_range = NULL) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.numeric(query_vector), length(query_vector) > 0)

  # Build search config from parameters
  search_config <- list()
  if (!is.null(nprobes)) search_config$nprobes <- as.integer(nprobes)
  if (!is.null(refine_factor)) search_config$refine_factor <- as.integer(refine_factor)
  if (!is.null(ef)) search_config$ef <- as.integer(ef)
  if (!is.null(column)) search_config$column <- as.character(column)
  if (!is.null(distance_type)) search_config$distance_type <- as.character(distance_type)
  if (isTRUE(bypass_vector_index)) search_config$bypass_vector_index <- TRUE

  # Distance range filter: numeric vector of length 2 (lower, upper) or named list

  if (!is.null(distance_range)) {
    stopifnot(is.numeric(distance_range), length(distance_range) == 2)
    search_config$distance_range_lower <- distance_range[1]
    search_config$distance_range_upper <- distance_range[2]
  }

  new_lancedb_lazy(
    table = table,
    mode = "search",
    qvec = as.double(query_vector),
    ops = list(),
    search_config = search_config
  )
}

#' Create a Full-Text Search Query
#'
#' Begins a lazy full-text search (BM25-based) on a LanceDB table. Requires
#' an FTS index to have been created on one or more text columns. The query
#' is not executed until [collect()] is called.
#'
#' @param table A `lancedb_table` object.
#' @param query_text Character string. The search query text. Can be a phrase
#'   (e.g., `"old man and the sea"`) or individual terms (e.g., `"old man sea"`).
#' @param columns Character vector. Optional column names to search in. If
#'   `NULL` (default), all FTS-indexed columns are searched.
#'
#' @return A `lancedb_lazy` object.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' con <- lancedb_connect("/tmp/my_lancedb")
#' tbl <- lancedb_open_table(con, "documents")
#'
#' # Create FTS index first
#' lancedb_create_index(tbl, "content", index_type = "fts",
#'                      with_position = TRUE, stem = TRUE)
#'
#' # Full-text search
#' results <- lancedb_fts_search(tbl, "machine learning") %>%
#'   select(title, content) %>%
#'   slice_head(n = 10) %>%
#'   collect()
#'
#' # Search specific columns
#' results <- lancedb_fts_search(tbl, "neural networks",
#'                                columns = c("title", "abstract")) %>%
#'   collect()
#' }
#'
#' @export
lancedb_fts_search <- function(table, query_text, columns = NULL) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.character(query_text), length(query_text) == 1, nchar(query_text) > 0)

  search_config <- list(fts_query = query_text)
  if (!is.null(columns)) {
    stopifnot(is.character(columns))
    search_config$fts_columns <- columns
  }

  new_lancedb_lazy(
    table = table,
    mode = "fts",
    qvec = NULL,
    ops = list(),
    search_config = search_config
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
    ops = list(),
    search_config = list()
  )
}

#' Create a Hybrid Search Query
#'
#' Begins a lazy hybrid search that combines vector similarity and full-text
#' search (BM25) on a LanceDB table. Results are merged using the specified
#' normalization method (reciprocal rank fusion by default). Requires both a
#' vector index and an FTS index on the table.
#'
#' @param table A `lancedb_table` object.
#' @param query_vector A numeric vector to use as the vector search component.
#' @param query_text Character string. The full-text search query.
#' @param fts_columns Character vector. Optional FTS column names to search.
#'   If `NULL`, all FTS-indexed columns are searched.
#' @param nprobes Integer. Number of IVF partitions for vector search.
#' @param refine_factor Integer. PQ refinement multiplier.
#' @param ef Integer. HNSW search candidates.
#' @param column Character. Name of the vector column. Default `NULL`.
#' @param distance_type Character. Override distance metric (`"l2"`, `"cosine"`,
#'   `"dot"`). Default `NULL`.
#' @param norm Character. Score normalization method for merging: `"rank"`
#'   (reciprocal rank fusion, default) or `"score"` (score-based).
#'
#' @return A `lancedb_lazy` object in `"hybrid"` mode.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' # Hybrid search combining vector and text
#' results <- lancedb_hybrid_search(tbl, runif(128), "machine learning") %>%
#'   select(title, content) %>%
#'   slice_head(n = 10) %>%
#'   collect()
#'
#' # With score-based normalization
#' results <- lancedb_hybrid_search(tbl, runif(128), "neural networks",
#'                                   norm = "score",
#'                                   column = "embedding",
#'                                   distance_type = "cosine") %>%
#'   collect()
#' }
#'
#' @seealso [lancedb_search()], [lancedb_fts_search()]
#' @export
lancedb_hybrid_search <- function(table, query_vector, query_text,
                                   fts_columns = NULL,
                                   nprobes = NULL,
                                   refine_factor = NULL,
                                   ef = NULL,
                                   column = NULL,
                                   distance_type = NULL,
                                   norm = "rank") {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.numeric(query_vector), length(query_vector) > 0)
  stopifnot(is.character(query_text), length(query_text) == 1, nchar(query_text) > 0)

  search_config <- list(fts_query = query_text)
  if (!is.null(fts_columns)) {
    stopifnot(is.character(fts_columns))
    search_config$fts_columns <- fts_columns
  }
  if (!is.null(nprobes)) search_config$nprobes <- as.integer(nprobes)
  if (!is.null(refine_factor)) search_config$refine_factor <- as.integer(refine_factor)
  if (!is.null(ef)) search_config$ef <- as.integer(ef)
  if (!is.null(column)) search_config$column <- as.character(column)
  if (!is.null(distance_type)) search_config$distance_type <- as.character(distance_type)
  if (!is.null(norm)) search_config$norm <- match.arg(norm, c("rank", "score"))

  new_lancedb_lazy(
    table = table,
    mode = "hybrid",
    qvec = as.double(query_vector),
    ops = list(),
    search_config = search_config
  )
}

# ---------------------------------------------------------------------------
# Query modifier verbs (piping-friendly)
# ---------------------------------------------------------------------------

#' Skip Rows in Query Results
#'
#' Adds an offset to the query, skipping the first `n` rows. Useful for
#' pagination when combined with [slice_head()].
#'
#' @param .data A `lancedb_lazy` object.
#' @param n Integer. Number of rows to skip.
#'
#' @return A new `lancedb_lazy` object with the offset appended.
#'
#' @examples
#' \dontrun{
#' # Page 2 of results (rows 11-20)
#' lancedb_scan(tbl) %>%
#'   lancedb_offset(10) %>%
#'   slice_head(n = 10) %>%
#'   collect()
#' }
#'
#' @export
lancedb_offset <- function(.data, n) {
  stopifnot(inherits(.data, "lancedb_lazy"))
  stopifnot(is.numeric(n), length(n) == 1, n >= 0)
  append_op(.data, list(op = "offset", n = as.integer(n)))
}

#' Enable Post-Filtering
#'
#' When enabled, filter predicates are applied after the search rather than
#' before. This ensures the search always uses the full index but may return
#' fewer results than the requested limit after filtering.
#'
#' @param .data A `lancedb_lazy` object.
#'
#' @return A new `lancedb_lazy` object with post-filtering enabled.
#'
#' @examples
#' \dontrun{
#' # Post-filter: search first, then apply filter
#' lancedb_search(tbl, runif(128)) %>%
#'   filter(category == "science") %>%
#'   lancedb_postfilter() %>%
#'   slice_head(n = 10) %>%
#'   collect()
#' }
#'
#' @export
lancedb_postfilter <- function(.data) {
  stopifnot(inherits(.data, "lancedb_lazy"))
  append_op(.data, list(op = "postfilter"))
}

#' Enable Fast Search Mode
#'
#' Enables approximate (faster) search that may skip some results for speed.
#' Useful for large-scale searches where approximate results are acceptable.
#'
#' @param .data A `lancedb_lazy` object.
#'
#' @return A new `lancedb_lazy` object with fast search enabled.
#'
#' @examples
#' \dontrun{
#' lancedb_search(tbl, runif(128)) %>%
#'   lancedb_fast_search() %>%
#'   slice_head(n = 10) %>%
#'   collect()
#' }
#'
#' @export
lancedb_fast_search <- function(.data) {
  stopifnot(inherits(.data, "lancedb_lazy"))
  append_op(.data, list(op = "fast_search"))
}

#' Include Row IDs in Results
#'
#' Adds the internal LanceDB row ID (`_rowid`) column to the query results.
#' This is useful for identifying specific rows for updates or deletes.
#'
#' @param .data A `lancedb_lazy` object.
#'
#' @return A new `lancedb_lazy` object that will include row IDs.
#'
#' @examples
#' \dontrun{
#' lancedb_scan(tbl) %>%
#'   lancedb_with_row_id() %>%
#'   slice_head(n = 10) %>%
#'   collect()
#' }
#'
#' @export
lancedb_with_row_id <- function(.data) {
  stopifnot(inherits(.data, "lancedb_lazy"))
  append_op(.data, list(op = "with_row_id"))
}

#' Select Computed Columns via SQL Expressions
#'
#' Adds a dynamic column projection to the lazy query using SQL expressions
#' evaluated by the DataFusion engine. Unlike [dplyr::select()], which only
#' picks existing columns by name, `lancedb_select_exprs()` can compute new
#' derived columns using SQL expressions.
#'
#' @param .data A `lancedb_lazy` object.
#' @param ... Named character strings. Each name becomes an output column name;
#'   each value is a SQL expression evaluated against the table data.
#'
#' @return A new `lancedb_lazy` object with the dynamic projection appended.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' con <- lancedb_connect("/tmp/my_lancedb")
#' tbl <- lancedb_open_table(con, "my_table")
#'
#' # Compute derived columns
#' results <- lancedb_scan(tbl) %>%
#'   lancedb_select_exprs(
#'     score_pct  = "score * 100",
#'     name_upper = "upper(name)",
#'     id_str     = "CAST(id AS VARCHAR)"
#'   ) %>%
#'   slice_head(n = 10) %>%
#'   collect()
#'
#' # After vector search, normalise the distance column
#' results <- lancedb_search(tbl, runif(128)) %>%
#'   lancedb_select_exprs(
#'     name      = "name",
#'     norm_dist = "_distance / 100"
#'   ) %>%
#'   slice_head(n = 5) %>%
#'   collect()
#' }
#'
#' @seealso [dplyr::select()], [lancedb_scan()], [lancedb_search()]
#' @export
lancedb_select_exprs <- function(.data, ...) {
  stopifnot(inherits(.data, "lancedb_lazy"))

  exprs <- list(...)

  if (length(exprs) == 0) {
    rlang::abort("lancedb_select_exprs() requires at least one named SQL expression.")
  }

  nms <- names(exprs)
  if (is.null(nms) || any(nms == "")) {
    rlang::abort(paste0(
      "All arguments to lancedb_select_exprs() must be named. ",
      "Use lancedb_select_exprs(col_name = 'sql_expr', ...)."
    ))
  }

  not_str <- !vapply(exprs, is.character, logical(1))
  if (any(not_str)) {
    rlang::abort("All SQL expression values in lancedb_select_exprs() must be character strings.")
  }

  append_op(.data, list(op = "select_expr", exprs = exprs))
}

#' Create a new lancedb_lazy object (internal constructor)
#' @noRd
new_lancedb_lazy <- function(table, mode, qvec, ops, search_config = list()) {
  structure(
    list(
      table = table,
      mode = mode,
      qvec = qvec,
      ops = ops,
      search_config = search_config
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
    ops = c(.data$ops, list(op)),
    search_config = .data$search_config
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
  if (x$mode %in% c("fts", "hybrid")) {
    fts_q <- x$search_config$fts_query
    if (!is.null(fts_q)) cat("  FTS query:", fts_q, "\n")
  }
  # Show search config if any non-default params
  sc <- x$search_config
  sc$fts_query <- NULL
  sc$fts_columns <- NULL
  if (length(sc) > 0) {
    cat("  Search config:")
    for (nm in names(sc)) {
      cat(" ", nm, "=", sc[[nm]])
    }
    cat("\n")
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
        "select_expr" = paste0(
          "select_exprs: ",
          paste(names(op$exprs), "=", unlist(op$exprs), collapse = ", ")
        ),
        "limit" = paste0("limit: ", op$n),
        "offset" = paste0("offset: ", op$n),
        "postfilter" = "postfilter",
        "fast_search" = "fast_search",
        "with_row_id" = "with_row_id",
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
    sc <- x$search_config
    if (length(sc) > 0) {
      for (nm in names(sc)) {
        cat("  ", nm, ":", sc[[nm]], "\n")
      }
    }
  } else if (x$mode == "fts") {
    cat("Type: Full-Text Search\n")
    cat("Query:", x$search_config$fts_query, "\n")
    if (!is.null(x$search_config$fts_columns)) {
      cat("Columns:", paste(x$search_config$fts_columns, collapse = ", "), "\n")
    }
  } else if (x$mode == "hybrid") {
    cat("Type: Hybrid Search (Vector + FTS)\n")
    cat("Query vector:", length(x$qvec), "dimensions\n")
    cat("FTS query:", x$search_config$fts_query, "\n")
    if (!is.null(x$search_config$fts_columns)) {
      cat("FTS columns:", paste(x$search_config$fts_columns, collapse = ", "), "\n")
    }
    sc <- x$search_config
    sc$fts_query <- NULL
    sc$fts_columns <- NULL
    if (length(sc) > 0) {
      for (nm in names(sc)) {
        cat("  ", nm, ":", sc[[nm]], "\n")
      }
    }
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
        "select_expr" = {
          cat("  SELECT (computed):\n")
          for (nm in names(op$exprs)) {
            cat("    ", nm, " = ", op$exprs[[nm]], "\n", sep = "")
          }
        },
        "limit" = cat("  LIMIT", op$n, "\n"),
        "offset" = cat("  OFFSET", op$n, "\n"),
        "postfilter" = cat("  POSTFILTER\n"),
        "fast_search" = cat("  FAST_SEARCH\n"),
        "with_row_id" = cat("  WITH_ROW_ID\n")
      )
    }
  }
  cat("========================\n")
  invisible(x)
}
