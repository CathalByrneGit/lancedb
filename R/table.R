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

# ---------------------------------------------------------------------------
# Basic table operations
# ---------------------------------------------------------------------------

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

#' Get Table Schema
#'
#' Returns the schema of a LanceDB table as a data.frame describing each
#' column's name, type, and nullability.
#'
#' @param table A `lancedb_table` object.
#'
#' @return A data.frame with columns `name`, `type`, and `nullable`.
#'
#' @examples
#' \dontrun{
#' lancedb_schema(tbl)
#' }
#'
#' @export
lancedb_schema <- function(table) {
  stopifnot(inherits(table, "lancedb_table"))
  schema_json <- rust_table_schema_json(table$ptr)
  fields <- jsonlite_parse(schema_json)
  data.frame(
    name = vapply(fields, function(f) f$name, character(1)),
    type = vapply(fields, function(f) f$type, character(1)),
    nullable = vapply(fields, function(f) isTRUE(f$nullable), logical(1)),
    stringsAsFactors = FALSE
  )
}

#' Get Current Table Version
#'
#' @param table A `lancedb_table` object.
#'
#' @return An integer version number.
#'
#' @export
lancedb_version <- function(table) {
  stopifnot(inherits(table, "lancedb_table"))
  rust_table_version(table$ptr)
}

#' Preview First N Rows
#'
#' Returns the first `n` rows from a table as a data.frame without building
#' a full lazy query.
#'
#' @param table A `lancedb_table` object.
#' @param n Number of rows to return. Default 5.
#'
#' @return A data.frame.
#'
#' @export
lancedb_head <- function(table, n = 5L) {
  stopifnot(inherits(table, "lancedb_table"))
  n <- as.integer(n)
  lancedb_scan(table) |>
    head.lancedb_lazy(n) |>
    collect.lancedb_lazy()
}

#' Convert Entire Table to Arrow
#'
#' Reads the full table contents as an Arrow Table.
#'
#' @param table A `lancedb_table` object.
#'
#' @return An `arrow::Table`.
#'
#' @export
lancedb_to_arrow <- function(table) {
  stopifnot(inherits(table, "lancedb_table"))
  collect.lancedb_lazy(lancedb_scan(table), as = "arrow")
}

# ---------------------------------------------------------------------------
# Data modification
# ---------------------------------------------------------------------------

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

#' Update Rows in a Table
#'
#' Updates column values for rows matching an optional filter predicate.
#' Values are specified as SQL expressions.
#'
#' @param table A `lancedb_table` object.
#' @param values A named list mapping column names to SQL expression strings.
#'   For literal values, wrap strings in single quotes: `list(status = "'active'")`.
#'   For computed values, use expressions: `list(score = "score + 1")`.
#' @param where Optional SQL WHERE predicate. If `NULL`, all rows are updated.
#'
#' @return The table (invisibly).
#'
#' @examples
#' \dontrun{
#' # Set all scores to 0
#' lancedb_update(tbl, values = list(score = "0"))
#'
#' # Update matching rows with a computed value
#' lancedb_update(tbl, values = list(score = "score * 2"), where = "category = 'A'")
#'
#' # Set a string literal
#' lancedb_update(tbl, values = list(status = "'active'"), where = "id > 10")
#' }
#'
#' @export
lancedb_update <- function(table, values, where = NULL) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.list(values), length(values) > 0, !is.null(names(values)))

  # Encode values as JSON object of column -> expression
  vals_json <- paste0(
    "{",
    paste0(
      '"', names(values), '":"',
      vapply(values, function(v) gsub('"', '\\\\"', as.character(v)), character(1)),
      '"',
      collapse = ","
    ),
    "}"
  )

  where_str <- if (is.null(where)) "" else as.character(where)
  rust_update_rows(table$ptr, where_str, vals_json)
  invisible(table)
}

#' Merge Insert (Upsert) Data into a Table
#'
#' Performs a merge insert operation, joining new data against existing rows
#' on the specified key column(s). Supports upsert, insert-if-not-exists,
#' and replace-range patterns.
#'
#' @param table A `lancedb_table` object.
#' @param data A `data.frame` or `arrow::Table` with the new/updated data.
#' @param on Character vector of column name(s) to join on.
#' @param when_matched_update_all Logical. If `TRUE`, update all columns when
#'   the key matches an existing row. Default `TRUE`.
#' @param when_not_matched_insert_all Logical. If `TRUE`, insert new rows when
#'   no match is found in the target. Default `TRUE`.
#' @param when_not_matched_by_source_delete Logical. If `TRUE`, delete target
#'   rows that have no match in the source data. Default `FALSE`.
#'
#' @return The table (invisibly).
#'
#' @examples
#' \dontrun{
#' # Upsert: update existing rows, insert new ones
#' new_data <- data.frame(id = c(1, 2, 999), name = c("updated", "updated", "new"))
#' lancedb_merge_insert(tbl, new_data, on = "id")
#'
#' # Insert only if not exists (no updates)
#' lancedb_merge_insert(tbl, new_data, on = "id",
#'                      when_matched_update_all = FALSE)
#'
#' # Replace range: delete target rows not in source
#' lancedb_merge_insert(tbl, new_data, on = "id",
#'                      when_not_matched_by_source_delete = TRUE)
#' }
#'
#' @export
lancedb_merge_insert <- function(table, data, on,
                                  when_matched_update_all = TRUE,
                                  when_not_matched_insert_all = TRUE,
                                  when_not_matched_by_source_delete = FALSE) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.character(on), length(on) >= 1)

  ipc_bytes <- data_to_ipc(data)
  rust_merge_insert(
    table$ptr,
    on,
    ipc_bytes,
    when_matched_update_all,
    when_not_matched_insert_all,
    when_not_matched_by_source_delete
  )
  invisible(table)
}

# ---------------------------------------------------------------------------
# Indexing
# ---------------------------------------------------------------------------

#' Create an Index on a Table
#'
#' Creates a scalar, vector, or full-text search index on the specified
#' column(s). Additional parameters can be passed via `...` for fine-tuning.
#'
#' @param table A `lancedb_table` object.
#' @param columns Character vector of column name(s) to index.
#' @param index_type Index type. One of:
#'   - `"auto"` (default): automatically select the best index type
#'   - Scalar: `"btree"`, `"bitmap"`, `"label_list"`
#'   - Full-text: `"fts"`
#'   - Vector: `"ivf_pq"`, `"ivf_flat"`, `"ivf_hnsw_pq"`, `"ivf_hnsw_sq"`
#' @param replace Logical. Replace existing index on the same column(s).
#'   Default `TRUE`.
#' @param metric Distance metric for vector indices. One of `"l2"` (default),
#'   `"cosine"`, `"dot"`. Ignored for non-vector index types.
#' @param ... Additional index-specific configuration parameters:
#'
#'   **Vector indices** (ivf_pq, ivf_flat, ivf_hnsw_pq, ivf_hnsw_sq):
#'   \describe{
#'     \item{`num_partitions`}{Number of IVF partitions (default: auto).}
#'     \item{`num_sub_vectors`}{Number of PQ sub-vectors (ivf_pq, ivf_hnsw_pq).}
#'     \item{`num_bits`}{Number of PQ bits (ivf_pq, ivf_hnsw_pq).}
#'     \item{`sample_rate`}{IVF training sample rate (default: 256).}
#'     \item{`max_iterations`}{IVF training max iterations (default: 50).}
#'     \item{`m`}{HNSW num edges per node (ivf_hnsw_pq, ivf_hnsw_sq; default: 20).}
#'     \item{`ef_construction`}{HNSW construction search depth (default: 300).}
#'   }
#'
#'   **Full-text search** (fts):
#'   \describe{
#'     \item{`with_position`}{Logical. Enable phrase queries (default: FALSE).}
#'     \item{`base_tokenizer`}{Tokenizer type: `"simple"` (default) or `"ngram"`.}
#'     \item{`language`}{Language for stemming/stop words, e.g. `"English"`.}
#'     \item{`stem`}{Logical. Enable stemming (default: FALSE).}
#'     \item{`remove_stop_words`}{Logical. Remove stop words (default: FALSE).}
#'     \item{`ascii_folding`}{Logical. Normalize accents (default: FALSE).}
#'     \item{`max_token_length`}{Integer. Max token length filter.}
#'     \item{`lower_case`}{Logical. Lowercase tokens (default: TRUE).}
#'   }
#'
#' @return The table (invisibly).
#'
#' @examples
#' \dontrun{
#' # Scalar index for fast filtering
#' lancedb_create_index(tbl, "category", index_type = "btree")
#'
#' # Vector index with custom parameters
#' lancedb_create_index(tbl, "embedding", index_type = "ivf_pq",
#'                      metric = "cosine", num_partitions = 256,
#'                      num_sub_vectors = 16)
#'
#' # HNSW vector index
#' lancedb_create_index(tbl, "embedding", index_type = "ivf_hnsw_sq",
#'                      metric = "cosine", m = 20, ef_construction = 150)
#'
#' # Full-text search with stemming and phrase support
#' lancedb_create_index(tbl, "text", index_type = "fts",
#'                      with_position = TRUE, language = "English",
#'                      stem = TRUE, remove_stop_words = TRUE)
#'
#' # FTS with n-gram tokenizer for substring matching
#' lancedb_create_index(tbl, "text", index_type = "fts",
#'                      base_tokenizer = "ngram")
#' }
#'
#' @export
lancedb_create_index <- function(table, columns, index_type = "auto",
                                  replace = TRUE, metric = "l2", ...) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.character(columns), length(columns) >= 1)
  stopifnot(is.character(index_type), length(index_type) == 1)
  stopifnot(is.character(metric), length(metric) == 1)

  # Build config JSON from metric + extra arguments
  config <- list(metric = metric, ...)
  config_json <- jsonlite_to_json(config)

  rust_create_index(table$ptr, columns, index_type, replace, config_json)
  invisible(table)
}

#' List Indices on a Table
#'
#' @param table A `lancedb_table` object.
#'
#' @return A data.frame with columns `name`, `index_type`, and `columns`.
#'
#' @export
lancedb_list_indices <- function(table) {
  stopifnot(inherits(table, "lancedb_table"))
  json_str <- rust_list_indices(table$ptr)
  indices <- jsonlite_parse(json_str)
  if (length(indices) == 0) {
    return(data.frame(
      name = character(0),
      index_type = character(0),
      columns = character(0),
      stringsAsFactors = FALSE
    ))
  }
  data.frame(
    name = vapply(indices, function(i) i$name, character(1)),
    index_type = vapply(indices, function(i) i$index_type, character(1)),
    columns = vapply(indices, function(i) paste(unlist(i$columns), collapse = ", "), character(1)),
    stringsAsFactors = FALSE
  )
}

#' Drop an Index from a Table
#'
#' @param table A `lancedb_table` object.
#' @param name Character string. Name of the index to drop.
#'
#' @return The table (invisibly).
#'
#' @export
lancedb_drop_index <- function(table, name) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.character(name), length(name) == 1)
  rust_drop_index(table$ptr, name)
  invisible(table)
}

# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------

#' Add Columns to a Table
#'
#' Adds new columns computed from SQL expressions referencing existing columns.
#'
#' @param table A `lancedb_table` object.
#' @param transforms A named list mapping new column names to SQL expressions.
#'
#' @return The table (invisibly).
#'
#' @examples
#' \dontrun{
#' # Add a column computed from existing ones
#' lancedb_add_columns(tbl, list(score_doubled = "score * 2"))
#'
#' # Add a constant column
#' lancedb_add_columns(tbl, list(status = "'pending'"))
#' }
#'
#' @export
lancedb_add_columns <- function(table, transforms) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.list(transforms), length(transforms) > 0, !is.null(names(transforms)))

  json <- paste0(
    "{",
    paste0(
      '"', names(transforms), '":"',
      vapply(transforms, function(v) gsub('"', '\\\\"', as.character(v)), character(1)),
      '"',
      collapse = ","
    ),
    "}"
  )

  rust_add_columns(table$ptr, json)
  invisible(table)
}

#' Alter Columns in a Table
#'
#' Modifies column properties such as name or nullability.
#'
#' @param table A `lancedb_table` object.
#' @param alterations A list of alteration specs. Each element is a named list
#'   with `path` (required) and optional `rename` and/or `nullable`.
#'
#' @return The table (invisibly).
#'
#' @examples
#' \dontrun{
#' # Rename a column
#' lancedb_alter_columns(tbl, list(list(path = "old_name", rename = "new_name")))
#'
#' # Make a column nullable
#' lancedb_alter_columns(tbl, list(list(path = "score", nullable = TRUE)))
#' }
#'
#' @export
lancedb_alter_columns <- function(table, alterations) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.list(alterations), length(alterations) > 0)

  # Build JSON array manually
  parts <- vapply(alterations, function(a) {
    stopifnot(!is.null(a$path))
    json_parts <- paste0('"path":"', a$path, '"')
    if (!is.null(a$rename)) {
      json_parts <- paste0(json_parts, ',"rename":"', a$rename, '"')
    }
    if (!is.null(a$nullable)) {
      json_parts <- paste0(json_parts, ',"nullable":', tolower(as.character(a$nullable)))
    }
    paste0("{", json_parts, "}")
  }, character(1))

  json <- paste0("[", paste(parts, collapse = ","), "]")
  rust_alter_columns(table$ptr, json)
  invisible(table)
}

#' Drop Columns from a Table
#'
#' Permanently removes columns from a table. This cannot be undone.
#'
#' @param table A `lancedb_table` object.
#' @param columns Character vector of column names to drop.
#'
#' @return The table (invisibly).
#'
#' @examples
#' \dontrun{
#' lancedb_drop_columns(tbl, c("temp_col", "debug_info"))
#' }
#'
#' @export
lancedb_drop_columns <- function(table, columns) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.character(columns), length(columns) >= 1)
  rust_drop_columns(table$ptr, columns)
  invisible(table)
}

# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------

#' List All Versions of a Table
#'
#' Returns the version history of a table, including version number,
#' timestamp, and metadata.
#'
#' @param table A `lancedb_table` object.
#'
#' @return A data.frame with columns `version`, `timestamp`, and `metadata`.
#'
#' @export
lancedb_list_versions <- function(table) {
  stopifnot(inherits(table, "lancedb_table"))
  json_str <- rust_list_versions(table$ptr)
  versions <- jsonlite_parse(json_str)
  if (length(versions) == 0) {
    return(data.frame(
      version = integer(0),
      timestamp = character(0),
      stringsAsFactors = FALSE
    ))
  }
  data.frame(
    version = vapply(versions, function(v) as.integer(v$version), integer(1)),
    timestamp = vapply(versions, function(v) as.character(v$timestamp), character(1)),
    stringsAsFactors = FALSE
  )
}

#' Checkout a Table Version
#'
#' Switches the table to a read-only view of a specific past version.
#' Use [lancedb_restore()] to make this version the current one, or
#' [lancedb_checkout_latest()] to return to the latest.
#'
#' @param table A `lancedb_table` object.
#' @param version Integer version number to checkout.
#'
#' @return The table (invisibly).
#'
#' @examples
#' \dontrun{
#' lancedb_checkout(tbl, version = 1)
#' old_data <- lancedb_head(tbl)
#' lancedb_checkout_latest(tbl)
#' }
#'
#' @export
lancedb_checkout <- function(table, version) {
  stopifnot(inherits(table, "lancedb_table"))
  stopifnot(is.numeric(version), length(version) == 1)
  rust_checkout(table$ptr, as.integer(version))
  invisible(table)
}

#' Checkout the Latest Table Version
#'
#' Returns the table to its latest version after a previous
#' [lancedb_checkout()] call.
#'
#' @param table A `lancedb_table` object.
#'
#' @return The table (invisibly).
#'
#' @export
lancedb_checkout_latest <- function(table) {
  stopifnot(inherits(table, "lancedb_table"))
  rust_checkout_latest(table$ptr)
  invisible(table)
}

#' Restore a Checked-Out Version
#'
#' Creates a new version of the table from the currently checked-out version.
#' Must be called after [lancedb_checkout()].
#'
#' @param table A `lancedb_table` object.
#'
#' @return The table (invisibly).
#'
#' @export
lancedb_restore <- function(table) {
  stopifnot(inherits(table, "lancedb_table"))
  rust_restore(table$ptr)
  invisible(table)
}

# ---------------------------------------------------------------------------
# Optimization
# ---------------------------------------------------------------------------

#' Compact Table Files
#'
#' Merges small files in the table for better read performance.
#'
#' @param table A `lancedb_table` object.
#'
#' @return Optimization stats as a character string (invisibly).
#'
#' @export
lancedb_compact_files <- function(table) {
  stopifnot(inherits(table, "lancedb_table"))
  stats <- rust_compact_files(table$ptr)
  invisible(stats)
}

#' Clean Up Old Table Versions
#'
#' Prunes old versions that are older than the specified number of days.
#'
#' @param table A `lancedb_table` object.
#' @param older_than_days Integer. Remove versions older than this many days.
#'   Default 7.
#'
#' @return The table (invisibly).
#'
#' @export
lancedb_cleanup_old_versions <- function(table, older_than_days = 7L) {
  stopifnot(inherits(table, "lancedb_table"))
  rust_cleanup_old_versions(table$ptr, as.integer(older_than_days))
  invisible(table)
}
