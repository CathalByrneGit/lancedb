# Tests for enhanced indexing features
# Requires compiled Rust backend.

skip_if_not_installed("arrow")

skip_if_no_backend <- function() {
  tryCatch({
    rust_connect(tempdir())
    TRUE
  }, error = function(e) {
    skip("LanceDB Rust backend not available (not compiled)")
  })
}

# Helper to create a test table with text and vector columns
make_indexing_table <- function(suffix) {
  db_path <- file.path(tempdir(), paste0("lancedb_idx_", Sys.getpid(), "_", suffix))
  con <- lancedb_connect(db_path)
  df <- data.frame(
    id = 1:50,
    category = rep(c("A", "B", "C", "D", "E"), each = 10),
    score = as.double(1:50),
    text = paste("Document", 1:50, "about", rep(c("science", "art", "math", "history", "music"), each = 10)),
    stringsAsFactors = FALSE
  )
  tbl <- lancedb_create_table(con, paste0("idx_", suffix), df, mode = "overwrite")
  list(con = con, tbl = tbl, df = df)
}

# ---------------------------------------------------------------------------
# Scalar index with parameters
# ---------------------------------------------------------------------------

test_that("lancedb_create_index creates btree index", {
  skip_if_no_backend()
  env <- make_indexing_table("btree")

  expect_no_error(
    lancedb_create_index(env$tbl, "category", index_type = "btree")
  )

  indices <- lancedb_list_indices(env$tbl)
  expect_true(nrow(indices) >= 1)
})

test_that("lancedb_create_index creates bitmap index", {
  skip_if_no_backend()
  env <- make_indexing_table("bitmap")

  expect_no_error(
    lancedb_create_index(env$tbl, "category", index_type = "bitmap")
  )

  indices <- lancedb_list_indices(env$tbl)
  expect_true(nrow(indices) >= 1)
})

# ---------------------------------------------------------------------------
# FTS index with configuration
# ---------------------------------------------------------------------------

test_that("lancedb_create_index creates FTS index with defaults", {
  skip_if_no_backend()
  env <- make_indexing_table("fts_default")

  expect_no_error(
    lancedb_create_index(env$tbl, "text", index_type = "fts")
  )

  indices <- lancedb_list_indices(env$tbl)
  expect_true(nrow(indices) >= 1)
})

test_that("lancedb_create_index creates FTS index with config params", {
  skip_if_no_backend()
  env <- make_indexing_table("fts_config")

  expect_no_error(
    lancedb_create_index(env$tbl, "text", index_type = "fts",
                          with_position = TRUE,
                          stem = TRUE,
                          lower_case = TRUE)
  )
})

test_that("lancedb_create_index creates FTS index with language", {
  skip_if_no_backend()
  env <- make_indexing_table("fts_lang")

  expect_no_error(
    lancedb_create_index(env$tbl, "text", index_type = "fts",
                          language = "English",
                          stem = TRUE,
                          remove_stop_words = TRUE)
  )
})

# ---------------------------------------------------------------------------
# Full-text search queries
# ---------------------------------------------------------------------------

test_that("lancedb_fts_search creates a lazy FTS query", {
  skip_if_no_backend()
  env <- make_indexing_table("fts_query")

  # Create FTS index first
  lancedb_create_index(env$tbl, "text", index_type = "fts")

  # Create a lazy FTS query
  lazy <- lancedb_fts_search(env$tbl, "science")
  expect_s3_class(lazy, "lancedb_lazy")
  expect_equal(lazy$mode, "fts")
  expect_equal(lazy$search_config$fts_query, "science")
})

test_that("lancedb_fts_search executes and returns results", {
  skip_if_no_backend()
  env <- make_indexing_table("fts_exec")

  # Create FTS index
  lancedb_create_index(env$tbl, "text", index_type = "fts")

  # Execute FTS search
  results <- lancedb_fts_search(env$tbl, "science") |>
    slice_head.lancedb_lazy(n = 20) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) > 0)
  # All results should contain "science" in text
  expect_true(all(grepl("science", results$text, ignore.case = TRUE)))
})

test_that("lancedb_fts_search with column restriction works", {
  skip_if_no_backend()
  env <- make_indexing_table("fts_cols")

  lancedb_create_index(env$tbl, "text", index_type = "fts")

  lazy <- lancedb_fts_search(env$tbl, "art", columns = "text")
  expect_equal(lazy$search_config$fts_columns, "text")

  results <- lazy |>
    slice_head.lancedb_lazy(n = 20) |>
    collect.lancedb_lazy()

  expect_true(nrow(results) > 0)
})

test_that("lancedb_fts_search with select and filter", {
  skip_if_no_backend()
  env <- make_indexing_table("fts_combo")

  lancedb_create_index(env$tbl, "text", index_type = "fts")

  results <- lancedb_fts_search(env$tbl, "math") |>
    select.lancedb_lazy(id, text, score) |>
    slice_head.lancedb_lazy(n = 5) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) <= 5)
  expect_true("id" %in% names(results))
  expect_true("text" %in% names(results))
})

# ---------------------------------------------------------------------------
# Vector search with query-time parameters
# ---------------------------------------------------------------------------

test_that("lancedb_search stores search config parameters", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_idx_", Sys.getpid(), "_search_cfg"))
  con <- lancedb_connect(db_path)

  # Create table with vector column
  n <- 100
  df <- data.frame(
    id = 1:n,
    stringsAsFactors = FALSE
  )
  df$vec <- I(lapply(1:n, function(i) rnorm(4)))
  tbl <- lancedb_create_table(con, "vectors", df, mode = "overwrite")

  # Create search with parameters
  lazy <- lancedb_search(tbl, rnorm(4),
                          nprobes = 50L,
                          refine_factor = 10L,
                          ef = 64L,
                          column = "vec",
                          distance_type = "cosine")

  expect_equal(lazy$search_config$nprobes, 50L)
  expect_equal(lazy$search_config$refine_factor, 10L)
  expect_equal(lazy$search_config$ef, 64L)
  expect_equal(lazy$search_config$column, "vec")
  expect_equal(lazy$search_config$distance_type, "cosine")
})

test_that("lancedb_search with bypass_vector_index works", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_idx_", Sys.getpid(), "_bypass"))
  con <- lancedb_connect(db_path)

  n <- 50
  df <- data.frame(id = 1:n, stringsAsFactors = FALSE)
  df$vec <- I(lapply(1:n, function(i) rnorm(4)))
  tbl <- lancedb_create_table(con, "bypass_test", df, mode = "overwrite")

  # Search with flat scan (no index)
  results <- lancedb_search(tbl, rnorm(4),
                             bypass_vector_index = TRUE) |>
    slice_head.lancedb_lazy(n = 5) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) > 0)
  expect_true("_distance" %in% names(results))
})

# ---------------------------------------------------------------------------
# Lazy query immutability with search_config
# ---------------------------------------------------------------------------

test_that("append_op preserves search_config", {
  skip_if_no_backend()
  env <- make_indexing_table("immutable")

  lazy1 <- lancedb_fts_search(env$tbl, "history")
  lazy2 <- append_op(lazy1, list(op = "limit", n = 10L))

  # Original unchanged

  expect_equal(length(lazy1$ops), 0)
  # New has the op
  expect_equal(length(lazy2$ops), 1)
  # Both share same search_config
  expect_equal(lazy1$search_config$fts_query, "history")
  expect_equal(lazy2$search_config$fts_query, "history")
})

# ---------------------------------------------------------------------------
# show_query prints FTS info
# ---------------------------------------------------------------------------

test_that("show_query handles FTS mode", {
  skip_if_no_backend()
  env <- make_indexing_table("show_fts")

  lazy <- lancedb_fts_search(env$tbl, "music", columns = "text")

  output <- capture.output(show_query(lazy))
  expect_true(any(grepl("Full-Text Search", output)))
  expect_true(any(grepl("music", output)))
})

test_that("show_query handles search with config", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_idx_", Sys.getpid(), "_show_search"))
  con <- lancedb_connect(db_path)
  df <- data.frame(id = 1:10, stringsAsFactors = FALSE)
  df$vec <- I(lapply(1:10, function(i) rnorm(4)))
  tbl <- lancedb_create_table(con, "show_search", df, mode = "overwrite")

  lazy <- lancedb_search(tbl, rnorm(4), nprobes = 50L)

  output <- capture.output(show_query(lazy))
  expect_true(any(grepl("Vector Search", output)))
  expect_true(any(grepl("nprobes", output)))
})

# ---------------------------------------------------------------------------
# jsonlite_to_json helper
# ---------------------------------------------------------------------------

test_that("jsonlite_to_json serializes config correctly", {
  result <- jsonlite_to_json(list())
  expect_equal(result, "{}")

  result <- jsonlite_to_json(list(metric = "cosine", num_partitions = 256))
  expect_true(grepl('"metric"', result))
  expect_true(grepl('"cosine"', result))
  expect_true(grepl('"num_partitions"', result))
  expect_true(grepl("256", result))
})

test_that("jsonlite_to_json handles booleans", {
  result <- jsonlite_to_json(list(with_position = TRUE, stem = FALSE))
  expect_true(grepl("true", result))
  expect_true(grepl("false", result))
})
