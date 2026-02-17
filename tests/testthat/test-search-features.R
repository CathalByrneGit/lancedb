# Tests for enhanced search features: hybrid search, offset, postfilter,
# fast_search, with_row_id, distance_range.
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
make_search_table <- function(suffix) {
  db_path <- file.path(tempdir(), paste0("lancedb_search_", Sys.getpid(), "_", suffix))
  con <- lancedb_connect(db_path)
  n <- 50
  df <- data.frame(
    id = 1:n,
    category = rep(c("A", "B", "C", "D", "E"), each = 10),
    score = as.double(1:n),
    text = paste("Document", 1:n, "about",
                 rep(c("science", "art", "math", "history", "music"), each = 10)),
    stringsAsFactors = FALSE
  )
  df$vec <- I(lapply(1:n, function(i) as.double(c(i, i + 1, i + 2, i + 3))))
  tbl <- lancedb_create_table(con, paste0("search_", suffix), df, mode = "overwrite")
  list(con = con, tbl = tbl, df = df)
}

# ---------------------------------------------------------------------------
# Hybrid search
# ---------------------------------------------------------------------------

test_that("lancedb_hybrid_search creates a lazy hybrid query", {
  skip_if_no_backend()
  env <- make_search_table("hybrid_lazy")

  lazy <- lancedb_hybrid_search(env$tbl, c(1, 2, 3, 4), "science")
  expect_s3_class(lazy, "lancedb_lazy")
  expect_equal(lazy$mode, "hybrid")
  expect_equal(lazy$search_config$fts_query, "science")
  expect_equal(length(lazy$qvec), 4)
})

test_that("lancedb_hybrid_search stores all config params", {
  skip_if_no_backend()
  env <- make_search_table("hybrid_cfg")

  lazy <- lancedb_hybrid_search(env$tbl, c(1, 2, 3, 4), "science",
                                 fts_columns = "text",
                                 nprobes = 50L,
                                 column = "vec",
                                 distance_type = "cosine",
                                 norm = "score")

  expect_equal(lazy$search_config$fts_query, "science")
  expect_equal(lazy$search_config$fts_columns, "text")
  expect_equal(lazy$search_config$nprobes, 50L)
  expect_equal(lazy$search_config$column, "vec")
  expect_equal(lazy$search_config$distance_type, "cosine")
  expect_equal(lazy$search_config$norm, "score")
})

test_that("lancedb_hybrid_search executes with vector + FTS", {
  skip_if_no_backend()
  env <- make_search_table("hybrid_exec")

  # Create FTS index
  lancedb_create_index(env$tbl, "text", index_type = "fts")

  # Execute hybrid search
  results <- lancedb_hybrid_search(env$tbl, c(1, 2, 3, 4), "science",
                                    column = "vec") |>
    slice_head.lancedb_lazy(n = 10) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) > 0)
})

test_that("lancedb_hybrid_search with select works", {
  skip_if_no_backend()
  env <- make_search_table("hybrid_sel")

  lancedb_create_index(env$tbl, "text", index_type = "fts")

  results <- lancedb_hybrid_search(env$tbl, c(1, 2, 3, 4), "art",
                                    column = "vec") |>
    select.lancedb_lazy(id, text) |>
    slice_head.lancedb_lazy(n = 5) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true("id" %in% names(results))
  expect_true("text" %in% names(results))
})

test_that("lancedb_hybrid_search validates inputs", {
  skip_if_no_backend()
  env <- make_search_table("hybrid_val")

  expect_error(lancedb_hybrid_search(env$tbl, "not_numeric", "science"))
  expect_error(lancedb_hybrid_search(env$tbl, c(1, 2), ""))
  expect_error(lancedb_hybrid_search(env$tbl, c(1, 2), 42))
})

# ---------------------------------------------------------------------------
# Offset
# ---------------------------------------------------------------------------

test_that("lancedb_offset appends offset op", {
  skip_if_no_backend()
  env <- make_search_table("offset")

  lazy <- lancedb_scan(env$tbl) |>
    lancedb_offset(10)

  expect_equal(length(lazy$ops), 1)
  expect_equal(lazy$ops[[1]]$op, "offset")
  expect_equal(lazy$ops[[1]]$n, 10L)
})

test_that("lancedb_offset executes correctly", {
  skip_if_no_backend()
  env <- make_search_table("offset_exec")

  # Get all rows first
  all_rows <- lancedb_scan(env$tbl) |>
    slice_head.lancedb_lazy(n = 50) |>
    collect.lancedb_lazy()

  # Get rows with offset
  offset_rows <- lancedb_scan(env$tbl) |>
    lancedb_offset(10) |>
    slice_head.lancedb_lazy(n = 5) |>
    collect.lancedb_lazy()

  expect_s3_class(offset_rows, "data.frame")
  expect_true(nrow(offset_rows) > 0)
})

test_that("lancedb_offset validates inputs", {
  skip_if_no_backend()
  env <- make_search_table("offset_val")

  lazy <- lancedb_scan(env$tbl)
  expect_error(lancedb_offset(lazy, -1))
  expect_error(lancedb_offset(lazy, "abc"))
  expect_error(lancedb_offset("not_lazy", 10))
})

# ---------------------------------------------------------------------------
# Postfilter
# ---------------------------------------------------------------------------

test_that("lancedb_postfilter appends postfilter op", {
  skip_if_no_backend()
  env <- make_search_table("postfilter")

  lazy <- lancedb_search(env$tbl, c(1, 2, 3, 4)) |>
    lancedb_postfilter()

  expect_equal(length(lazy$ops), 1)
  expect_equal(lazy$ops[[1]]$op, "postfilter")
})

test_that("lancedb_postfilter executes with search", {
  skip_if_no_backend()
  env <- make_search_table("postfilter_exec")

  results <- lancedb_search(env$tbl, c(1, 2, 3, 4)) |>
    filter.lancedb_lazy(category == "A") |>
    lancedb_postfilter() |>
    slice_head.lancedb_lazy(n = 10) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) > 0)
})

# ---------------------------------------------------------------------------
# Fast search
# ---------------------------------------------------------------------------

test_that("lancedb_fast_search appends fast_search op", {
  skip_if_no_backend()
  env <- make_search_table("fast")

  lazy <- lancedb_search(env$tbl, c(1, 2, 3, 4)) |>
    lancedb_fast_search()

  expect_equal(length(lazy$ops), 1)
  expect_equal(lazy$ops[[1]]$op, "fast_search")
})

test_that("lancedb_fast_search executes with search", {
  skip_if_no_backend()
  env <- make_search_table("fast_exec")

  results <- lancedb_search(env$tbl, c(1, 2, 3, 4)) |>
    lancedb_fast_search() |>
    slice_head.lancedb_lazy(n = 5) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) > 0)
})

# ---------------------------------------------------------------------------
# With row ID
# ---------------------------------------------------------------------------

test_that("lancedb_with_row_id appends with_row_id op", {
  skip_if_no_backend()
  env <- make_search_table("rowid")

  lazy <- lancedb_scan(env$tbl) |>
    lancedb_with_row_id()

  expect_equal(length(lazy$ops), 1)
  expect_equal(lazy$ops[[1]]$op, "with_row_id")
})

test_that("lancedb_with_row_id includes _rowid column", {
  skip_if_no_backend()
  env <- make_search_table("rowid_exec")

  results <- lancedb_scan(env$tbl) |>
    lancedb_with_row_id() |>
    slice_head.lancedb_lazy(n = 5) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) > 0)
  expect_true("_rowid" %in% names(results))
})

# ---------------------------------------------------------------------------
# Distance range
# ---------------------------------------------------------------------------

test_that("lancedb_search stores distance_range in config", {
  skip_if_no_backend()
  env <- make_search_table("drange")

  lazy <- lancedb_search(env$tbl, c(1, 2, 3, 4),
                          distance_range = c(0.0, 100.0))

  expect_equal(lazy$search_config$distance_range_lower, 0.0)
  expect_equal(lazy$search_config$distance_range_upper, 100.0)
})

test_that("lancedb_search with distance_range executes", {
  skip_if_no_backend()
  env <- make_search_table("drange_exec")

  results <- lancedb_search(env$tbl, c(1, 2, 3, 4),
                             distance_range = c(0.0, 10000.0)) |>
    slice_head.lancedb_lazy(n = 10) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) > 0)
  expect_true("_distance" %in% names(results))
})

test_that("distance_range validates inputs", {
  skip_if_no_backend()
  env <- make_search_table("drange_val")

  expect_error(lancedb_search(env$tbl, c(1, 2, 3, 4),
                               distance_range = c(1.0)))
  expect_error(lancedb_search(env$tbl, c(1, 2, 3, 4),
                               distance_range = "not_numeric"))
})

# ---------------------------------------------------------------------------
# ops_to_json handles new op types
# ---------------------------------------------------------------------------

test_that("ops_to_json serializes offset op", {
  json <- ops_to_json(list(list(op = "offset", n = 10L)))
  expect_true(grepl('"op":"offset"', json))
  expect_true(grepl('"n":10', json))
})

test_that("ops_to_json serializes postfilter op", {
  json <- ops_to_json(list(list(op = "postfilter")))
  expect_equal(json, '[{"op":"postfilter"}]')
})

test_that("ops_to_json serializes fast_search op", {
  json <- ops_to_json(list(list(op = "fast_search")))
  expect_equal(json, '[{"op":"fast_search"}]')
})

test_that("ops_to_json serializes with_row_id op", {
  json <- ops_to_json(list(list(op = "with_row_id")))
  expect_equal(json, '[{"op":"with_row_id"}]')
})

# ---------------------------------------------------------------------------
# show_query for hybrid mode
# ---------------------------------------------------------------------------

test_that("show_query handles hybrid mode", {
  skip_if_no_backend()
  env <- make_search_table("show_hybrid")

  lazy <- lancedb_hybrid_search(env$tbl, c(1, 2, 3, 4), "science",
                                 column = "vec")

  output <- capture.output(show_query(lazy))
  expect_true(any(grepl("Hybrid Search", output)))
  expect_true(any(grepl("science", output)))
})

test_that("show_query includes new ops", {
  skip_if_no_backend()
  env <- make_search_table("show_ops")

  lazy <- lancedb_scan(env$tbl) |>
    lancedb_offset(10) |>
    lancedb_with_row_id()

  output <- capture.output(show_query(lazy))
  expect_true(any(grepl("OFFSET", output)))
  expect_true(any(grepl("WITH_ROW_ID", output)))
})

# ---------------------------------------------------------------------------
# print.lancedb_lazy for hybrid and new ops
# ---------------------------------------------------------------------------

test_that("print.lancedb_lazy shows hybrid mode", {
  skip_if_no_backend()
  env <- make_search_table("print_hybrid")

  lazy <- lancedb_hybrid_search(env$tbl, c(1, 2, 3, 4), "music",
                                 column = "vec")

  output <- capture.output(print(lazy))
  expect_true(any(grepl("hybrid", output)))
  expect_true(any(grepl("music", output)))
})

test_that("print.lancedb_lazy shows new ops", {
  skip_if_no_backend()
  env <- make_search_table("print_ops")

  lazy <- lancedb_search(env$tbl, c(1, 2, 3, 4)) |>
    lancedb_offset(5) |>
    lancedb_postfilter() |>
    lancedb_fast_search()

  output <- capture.output(print(lazy))
  expect_true(any(grepl("offset", output)))
  expect_true(any(grepl("postfilter", output)))
  expect_true(any(grepl("fast_search", output)))
})

# ---------------------------------------------------------------------------
# Query immutability with new ops
# ---------------------------------------------------------------------------

test_that("new ops preserve query immutability", {
  skip_if_no_backend()
  env <- make_search_table("immutable_ops")

  lazy1 <- lancedb_scan(env$tbl)
  lazy2 <- lancedb_offset(lazy1, 10)
  lazy3 <- lancedb_with_row_id(lazy2)
  lazy4 <- lancedb_postfilter(lazy3)

  # Each append creates a new object

  expect_equal(length(lazy1$ops), 0)
  expect_equal(length(lazy2$ops), 1)
  expect_equal(length(lazy3$ops), 2)
  expect_equal(length(lazy4$ops), 3)
})
