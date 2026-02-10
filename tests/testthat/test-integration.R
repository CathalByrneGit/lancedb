# End-to-end integration tests
# These tests require the compiled Rust backend.
# They are skipped if the native library is not available.

skip_if_not_installed("arrow")

skip_if_no_backend <- function() {
  tryCatch({
    rust_connect(tempdir())
    TRUE
  }, error = function(e) {
    skip("LanceDB Rust backend not available (not compiled)")
  })
}

# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------

test_that("lancedb_connect creates a connection to a temp directory", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid()))
  con <- lancedb_connect(db_path)

  expect_s3_class(con, "lancedb_connection")
  expect_equal(con$uri, normalizePath(db_path, mustWork = FALSE))
  expect_output(print(con), "lancedb_connection")
})

# ---------------------------------------------------------------------------
# Table creation + basic operations
# ---------------------------------------------------------------------------

test_that("create_table and open_table round-trip works", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_rt"))
  con <- lancedb_connect(db_path)

  df <- data.frame(
    id = 1:50,
    name = paste0("item_", 1:50),
    score = seq(0.02, 1.0, length.out = 50),
    category = rep(c("A", "B", "C", "D", "E"), each = 10),
    stringsAsFactors = FALSE
  )

  tbl <- lancedb_create_table(con, "test_roundtrip", df, mode = "overwrite")
  expect_s3_class(tbl, "lancedb_table")
  expect_equal(tbl$name, "test_roundtrip")

  # Re-open the table
  tbl2 <- lancedb_open_table(con, "test_roundtrip")
  expect_s3_class(tbl2, "lancedb_table")

  # Count rows
  expect_equal(lancedb_count_rows(tbl), 50)

  # Print should work
  expect_output(print(tbl), "lancedb_table")
  expect_output(print(tbl), "test_roundtrip")
})

# ---------------------------------------------------------------------------
# Scan → collect (basic)
# ---------------------------------------------------------------------------

test_that("scan + collect returns all data as data.frame", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_scan"))
  con <- lancedb_connect(db_path)

  df <- data.frame(
    id = 1:20,
    name = paste0("item_", 1:20),
    value = as.double(1:20),
    stringsAsFactors = FALSE
  )

  tbl <- lancedb_create_table(con, "scan_test", df, mode = "overwrite")
  lazy <- lancedb_scan(tbl)

  result <- collect.lancedb_lazy(lazy)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 20)
  expect_true("id" %in% names(result))
  expect_true("name" %in% names(result))
  expect_true("value" %in% names(result))
})

test_that("scan + collect as arrow returns Arrow Table", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_arrow"))
  con <- lancedb_connect(db_path)

  df <- data.frame(id = 1:5, val = c(10.0, 20.0, 30.0, 40.0, 50.0))
  tbl <- lancedb_create_table(con, "arrow_test", df, mode = "overwrite")

  result <- collect.lancedb_lazy(lancedb_scan(tbl), as = "arrow")
  expect_true(inherits(result, "ArrowTabular") || inherits(result, "Table"))
})

# ---------------------------------------------------------------------------
# Scan → filter → collect
# ---------------------------------------------------------------------------

test_that("scan + filter (string) + collect works", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_filt1"))
  con <- lancedb_connect(db_path)

  df <- data.frame(
    id = 1:30,
    score = as.double(1:30),
    stringsAsFactors = FALSE
  )

  tbl <- lancedb_create_table(con, "filter_str", df, mode = "overwrite")

  result <- lancedb_scan(tbl) |>
    filter.lancedb_lazy("score > 20") |>
    collect.lancedb_lazy()

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 10)
  expect_true(all(result$score > 20))
})

test_that("scan + filter (R expression) + collect works", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_filt2"))
  con <- lancedb_connect(db_path)

  df <- data.frame(
    id = 1:30,
    score = as.double(1:30),
    stringsAsFactors = FALSE
  )

  tbl <- lancedb_create_table(con, "filter_expr", df, mode = "overwrite")

  result <- lancedb_scan(tbl) |>
    filter.lancedb_lazy(score > 25) |>
    collect.lancedb_lazy()

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 5)
  expect_true(all(result$score > 25))
})

# ---------------------------------------------------------------------------
# Scan → select → collect
# ---------------------------------------------------------------------------

test_that("scan + select + collect returns only selected columns", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_sel"))
  con <- lancedb_connect(db_path)

  df <- data.frame(
    id = 1:10,
    name = paste0("item_", 1:10),
    score = runif(10),
    extra = letters[1:10],
    stringsAsFactors = FALSE
  )

  tbl <- lancedb_create_table(con, "select_test", df, mode = "overwrite")

  result <- lancedb_scan(tbl) |>
    select.lancedb_lazy(id, name) |>
    collect.lancedb_lazy()

  expect_s3_class(result, "data.frame")
  expect_equal(names(result), c("id", "name"))
  expect_equal(nrow(result), 10)
})

# ---------------------------------------------------------------------------
# Scan → slice_head → collect
# ---------------------------------------------------------------------------

test_that("scan + slice_head + collect limits rows", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_lim"))
  con <- lancedb_connect(db_path)

  df <- data.frame(id = 1:100, val = runif(100))
  tbl <- lancedb_create_table(con, "limit_test", df, mode = "overwrite")

  result <- lancedb_scan(tbl) |>
    slice_head.lancedb_lazy(n = 7) |>
    collect.lancedb_lazy()

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 7)
})

# ---------------------------------------------------------------------------
# Full pipeline: scan → filter → select → limit → collect
# ---------------------------------------------------------------------------

test_that("full pipeline: scan + filter + select + limit + collect", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_pipe"))
  con <- lancedb_connect(db_path)

  df <- data.frame(
    id = 1:100,
    name = paste0("item_", 1:100),
    score = as.double(1:100),
    category = rep(c("X", "Y"), each = 50),
    stringsAsFactors = FALSE
  )

  tbl <- lancedb_create_table(con, "pipeline_test", df, mode = "overwrite")

  result <- lancedb_scan(tbl) |>
    filter.lancedb_lazy(score > 50) |>
    select.lancedb_lazy(id, name, score) |>
    slice_head.lancedb_lazy(n = 10) |>
    collect.lancedb_lazy()

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) <= 10)
  expect_true(all(result$score > 50))
  expect_equal(names(result), c("id", "name", "score"))
})

# ---------------------------------------------------------------------------
# Add data + delete rows
# ---------------------------------------------------------------------------

test_that("lancedb_add appends rows and lancedb_delete removes them", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_addel"))
  con <- lancedb_connect(db_path)

  df <- data.frame(id = 1:10, val = as.double(1:10))
  tbl <- lancedb_create_table(con, "add_del_test", df, mode = "overwrite")

  expect_equal(lancedb_count_rows(tbl), 10)

  # Append more data
  new_data <- data.frame(id = 11:15, val = as.double(11:15))
  lancedb_add(tbl, new_data)
  expect_equal(lancedb_count_rows(tbl), 15)

  # Delete some rows
  lancedb_delete(tbl, "id > 12")
  expect_equal(lancedb_count_rows(tbl), 12)
})

# ---------------------------------------------------------------------------
# Count rows with filter
# ---------------------------------------------------------------------------

test_that("lancedb_count_rows with filter works", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_cnt"))
  con <- lancedb_connect(db_path)

  df <- data.frame(
    id = 1:50,
    category = rep(c("A", "B"), each = 25),
    stringsAsFactors = FALSE
  )

  tbl <- lancedb_create_table(con, "count_test", df, mode = "overwrite")

  expect_equal(lancedb_count_rows(tbl), 50)
  expect_equal(lancedb_count_rows(tbl, filter = "category = 'A'"), 25)
})

# ---------------------------------------------------------------------------
# show_query and print on real lazy objects
# ---------------------------------------------------------------------------

test_that("show_query on real table works", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_sq"))
  con <- lancedb_connect(db_path)

  df <- data.frame(id = 1:5, val = 1.0:5.0)
  tbl <- lancedb_create_table(con, "show_q_test", df, mode = "overwrite")

  lazy <- lancedb_scan(tbl) |>
    filter.lancedb_lazy("id > 2") |>
    select.lancedb_lazy(id, val) |>
    slice_head.lancedb_lazy(n = 3)

  expect_output(show_query(lazy), "LanceDB Query Plan")
  expect_output(show_query(lazy), "WHERE")
  expect_output(show_query(lazy), "SELECT")
  expect_output(show_query(lazy), "LIMIT")
})
