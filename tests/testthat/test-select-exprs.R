# Tests for lancedb_select_exprs() — dynamic SQL column projection

skip_if_not_installed("arrow")

skip_if_no_backend <- function() {
  tryCatch({
    rust_connect(tempdir())
    TRUE
  }, error = function(e) {
    skip("LanceDB Rust backend not available (not compiled)")
  })
}

make_test_table <- function(suffix) {
  db_path <- file.path(tempdir(), paste0("lancedb_exprs_", Sys.getpid(), "_", suffix))
  con <- lancedb_connect(db_path)
  df <- data.frame(
    id    = 1:20,
    name  = paste0("item_", 1:20),
    score = as.double(1:20) / 20.0,
    stringsAsFactors = FALSE
  )
  df$vec <- I(lapply(1:20, function(i) as.double(c(i, i + 1))))
  tbl <- lancedb_create_table(con, paste0("tbl_", suffix), df, mode = "overwrite")
  list(con = con, tbl = tbl, df = df)
}

# ---------------------------------------------------------------------------
# Basic validation
# ---------------------------------------------------------------------------

test_that("lancedb_select_exprs returns a lancedb_lazy", {
  skip_if_no_backend()
  env <- make_test_table("basic")

  lazy <- lancedb_scan(env$tbl) |>
    lancedb_select_exprs(score_pct = "score * 100")

  expect_s3_class(lazy, "lancedb_lazy")
  expect_equal(length(lazy$ops), 1)
  expect_equal(lazy$ops[[1]]$op, "select_expr")
  expect_equal(lazy$ops[[1]]$exprs$score_pct, "score * 100")
})

test_that("lancedb_select_exprs stores multiple expressions", {
  skip_if_no_backend()
  env <- make_test_table("multi")

  lazy <- lancedb_scan(env$tbl) |>
    lancedb_select_exprs(
      score_pct  = "score * 100",
      id_str     = "CAST(id AS VARCHAR)",
      name_upper = "upper(name)"
    )

  expect_equal(length(lazy$ops[[1]]$exprs), 3)
  expect_equal(lazy$ops[[1]]$exprs$score_pct, "score * 100")
  expect_equal(lazy$ops[[1]]$exprs$id_str, "CAST(id AS VARCHAR)")
  expect_equal(lazy$ops[[1]]$exprs$name_upper, "upper(name)")
})

test_that("lancedb_select_exprs errors on empty args", {
  skip_if_no_backend()
  env <- make_test_table("empty")

  expect_error(
    lancedb_select_exprs(lancedb_scan(env$tbl)),
    "requires at least one"
  )
})

test_that("lancedb_select_exprs errors on unnamed args", {
  skip_if_no_backend()
  env <- make_test_table("unnamed")

  expect_error(
    lancedb_select_exprs(lancedb_scan(env$tbl), "score * 100"),
    "must be named"
  )
})

test_that("lancedb_select_exprs errors on non-character values", {
  skip_if_no_backend()
  env <- make_test_table("nonchar")

  expect_error(
    lancedb_select_exprs(lancedb_scan(env$tbl), score_pct = 100),
    "character strings"
  )
})

test_that("lancedb_select_exprs errors on non-lazy input", {
  skip_if_no_backend()
  expect_error(
    lancedb_select_exprs("not_a_lazy", score = "score"),
    "inherits"
  )
})

# ---------------------------------------------------------------------------
# Execution tests
# ---------------------------------------------------------------------------

test_that("lancedb_select_exprs executes and returns computed columns", {
  skip_if_no_backend()
  env <- make_test_table("exec")

  results <- lancedb_scan(env$tbl) |>
    lancedb_select_exprs(score_pct = "score * 100") |>
    slice_head.lancedb_lazy(n = 5) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) > 0)
  expect_true("score_pct" %in% names(results))
  # score * 100 should be in [0, 100]
  expect_true(all(results$score_pct <= 100.0 + 1e-9))
})

test_that("lancedb_select_exprs returns multiple computed columns", {
  skip_if_no_backend()
  env <- make_test_table("multi_exec")

  results <- lancedb_scan(env$tbl) |>
    lancedb_select_exprs(
      score_pct  = "score * 100",
      id_str     = "CAST(id AS VARCHAR)"
    ) |>
    slice_head.lancedb_lazy(n = 3) |>
    collect.lancedb_lazy()

  expect_true("score_pct" %in% names(results))
  expect_true("id_str" %in% names(results))
  expect_equal(nrow(results), 3)
})

test_that("lancedb_select_exprs works after filter", {
  skip_if_no_backend()
  env <- make_test_table("filter_exec")

  results <- lancedb_scan(env$tbl) |>
    filter.lancedb_lazy(id > 10) |>
    lancedb_select_exprs(
      name       = "name",
      score_pct  = "score * 100"
    ) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true(nrow(results) > 0)
  expect_true("name" %in% names(results))
  expect_true("score_pct" %in% names(results))
})

test_that("lancedb_select_exprs works with vector search", {
  skip_if_no_backend()
  env <- make_test_table("vec_exec")

  results <- lancedb_search(env$tbl, c(1.0, 2.0)) |>
    lancedb_select_exprs(
      name      = "name",
      norm_dist = "_distance / 100"
    ) |>
    slice_head.lancedb_lazy(n = 5) |>
    collect.lancedb_lazy()

  expect_s3_class(results, "data.frame")
  expect_true("name" %in% names(results))
  expect_true("norm_dist" %in% names(results))
})

# ---------------------------------------------------------------------------
# ops_to_json serialisation
# ---------------------------------------------------------------------------

test_that("ops_to_json serializes select_expr correctly", {
  op <- list(op = "select_expr", exprs = list(score_pct = "score * 100"))
  json <- ops_to_json(list(op))
  expect_true(grepl('"op":"select_expr"', json))
  expect_true(grepl('"score_pct"', json))
  expect_true(grepl('"score \\* 100"', json))
})

test_that("ops_to_json serializes multiple select_expr expressions", {
  op <- list(
    op = "select_expr",
    exprs = list(a = "score * 2", b = "upper(name)")
  )
  json <- ops_to_json(list(op))
  expect_true(grepl('"a"', json))
  expect_true(grepl('"b"', json))
  expect_true(grepl('"score \\* 2"', json))
})

# ---------------------------------------------------------------------------
# show_query and print for select_expr
# ---------------------------------------------------------------------------

test_that("show_query displays select_expr op", {
  skip_if_no_backend()
  env <- make_test_table("show_expr")

  lazy <- lancedb_scan(env$tbl) |>
    lancedb_select_exprs(score_pct = "score * 100")

  output <- capture.output(show_query(lazy))
  expect_true(any(grepl("computed", output)))
  expect_true(any(grepl("score_pct", output)))
})

test_that("print.lancedb_lazy shows select_expr op", {
  skip_if_no_backend()
  env <- make_test_table("print_expr")

  lazy <- lancedb_scan(env$tbl) |>
    lancedb_select_exprs(score_pct = "score * 100")

  output <- capture.output(print(lazy))
  expect_true(any(grepl("select_exprs", output)))
  expect_true(any(grepl("score_pct", output)))
})

# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------

test_that("lancedb_select_exprs preserves query immutability", {
  skip_if_no_backend()
  env <- make_test_table("immut_expr")

  lazy1 <- lancedb_scan(env$tbl)
  lazy2 <- lancedb_select_exprs(lazy1, score_pct = "score * 100")
  lazy3 <- lancedb_select_exprs(lazy2, id_str = "CAST(id AS VARCHAR)")

  expect_equal(length(lazy1$ops), 0)
  expect_equal(length(lazy2$ops), 1)
  expect_equal(length(lazy3$ops), 2)
  expect_equal(lazy3$ops[[2]]$exprs$id_str, "CAST(id AS VARCHAR)")
})
