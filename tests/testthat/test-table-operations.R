# Tests for new table operations
# These tests require the compiled Rust backend.

skip_if_not_installed("arrow")

skip_if_no_backend <- function() {
  tryCatch({
    rust_connect(tempdir())
    TRUE
  }, error = function(e) {
    skip("LanceDB Rust backend not available (not compiled)")
  })
}

# Helper to create a test table with common data
make_test_table <- function(suffix) {
  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_", suffix))
  con <- lancedb_connect(db_path)
  df <- data.frame(
    id = 1:20,
    name = paste0("item_", 1:20),
    score = as.double(1:20),
    category = rep(c("A", "B"), each = 10),
    stringsAsFactors = FALSE
  )
  tbl <- lancedb_create_table(con, paste0("test_", suffix), df, mode = "overwrite")
  list(con = con, tbl = tbl, df = df)
}

# ---------------------------------------------------------------------------
# Connection: list_tables, drop_table
# ---------------------------------------------------------------------------

test_that("lancedb_list_tables returns table names", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_list"))
  con <- lancedb_connect(db_path)

  df <- data.frame(id = 1:5, val = 1.0:5.0)
  lancedb_create_table(con, "alpha", df, mode = "overwrite")
  lancedb_create_table(con, "beta", df, mode = "overwrite")

  tables <- lancedb_list_tables(con)
  expect_type(tables, "character")
  expect_true("alpha" %in% tables)
  expect_true("beta" %in% tables)
})

test_that("lancedb_drop_table removes a table", {
  skip_if_no_backend()

  db_path <- file.path(tempdir(), paste0("lancedb_test_", Sys.getpid(), "_drop"))
  con <- lancedb_connect(db_path)

  df <- data.frame(id = 1:5, val = 1.0:5.0)
  lancedb_create_table(con, "to_drop", df, mode = "overwrite")

  expect_true("to_drop" %in% lancedb_list_tables(con))
  lancedb_drop_table(con, "to_drop")
  expect_false("to_drop" %in% lancedb_list_tables(con))
})

# ---------------------------------------------------------------------------
# Schema & version
# ---------------------------------------------------------------------------

test_that("lancedb_schema returns field info as data.frame", {
  skip_if_no_backend()
  env <- make_test_table("schema")

  schema <- lancedb_schema(env$tbl)
  expect_s3_class(schema, "data.frame")
  expect_true("name" %in% names(schema))
  expect_true("type" %in% names(schema))
  expect_true("nullable" %in% names(schema))
  expect_true("id" %in% schema$name)
  expect_true("name" %in% schema$name)
  expect_true("score" %in% schema$name)
})

test_that("lancedb_version returns an integer", {
  skip_if_no_backend()
  env <- make_test_table("version")

  ver <- lancedb_version(env$tbl)
  expect_true(is.numeric(ver))
  expect_true(ver >= 1)
})

# ---------------------------------------------------------------------------
# Head & to_arrow
# ---------------------------------------------------------------------------

test_that("lancedb_head returns first n rows", {
  skip_if_no_backend()
  env <- make_test_table("head")

  result <- lancedb_head(env$tbl, n = 3)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 3)
})

test_that("lancedb_to_arrow returns Arrow Table", {
  skip_if_no_backend()
  env <- make_test_table("toarrow")

  result <- lancedb_to_arrow(env$tbl)
  expect_true(inherits(result, "ArrowTabular") || inherits(result, "Table"))
})

# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

test_that("lancedb_update modifies all rows without where clause", {
  skip_if_no_backend()
  env <- make_test_table("update_all")

  lancedb_update(env$tbl, values = list(score = "0"))

  result <- lancedb_scan(env$tbl) |>
    select.lancedb_lazy(score) |>
    collect.lancedb_lazy()
  expect_true(all(result$score == 0))
})

test_that("lancedb_update modifies only matching rows with where clause", {
  skip_if_no_backend()
  env <- make_test_table("update_where")

  lancedb_update(env$tbl, values = list(score = "999"), where = "id <= 5")

  result <- lancedb_scan(env$tbl) |>
    collect.lancedb_lazy()
  expect_equal(sum(result$score == 999), 5)
  expect_true(all(result$score[result$id > 5] != 999))
})

# ---------------------------------------------------------------------------
# Merge Insert (upsert)
# ---------------------------------------------------------------------------

test_that("lancedb_merge_insert performs upsert", {
  skip_if_no_backend()
  env <- make_test_table("upsert")

  # Upsert: update id=1, insert id=999
  new_data <- data.frame(
    id = c(1L, 999L),
    name = c("updated_1", "new_999"),
    score = c(100.0, 100.0),
    category = c("A", "Z"),
    stringsAsFactors = FALSE
  )

  lancedb_merge_insert(env$tbl, new_data, on = "id")

  count <- lancedb_count_rows(env$tbl)
  expect_equal(count, 21)  # 20 original + 1 new

  result <- lancedb_scan(env$tbl) |>
    filter.lancedb_lazy("id = 1") |>
    collect.lancedb_lazy()
  expect_equal(result$name[1], "updated_1")
})

test_that("lancedb_merge_insert insert-if-not-exists only", {
  skip_if_no_backend()
  env <- make_test_table("insert_only")

  new_data <- data.frame(
    id = c(1L, 888L),
    name = c("should_not_update", "new_888"),
    score = c(-1.0, 888.0),
    category = c("X", "X"),
    stringsAsFactors = FALSE
  )

  lancedb_merge_insert(env$tbl, new_data, on = "id",
                        when_matched_update_all = FALSE)

  count <- lancedb_count_rows(env$tbl)
  expect_equal(count, 21)  # 20 + 1 new (id=888)

  # id=1 should NOT have been updated
  result <- lancedb_scan(env$tbl) |>
    filter.lancedb_lazy("id = 1") |>
    collect.lancedb_lazy()
  expect_equal(result$name[1], "item_1")
})

# ---------------------------------------------------------------------------
# Indexing
# ---------------------------------------------------------------------------

test_that("lancedb_create_index and lancedb_list_indices work for scalar index", {
  skip_if_no_backend()
  env <- make_test_table("index")

  lancedb_create_index(env$tbl, "category", index_type = "btree")

  indices <- lancedb_list_indices(env$tbl)
  expect_s3_class(indices, "data.frame")
  expect_true(nrow(indices) >= 1)
  expect_true("name" %in% names(indices))
  expect_true("index_type" %in% names(indices))
  expect_true("columns" %in% names(indices))
})

# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------

test_that("lancedb_add_columns adds a computed column", {
  skip_if_no_backend()
  env <- make_test_table("addcol")

  lancedb_add_columns(env$tbl, list(double_score = "score * 2"))

  schema <- lancedb_schema(env$tbl)
  expect_true("double_score" %in% schema$name)

  result <- lancedb_head(env$tbl, n = 5)
  expect_true("double_score" %in% names(result))
})

test_that("lancedb_drop_columns removes columns", {
  skip_if_no_backend()
  env <- make_test_table("dropcol")

  lancedb_drop_columns(env$tbl, "category")

  schema <- lancedb_schema(env$tbl)
  expect_false("category" %in% schema$name)
})

test_that("lancedb_alter_columns renames a column", {
  skip_if_no_backend()
  env <- make_test_table("altercol")

  lancedb_alter_columns(env$tbl, list(
    list(path = "category", rename = "group")
  ))

  schema <- lancedb_schema(env$tbl)
  expect_false("category" %in% schema$name)
  expect_true("group" %in% schema$name)
})

# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------

test_that("lancedb_list_versions returns version history", {
  skip_if_no_backend()
  env <- make_test_table("versions")

  versions <- lancedb_list_versions(env$tbl)
  expect_s3_class(versions, "data.frame")
  expect_true(nrow(versions) >= 1)
  expect_true("version" %in% names(versions))
  expect_true("timestamp" %in% names(versions))
})

test_that("lancedb_checkout and lancedb_checkout_latest work", {
  skip_if_no_backend()
  env <- make_test_table("checkout")

  v1 <- lancedb_version(env$tbl)

  # Add data to create a new version
  lancedb_add(env$tbl, data.frame(
    id = 100L, name = "extra", score = 0.0, category = "Z",
    stringsAsFactors = FALSE
  ))

  v2 <- lancedb_version(env$tbl)
  expect_true(v2 > v1)

  # Checkout v1 (fewer rows)
  lancedb_checkout(env$tbl, version = v1)
  expect_equal(lancedb_count_rows(env$tbl), 20)

  # Go back to latest
  lancedb_checkout_latest(env$tbl)
  expect_equal(lancedb_count_rows(env$tbl), 21)
})

test_that("lancedb_restore creates a new version from checkout", {
  skip_if_no_backend()
  env <- make_test_table("restore")

  v1 <- lancedb_version(env$tbl)

  # Add data
  lancedb_add(env$tbl, data.frame(
    id = 200L, name = "extra2", score = 0.0, category = "Z",
    stringsAsFactors = FALSE
  ))

  # Checkout v1 and restore
  lancedb_checkout(env$tbl, version = v1)
  lancedb_restore(env$tbl)

  # After restore, we have a new version with original data
  expect_equal(lancedb_count_rows(env$tbl), 20)
  v3 <- lancedb_version(env$tbl)
  expect_true(v3 > v1)
})

# ---------------------------------------------------------------------------
# Optimization
# ---------------------------------------------------------------------------

test_that("lancedb_compact_files runs without error", {
  skip_if_no_backend()
  env <- make_test_table("compact")

  # Add some more data to create fragments
  for (i in 1:3) {
    lancedb_add(env$tbl, data.frame(
      id = as.integer(100 + i), name = paste0("extra_", i),
      score = as.double(i), category = "X",
      stringsAsFactors = FALSE
    ))
  }

  expect_no_error(lancedb_compact_files(env$tbl))
})

test_that("lancedb_cleanup_old_versions runs without error", {
  skip_if_no_backend()
  env <- make_test_table("cleanup")

  # With default older_than_days = 7, nothing will be pruned (too recent)
  # but the function should not error
  expect_no_error(lancedb_cleanup_old_versions(env$tbl))
})
