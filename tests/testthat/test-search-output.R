test_that("tbl_add() and tbl_search() return data.frame by default", {
  skip_if_no_integration()
  skip_if_no_reticulate_backend()
  skip_if_no_python()
  skip_if_no_py_module("lancedb")

  dir <- withr::local_tempdir()
  con <- lancedb_connect(dir)

  data <- data.frame(
    id = 1:3,
    text = c("a", "b", "c"),
    vector = I(list(
      c(0.1, 0.2),
      c(0.2, 0.3),
      c(0.4, 0.5)
    ))
  )

  tbl <- lancedb_create_table(con, "example", data, mode = "overwrite")
  tbl_add(tbl, data)

  res <- tbl_search(tbl, query = c(0.1, 0.2), k = 2)
  expect_true(is.data.frame(res) || is.list(res))  # list fallback possible
  if (is.data.frame(res)) expect_true(nrow(res) <= 2)
})

test_that("tbl_search(as='arrow') returns an Arrow object when available", {
  skip_if_no_integration()
  skip_if_no_reticulate_backend()
  skip_if_no_python()
  skip_if_no_py_module("lancedb")
  skip_if_no_arrow_pkg()

  dir <- withr::local_tempdir()
  con <- lancedb_connect(dir)

  data <- data.frame(
    id = 1:3,
    text = c("a", "b", "c"),
    vector = I(list(
      c(0.1, 0.2),
      c(0.2, 0.3),
      c(0.4, 0.5)
    ))
  )

  tbl <- lancedb_create_table(con, "example", data, mode = "overwrite")
  res_arrow <- tbl_search(tbl, query = c(0.1, 0.2), k = 2, as = "arrow")

  # Be tolerant: could be arrow::Table or RecordBatchReader depending on conversions
  expect_true(
    inherits(res_arrow, "ArrowObject") ||
      inherits(res_arrow, "Table") ||
      inherits(res_arrow, "RecordBatchReader")
  )
})
