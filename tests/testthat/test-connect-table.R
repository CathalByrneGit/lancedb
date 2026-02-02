test_that("can connect, create table, open table (reticulate)", {
  skip_if_no_integration()
  skip_if_no_reticulate_backend()
  skip_if_no_python()
  skip_if_no_py_module("lancedb")

  dir <- withr::local_tempdir()
  con <- lancedb_connect(dir)

  expect_s3_class(con, "lancedb_connection")
  expect_true(is.list(con))
  expect_true(!is.null(con$.py))

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
  expect_s3_class(tbl, "lancedb_table")
  expect_identical(tbl$name, "example")

  tbl2 <- lancedb_open_table(con, "example")
  expect_s3_class(tbl2, "lancedb_table")
  expect_identical(tbl2$name, "example")
})
