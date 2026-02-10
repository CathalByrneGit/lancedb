test_that("data_to_ipc works with data.frame", {
  skip_if_not_installed("arrow")

  df <- data.frame(
    x = 1:5,
    y = letters[1:5],
    stringsAsFactors = FALSE
  )

  ipc <- data_to_ipc(df)
  expect_type(ipc, "raw")
  expect_true(length(ipc) > 0)

  # Round-trip: decode back
  reader <- arrow::RecordBatchStreamReader$create(ipc)
  result <- reader$read_table()
  result_df <- as.data.frame(result)
  expect_equal(result_df$x, df$x)
  expect_equal(result_df$y, df$y)
})

test_that("data_to_ipc works with arrow::Table", {
  skip_if_not_installed("arrow")

  tbl <- arrow::arrow_table(a = 1:3, b = c("x", "y", "z"))
  ipc <- data_to_ipc(tbl)
  expect_type(ipc, "raw")
  expect_true(length(ipc) > 0)
})

test_that("data_to_ipc rejects invalid input", {
  expect_error(data_to_ipc("not a data.frame"), "data.frame")
  expect_error(data_to_ipc(42), "data.frame")
})

test_that("jsonlite_parse handles simple arrays", {
  json <- '[{"name":"x","type":"Int32","nullable":true}]'
  result <- jsonlite_parse(json)
  expect_type(result, "list")
  expect_equal(length(result), 1)
})
