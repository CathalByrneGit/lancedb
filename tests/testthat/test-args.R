test_that("tbl_search() validates tbl and query", {
  expect_error(
    tbl_search(list(), query = numeric()),
    "`tbl` must be a lancedb_table"
  )

  fake_tbl <- structure(list(), class = "lancedb_table")
  expect_error(
    tbl_search(fake_tbl, query = numeric()),
    "`query` must be a non-empty numeric vector"
  )
})

test_that("tbl_add() validates tbl", {
  expect_error(
    tbl_add(list(), data.frame()),
    "`tbl` must be a lancedb_table"
  )
})
