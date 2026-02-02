test_that("lancedb_backend() defaults to reticulate", {
  old <- getOption("lancedb.backend")
  on.exit(options(lancedb.backend = old), add = TRUE)

  options(lancedb.backend = NULL)
  expect_identical(lancedb_backend(), "reticulate")
})

test_that("lancedb_backend() errors on unknown backend", {
  old <- getOption("lancedb.backend")
  on.exit(options(lancedb.backend = old), add = TRUE)

  options(lancedb.backend = "nope")
  expect_error(lancedb_backend(), "Unknown backend")
})
