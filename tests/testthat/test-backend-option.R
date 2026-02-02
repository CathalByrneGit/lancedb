test_that("lancedbr_backend() defaults to reticulate", {
  old <- getOption("lancedbr.backend")
  on.exit(options(lancedbr.backend = old), add = TRUE)

  options(lancedbr.backend = NULL)
  expect_identical(lancedbr_backend(), "reticulate")
})

test_that("lancedbr_backend() errors on unknown backend", {
  old <- getOption("lancedbr.backend")
  on.exit(options(lancedbr.backend = old), add = TRUE)

  options(lancedbr.backend = "nope")
  expect_error(lancedbr_backend(), "Unknown backend")
})
