test_that("lancedbr_info() returns a printable info object", {
  # lancedbr_info() should be safe even without python working,
  # but it may contain NULL python fields.
  info <- lancedbr_info(pkgs = c("lancedb"))
  expect_s3_class(info, "lancedbr_info")
  expect_true(is.list(info))
  expect_true(!is.null(info$backend))
  expect_true(!is.null(info$r))

  expect_output(print(info), "lancedbr info")
})

test_that("lancedbr_info() reports python package versions when integration enabled", {
  skip_if_no_integration()
  skip_if_no_reticulate_backend()
  skip_if_no_python()
  skip_if_no_py_module("lancedb")

  info <- lancedbr_info(pkgs = c("lancedb"))
  expect_true("python_packages" %in% names(info))
  # Named vector/list with 'lancedb' entry
  expect_true("lancedb" %in% names(info$python_packages))
})
