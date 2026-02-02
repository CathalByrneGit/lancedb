# Integration tests are disabled by default.
# Enable locally or in CI with:
# Sys.setenv(LANCEDBR_RUN_INTEGRATION_TESTS = "true")
skip_if_no_integration <- function() {
  val <- Sys.getenv("LANCEDBR_RUN_INTEGRATION_TESTS", unset = "")
  if (!identical(tolower(val), "true")) {
    testthat::skip("Integration tests disabled (set LANCEDBR_RUN_INTEGRATION_TESTS=true).")
  }
}

skip_if_no_reticulate_backend <- function() {
  if (!identical(lancedbr_backend(), "reticulate")) {
    testthat::skip("Not running reticulate backend tests on non-reticulate backend.")
  }
}

skip_if_no_python <- function() {
  ok <- TRUE
  tryCatch(.ensure_python(), error = function(e) ok <<- FALSE)
  if (!ok) testthat::skip("No usable Python available via reticulate.")
}


skip_if_no_py_module <- function(module) {
  available <- FALSE
  available <- tryCatch(reticulate::py_module_available(module), error = function(e) FALSE)
  if (!isTRUE(available)) testthat::skip(paste0("Python module not available: ", module))
}

skip_if_no_arrow_pkg <- function() {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    testthat::skip("R package 'arrow' not installed.")
  }
}
