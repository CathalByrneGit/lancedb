#' Get Python package versions
#'
#' Internal helper that retrieves installed Python package versions
#' using \code{importlib.metadata}. This works on Python >= 3.8 and
#' falls back to the \code{importlib_metadata} backport if needed.
#'
#' The Python environment is initialized via \code{.ensure_python()}.
#'
#' @param pkgs Character vector of Python package names.
#'
#' @return A named character vector (or list) of package versions.
#'   Packages that are not installed return \code{NULL}.
#'
#' @keywords internal
# internal: get python package versions using importlib.metadata (works on py>=3.8)
.py_pkg_versions <- function(pkgs) {
  .ensure_python()

  code <- "
try:
    from importlib.metadata import version, PackageNotFoundError
except Exception:
    from importlib_metadata import version, PackageNotFoundError

def get_version_one(p):
    try:
        return version(p)
    except PackageNotFoundError:
        return None
"
  reticulate::py_run_string(code, local = TRUE)

  out <- stats::setNames(rep(NA_character_, length(pkgs)), pkgs)
  for (p in pkgs) {
    v <- tryCatch(reticulate::py$get_version_one(p), error = function(e) NULL)
    if (!is.null(v)) out[[p]] <- as.character(v)
  }
  out
}




#' Report LanceDB R and Python environment information
#'
#' Collects diagnostic information about the current \pkg{lancedbr}
#' setup, including:
#'
#' \itemize{
#'   \item active backend
#'   \item R version and platform
#'   \item Python configuration used by \pkg{reticulate}
#'   \item installed Python package versions (e.g., \code{lancedb}, \code{pyarrow})
#' }
#'
#' This function is useful for debugging, reproducibility, and
#' reporting issues.
#'
#' @param pkgs Character vector of Python packages to report versions for.
#'
#' @return An object of class \code{"lancedbr_info"}.
#'
#' @examples
#' \dontrun{
#' lancedbr_info()
#' }
#'
#' @export
lancedbr_info <- function(pkgs = c("lancedb", "pyarrow", "numpy", "pandas")) {
  backend <- lancedbr_backend()

  # Avoid forcing python init if user just wants backend
  cfg <- tryCatch(reticulate::py_config(error = FALSE), error = function(e) NULL)

  versions <- tryCatch(.py_pkg_versions(pkgs), error = function(e) NULL)

  info <- list(
    date = as.character(Sys.time()),
    backend = backend,
    r = list(
      version = paste(R.version$major, R.version$minor, sep = "."),
      platform = R.version$platform
    ),
    python = cfg,
    python_packages = versions,
    options = list(
      lancedbr.backend = getOption("lancedbr.backend"),
      lancedbr.conda_env = Sys.getenv("LANCEDBR_CONDA_ENV", unset = NA_character_),
      lancedbr.python = Sys.getenv("LANCEDBR_PYTHON", unset = NA_character_)
    )
  )

  class(info) <- "lancedbr_info"
  info
}


#' Print method for lancedbr_info objects
#'
#' Nicely formats and prints information returned by
#' \code{lancedbr_info()}.
#'
#' @param x A \code{lancedbr_info} object.
#' @param ... Unused.
#'
#' @return Invisibly returns \code{x}.
#'
#' @keywords internal
print.lancedbr_info <- function(x, ...) {
  cat("lancedbr info\n")
  cat("- date: ", x$date, "\n", sep = "")
  cat("- backend: ", x$backend, "\n", sep = "")
  cat("- R: ", x$r$version, " (", x$r$platform, ")\n", sep = "")

  if (!is.null(x$python)) {
    cat("- python: ", x$python$python, "\n", sep = "")
    cat("  - version: ", x$python$version, "\n", sep = "")
  } else {
    cat("- python: <not initialized>\n")
  }

  if (!is.null(x$python_packages)) {
    cat("- python packages:\n")
    for (nm in names(x$python_packages)) {
      cat(
        "  - ", nm, ": ",
        ifelse(
          is.na(x$python_packages[[nm]]) || is.null(x$python_packages[[nm]]),
          "<not installed>",
          x$python_packages[[nm]]
        ),
        "\n",
        sep = ""
      )
    }
  } else {
    cat("- python packages: <unavailable>\n")
  }

  invisible(x)
}
