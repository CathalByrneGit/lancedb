#' Package load hook for
#'
#' Internal initialization hook called when the package is loaded.
#' No heavy work (such as initializing Python or installing dependencies)
#' should be performed here.
#'
#' @param libname Character string giving the library directory.
#' @param pkgname Character string giving the package name.
#'
#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # no heavy work here; just set defaults
}


#' Read  configuration
#'
#' Retrieves configuration settings for \pkg{}, primarily
#' controlling how the Python environment is selected when using
#' the \code{"reticulate"} backend.
#'
#' Power users can override defaults using environment variables:
#' \itemize{
#'   \item \code{LANCEDB_PYTHON}: Path to a specific Python executable
#'   \item \code{LANCEDB_CONDA_ENV}: Name of a conda environment to use
#' }
#'
#' @return A named list with elements:
#' \describe{
#'   \item{python}{Character scalar giving the Python executable path,
#'     or \code{NA} if not set.}
#'   \item{conda_env}{Character scalar giving the conda environment name.}
#' }
#'
#' @examples
#' lancedb_config()
#'
#' @export
lancedb_config <- function() {
  list(
    python = Sys.getenv("LANCEDB_PYTHON", unset = NA_character_),
    conda_env = Sys.getenv("LANCEDB_CONDA_ENV", unset = NA_character_)
  )
}
