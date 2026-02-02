##' Ensure Python environment and LanceDB dependencies are available
#'
#' Internal helper that initializes a Python environment for use with
#' reticulate and ensures the Python package 'lancedb' is installed.
#'
#' Strategy:
#' 1. If LANCEDBR_PYTHON is set, use that interpreter.
#' 2. Else if LANCEDBR_CONDA_ENV is set (or default), use that conda env.
#' 3. Else fall back to reticulate-managed Python (py_require / uv).
#'
#' @return Invisibly returns TRUE on success.
#' @keywords internal
.ensure_python <- function() {
  cfg <- lancedbr_config()

  # 1) Explicit python path override
  if (!is.na(cfg$python) && nzchar(cfg$python)) {
    reticulate::use_python(cfg$python, required = TRUE)
    if (!reticulate::py_module_available("lancedb")) {
      reticulate::py_install("lancedb", pip = TRUE)
    }
    return(invisible(TRUE))
  }

  # 2) If user specified a conda env explicitly, honor it
  #    (You can decide whether to treat default "r-lancedb" as "explicit";
  #     this version does.)
  env <- cfg$conda_env
  if (!is.na(env) && nzchar(env)) {
    # # Use conda if it exists; otherwise create it
    # if (!reticulate::miniconda_exists()) {
    #   reticulate::install_miniconda()
    # }
    if (!env %in% reticulate::conda_list()$name) {
      reticulate::conda_create(envname = env)
    }
    reticulate::use_condaenv(env, required = TRUE)

    if (!reticulate::py_module_available("lancedb")) {
      reticulate::py_install("lancedb", envname = env, pip = TRUE)
    }
    return(invisible(TRUE))
  }

  # 3) Fall back to reticulate-managed Python (uv/py_require)
  # Ensure a python is initialized and available
  # (py_require triggers reticulate to provision an interpreter if needed)
  if (requireNamespace("reticulate", quietly = TRUE) &&
      utils::packageVersion("reticulate") >= "1.39.0") {
    # Be minimal: require just python, then install lancedb if missing
    reticulate::py_require()
  } else {
    # Older reticulate fallback: rely on py_config initialization
    reticulate::py_config()
  }

  if (!reticulate::py_module_available("lancedb")) {
    reticulate::py_install("lancedb", pip = TRUE)
  }

  invisible(TRUE)
}


#' Import the Python LanceDB module
#'
#' Internal helper that ensures the Python environment is initialized
#' (via \code{.ensure_python()}) and then imports the \code{lancedb}
#' Python module using \pkg{reticulate}.
#'
#' This function centralizes Python module import so backend implementations
#' do not need to repeat environment setup logic.
#'
#' @return A Python module object corresponding to \code{lancedb}.
#'
#' @keywords internal
.py_lancedb <- function() {
  .ensure_python()
  reticulate::import("lancedb", delay_load = FALSE)
}




#' Get the active LanceDB backend
#'
#' Returns the currently selected backend implementation used by
#' \pkg{lancedbr}. The backend is controlled via the global option
#' \code{lancedbr.backend}.
#'
#' Currently supported values are:
#' \itemize{
#'   \item \code{"reticulate"} – Python-based backend using \pkg{reticulate}
#'   \item \code{"extendr"} – Reserved for a future Rust-based backend
#' }
#'
#' @return A length-one character string naming the active backend.
#'
#' @examples
#' lancedbr_backend()
#' options(lancedbr.backend = "reticulate")
#'
#' @export
lancedbr_backend <- function() {
  backend <- getOption("lancedbr.backend", default = "reticulate")
  backend <- tolower(as.character(backend))

  if (!backend %in% c("reticulate", "extendr")) {
    stop(
      "Unknown backend: ", backend,
      ". Use options(lancedbr.backend = 'reticulate')",
      call. = FALSE
    )
  }
  backend
}


#' Dispatch a backend-specific implementation
#'
#' Internal dispatcher that routes a function call to the active backend
#' implementation based on \code{lancedbr_backend()}.
#'
#' Backend-specific functions must follow the naming convention:
#' \itemize{
#'   \item \code{.rt_<fn>} for the reticulate backend
#'   \item \code{.ex_<fn>} for the extendr backend
#' }
#'
#' This allows the public API to remain stable while backend
#' implementations change.
#'
#' @param fn Character scalar giving the logical function name
#'   (without backend prefix).
#' @param ... Arguments forwarded to the backend implementation.
#'
#' @return The return value of the backend-specific function.
#'
#' @keywords internal
.backend_call <- function(fn, ...) {
  backend <- lancedbr_backend()
  impl <- switch(
    backend,
    reticulate = get0(paste0(".rt_", fn), mode = "function"),
    extendr    = get0(paste0(".ex_", fn), mode = "function")
  )

  if (is.null(impl)) {
    stop(
      "Backend '", backend,
      "' does not implement ", fn, "() yet.",
      call. = FALSE
    )
  }
  impl(...)
}


