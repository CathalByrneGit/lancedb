#' @useDynLib lancedb, .registration = TRUE
NULL

.onLoad <- function(libname, pkgname) {
  # Register S3 methods for dplyr generics
  vctrs_method_register <- function(generic, class) {
    stopifnot(is.character(generic), length(generic) == 1)
    stopifnot(is.character(class), length(class) == 1)

    pieces <- strsplit(generic, "::")[[1]]
    stopifnot(length(pieces) == 2)
    package <- pieces[[1]]
    name <- pieces[[2]]

    method <- get(paste0(name, ".", class), envir = parent.env(environment()))
    registerS3method(name, class, method, envir = asNamespace(package))
  }

  vctrs_method_register("dplyr::filter", "lancedb_lazy")
  vctrs_method_register("dplyr::select", "lancedb_lazy")
  vctrs_method_register("dplyr::slice_head", "lancedb_lazy")
  vctrs_method_register("dplyr::arrange", "lancedb_lazy")
  vctrs_method_register("dplyr::collect", "lancedb_lazy")
  vctrs_method_register("dplyr::pull", "lancedb_lazy")
}
