\name{lancedb-package}
\alias{lancedb-package}
\alias{lancedb}
\docType{package}
\title{R Client for LanceDB Embedded Vector Database}
\description{
Provides an R interface to LanceDB, an embedded vector database for AI
applications. Uses Rust bindings via extendr for high performance. Supports
lazy query building with dplyr-compatible verbs (\code{filter}, \code{select},
\code{slice_head}, \code{arrange}, \code{collect}) for both vector similarity
search and table scans. Queries are built lazily and executed only when
collected, following the same pattern as dbplyr.
}
\section{Main functions}{
\describe{
  \item{\code{\link{lancedb_connect}}}{Connect to a database}
  \item{\code{\link{lancedb_create_table}}}{Create a new table}
  \item{\code{\link{lancedb_open_table}}}{Open an existing table}
  \item{\code{\link{lancedb_search}}}{Start a vector similarity search (lazy)}
  \item{\code{\link{lancedb_scan}}}{Start a full table scan (lazy)}
  \item{\code{\link{show_query}}}{Display the query plan}
}
}
\section{dplyr verbs}{
The following dplyr generics have S3 methods for \code{lancedb_lazy} objects:
\describe{
  \item{\code{\link[dplyr]{filter}}}{Add WHERE filter predicates}
  \item{\code{\link[dplyr]{select}}}{Choose columns to return}
  \item{\code{\link[dplyr]{slice_head}}}{Limit number of rows}
  \item{\code{\link[dplyr]{arrange}}}{Order results (backend-dependent)}
  \item{\code{\link[dplyr]{collect}}}{Execute and return results}
  \item{\code{\link[dplyr]{pull}}}{Execute and extract one column}
}
}
\author{
LanceDB Contributors
}
