\name{slice_head.lancedb_lazy}
\alias{slice_head.lancedb_lazy}
\alias{head.lancedb_lazy}
\title{Limit Results from a LanceDB Lazy Query}
\usage{
\method{slice_head}{lancedb_lazy}(.data, n, prop = NULL, by = NULL)

\method{head}{lancedb_lazy}(x, n = 6L, ...)
}
\arguments{
\item{.data, x}{A \code{lancedb_lazy} object.}
\item{n}{Integer. Number of rows to return.}
\item{prop}{Not supported (present for dplyr compatibility).}
\item{by}{Not supported (present for dplyr compatibility).}
\item{...}{Ignored.}
}
\value{
A new \code{lancedb_lazy} object with the limit appended.
}
\description{
Adds a LIMIT clause to the query plan, restricting the number of rows
returned. \code{head()} is a convenience alias.
}
\examples{
\dontrun{
lancedb_scan(tbl) \%>\% slice_head(n = 10)

# Equivalent
head(lancedb_scan(tbl), 10)
}
}
