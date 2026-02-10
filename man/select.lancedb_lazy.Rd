\name{select.lancedb_lazy}
\alias{select.lancedb_lazy}
\title{Select Columns from a LanceDB Lazy Query}
\usage{
\method{select}{lancedb_lazy}(.data, ...)
}
\arguments{
\item{.data}{A \code{lancedb_lazy} object.}
\item{...}{Column names (unquoted or character strings).}
}
\value{
A new \code{lancedb_lazy} object with the selection appended.
}
\description{
Specifies which columns to return from the query. Currently supports bare
column names only (tidyselect helpers are not yet supported).
}
\examples{
\dontrun{
lancedb_scan(tbl) \%>\% select(name, age, score)
}
}
