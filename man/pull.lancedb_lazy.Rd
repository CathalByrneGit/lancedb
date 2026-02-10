\name{pull.lancedb_lazy}
\alias{pull.lancedb_lazy}
\title{Pull a Single Column from a LanceDB Lazy Query}
\usage{
\method{pull}{lancedb_lazy}(.data, var = -1, name = NULL, ...)
}
\arguments{
\item{.data}{A \code{lancedb_lazy} object.}
\item{var}{Column to extract. Can be an unquoted column name, a string, or
an integer position (negative counts from the right).}
\item{name}{Not supported (present for dplyr compatibility).}
\item{...}{Ignored.}
}
\value{
A vector containing the values of the requested column.
}
\description{
Convenience terminal that collects the query results and extracts a single
column as a vector. Equivalent to \code{collect(.data) |> dplyr::pull(var)}.
}
\examples{
\dontrun{
# Pull by name
lancedb_scan(tbl) \%>\% pull(name)

# Pull last column
lancedb_scan(tbl) \%>\% pull(-1)
}
}
