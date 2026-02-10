\name{arrange.lancedb_lazy}
\alias{arrange.lancedb_lazy}
\title{Arrange (Sort) a LanceDB Lazy Query}
\usage{
\method{arrange}{lancedb_lazy}(.data, ...)
}
\arguments{
\item{.data}{A \code{lancedb_lazy} object.}
\item{...}{Column expressions to sort by. Use \code{desc(col)} for descending order.}
}
\value{
A new \code{lancedb_lazy} object with the order appended.
}
\description{
Adds an ORDER BY clause to the query plan. Note: ordering support depends
on the query mode and backend. For \code{"scan"} mode queries, the backend may
not support ordering, in which case an error is raised at execution time.
\code{"search"} mode results are ordered by similarity distance by default.
}
\examples{
\dontrun{
lancedb_scan(tbl) \%>\% arrange(age, desc(name))
}
}
