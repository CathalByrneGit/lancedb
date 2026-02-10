\name{collect.lancedb_lazy}
\alias{collect.lancedb_lazy}
\title{Execute a LanceDB Lazy Query}
\usage{
\method{collect}{lancedb_lazy}(x, ..., as = c("data.frame", "arrow"))
}
\arguments{
\item{x}{A \code{lancedb_lazy} object.}
\item{...}{Additional arguments (ignored).}
\item{as}{Output format: \code{"data.frame"} (default) or \code{"arrow"} for an
\code{arrow::Table}.}
}
\value{
A \code{data.frame} or \code{arrow::Table} depending on \code{as}.
}
\description{
Terminal method that executes the accumulated query plan against the LanceDB
backend and returns the results. This is where all lazy operations (filter,
select, limit) are actually applied.
}
\examples{
\dontrun{
library(dplyr)

# Collect as data.frame (default)
results <- lancedb_scan(tbl) \%>\%
  filter(age > 30) \%>\%
  select(name, age) \%>\%
  slice_head(n = 10) \%>\%
  collect()

# Collect as Arrow Table
arrow_results <- lancedb_scan(tbl) \%>\%
  filter(age > 30) \%>\%
  collect(as = "arrow")
}
}
