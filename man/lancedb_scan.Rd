\name{lancedb_scan}
\alias{lancedb_scan}
\title{Create a Table Scan Query}
\usage{
lancedb_scan(table)
}
\arguments{
\item{table}{A \code{lancedb_table} object.}
}
\value{
A \code{lancedb_lazy} object in \code{"scan"} mode.
}
\description{
Begins a lazy full table scan on a LanceDB table. The query is not executed
until \code{\link[dplyr]{collect}} is called. Use dplyr verbs (\code{filter},
\code{select}, \code{slice_head}) to refine the scan.
}
\examples{
\dontrun{
library(dplyr)

con <- lancedb_connect("/tmp/my_lancedb")
tbl <- lancedb_open_table(con, "my_table")

results <- lancedb_scan(tbl) \%>\%
  filter(age > 30) \%>\%
  select(name, age) \%>\%
  slice_head(n = 100) \%>\%
  collect()
}
}
