\name{lancedb_count_rows}
\alias{lancedb_count_rows}
\title{Count Rows in a Table}
\usage{
lancedb_count_rows(table, filter = NULL)
}
\arguments{
\item{table}{A \code{lancedb_table} object.}
\item{filter}{Optional filter expression string.}
}
\value{
Integer count of rows.
}
\description{
Returns the number of rows in the table, optionally applying a filter predicate.
}
\examples{
\dontrun{
lancedb_count_rows(tbl)
lancedb_count_rows(tbl, filter = "age > 30")
}
}
