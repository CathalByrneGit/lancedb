\name{lancedb_delete}
\alias{lancedb_delete}
\title{Delete Rows from a Table}
\usage{
lancedb_delete(table, predicate)
}
\arguments{
\item{table}{A \code{lancedb_table} object.}
\item{predicate}{SQL predicate string for rows to delete (e.g., \code{"age < 18"}).}
}
\value{
The table (invisibly).
}
\description{
Removes rows from a LanceDB table that match the given predicate expression.
}
\examples{
\dontrun{
lancedb_delete(tbl, "category = 'spam'")
}
}
