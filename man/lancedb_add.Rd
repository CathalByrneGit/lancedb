\name{lancedb_add}
\alias{lancedb_add}
\title{Add Data to a Table}
\usage{
lancedb_add(table, data, mode = c("append", "overwrite"))
}
\arguments{
\item{table}{A \code{lancedb_table} object.}
\item{data}{A \code{data.frame} or \code{arrow::Table}.}
\item{mode}{One of \code{"append"} (default) or \code{"overwrite"}.}
}
\value{
The table (invisibly).
}
\description{
Inserts data into an existing LanceDB table. By default appends rows; use
\code{mode = "overwrite"} to replace existing data.
}
\examples{
\dontrun{
new_data <- data.frame(id = 101:110, name = paste0("new_", 1:10))
lancedb_add(tbl, new_data)
}
}
