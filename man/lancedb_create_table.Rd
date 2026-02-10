\name{lancedb_create_table}
\alias{lancedb_create_table}
\title{Create a New LanceDB Table}
\usage{
lancedb_create_table(con, name, data, mode = c("create", "overwrite"))
}
\arguments{
\item{con}{A \code{lancedb_connection} object.}
\item{name}{Character string. Name for the new table.}
\item{data}{A \code{data.frame} or \code{arrow::Table} to use as initial data.}
\item{mode}{Character string. One of \code{"create"} (error if exists) or
\code{"overwrite"} (replace if exists). Default is \code{"create"}.}
}
\value{
A \code{lancedb_table} object.
}
\description{
Creates a table in the database from a data.frame or Arrow Table.
}
\examples{
\dontrun{
con <- lancedb_connect("/tmp/my_lancedb")
df <- data.frame(id = 1:3, text = c("a", "b", "c"))
tbl <- lancedb_create_table(con, "my_table", df)
}
}
