\name{lancedb_open_table}
\alias{lancedb_open_table}
\title{Open an Existing LanceDB Table}
\usage{
lancedb_open_table(con, name)
}
\arguments{
\item{con}{A \code{lancedb_connection} object.}
\item{name}{Character string. Name of the table to open.}
}
\value{
A \code{lancedb_table} object.
}
\description{
Opens a table by name from an existing LanceDB connection.
}
\examples{
\dontrun{
con <- lancedb_connect("/tmp/my_lancedb")
tbl <- lancedb_open_table(con, "my_table")
}
}
