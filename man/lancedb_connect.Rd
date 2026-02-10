\name{lancedb_connect}
\alias{lancedb_connect}
\title{Connect to a LanceDB Database}
\usage{
lancedb_connect(uri)
}
\arguments{
\item{uri}{Character string. Path to the database directory (e.g.,
\code{"/tmp/my_lancedb"}) or a cloud storage URI.}
}
\value{
A \code{lancedb_connection} object.
}
\description{
Opens a connection to a LanceDB database at the given URI. The URI can be
a local directory path (embedded mode) or a cloud storage path.
}
\examples{
\dontrun{
con <- lancedb_connect("/tmp/my_lancedb")
print(con)
}
}
