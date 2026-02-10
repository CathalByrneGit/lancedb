\name{print.lancedb_connection}
\alias{print.lancedb_connection}
\alias{print.lancedb_table}
\alias{print.lancedb_lazy}
\title{Print Methods for LanceDB Objects}
\usage{
\method{print}{lancedb_connection}(x, ...)
\method{print}{lancedb_table}(x, ...)
\method{print}{lancedb_lazy}(x, ...)
}
\arguments{
\item{x}{A LanceDB object (\code{lancedb_connection}, \code{lancedb_table}, or
\code{lancedb_lazy}).}
\item{...}{Ignored.}
}
\value{
The object (invisibly).
}
\description{
Display methods for LanceDB connection, table, and lazy query objects.
\code{print.lancedb_lazy} shows the query plan including mode, table, and
all accumulated operations.
}
