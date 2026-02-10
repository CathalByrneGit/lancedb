\name{lancedb_search}
\alias{lancedb_search}
\title{Create a Vector Search Query}
\usage{
lancedb_search(table, query_vector)
}
\arguments{
\item{table}{A \code{lancedb_table} object.}
\item{query_vector}{A numeric vector to use as the search query.}
}
\value{
A \code{lancedb_lazy} object in \code{"search"} mode.
}
\description{
Begins a lazy vector similarity search on a LanceDB table. The query is not
executed until \code{\link[dplyr]{collect}} is called. Use dplyr verbs
(\code{filter}, \code{select}, \code{slice_head}) to refine the query.

Results include a \code{_distance} column with the similarity score.
}
\examples{
\dontrun{
library(dplyr)

con <- lancedb_connect("/tmp/my_lancedb")
tbl <- lancedb_open_table(con, "embeddings")

results <- lancedb_search(tbl, runif(128)) \%>\%
  filter(category == "science") \%>\%
  select(title, category) \%>\%
  slice_head(n = 10) \%>\%
  collect()
}
}
