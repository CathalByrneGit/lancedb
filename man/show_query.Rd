\name{show_query}
\alias{show_query}
\title{Show the Query Plan}
\usage{
show_query(x)
}
\arguments{
\item{x}{A \code{lancedb_lazy} object.}
}
\value{
The query object (invisibly).
}
\description{
Displays a human-readable representation of the lazy query plan, similar
to \code{dbplyr}'s \code{show_query()}. Shows the query mode, table name, and all
accumulated operations.
}
\examples{
\dontrun{
lancedb_scan(tbl) \%>\%
  filter(age > 30) \%>\%
  select(name, age) \%>\%
  show_query()
}
}
