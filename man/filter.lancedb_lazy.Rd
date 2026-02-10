\name{filter.lancedb_lazy}
\alias{filter.lancedb_lazy}
\title{Filter a LanceDB Lazy Query}
\usage{
\method{filter}{lancedb_lazy}(.data, ..., .preserve = FALSE)
}
\arguments{
\item{.data}{A \code{lancedb_lazy} object.}
\item{...}{Filter predicates. Can be R expressions (e.g., \code{age > 30}) or
string expressions (e.g., \code{"age > 30"}).}
\item{.preserve}{Ignored (present for dplyr compatibility).}
}
\value{
A new \code{lancedb_lazy} object with the filter appended.
}
\description{
Adds a filter (WHERE) clause to the lazy query plan. Predicates can be
R expressions or string expressions. Multiple predicates are combined with AND.

\strong{Supported R expression syntax:}
\itemize{
  \item Comparisons: \code{==}, \code{!=}, \code{>}, \code{>=}, \code{<}, \code{<=}
  \item Boolean: \code{&} (AND), \code{|} (OR), \code{!} (NOT)
  \item Set membership: \code{\%in\%}
  \item Null check: \code{is.na()}
  \item Range: \code{between(x, lo, hi)}
  \item Nested fields: \code{stats$strength} translates to \code{stats.strength}
}
}
\examples{
\dontrun{
# R expression filter
lancedb_scan(tbl) \%>\% filter(age > 30, name == "Alice")

# String filter (passed through directly)
lancedb_scan(tbl) \%>\% filter("age > 30 AND name = 'Alice'")

# Combined predicates
lancedb_scan(tbl) \%>\% filter(score > 0.5 & category \%in\% c("A", "B"))
}
}
