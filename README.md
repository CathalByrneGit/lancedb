# lancedbr

`lancedbr` provides an R interface to [LanceDB](https://lancedb.com), a
vector database built on Apache Arrow.

The package exposes a small, stable R API for creating tables, adding
data, and performing vector similarity search. Internally, it uses the
Python LanceDB client via the `reticulate` package, with a design
intended to support a future native Rust backend.

## Status

This package is **experimental**. The public R API is expected to be
stable, but backend behavior and performance characteristics may change
as the implementation evolves.

## Installation

From source (for now):

```r
# install.packages("devtools")
devtools::install_github("your-org/lancedbr")
```

The first time you use the package, a Python environment may be created
automatically and the Python `lancedb` package installed.

## Basic usage

```r
library(lancedbr)

# Connect to a database (local directory)
con <- lancedb_connect("~/mydb")

# Create a table
data <- data.frame(
  id = 1:3,
  text = c("a", "b", "c"),
  vector = I(list(
    c(0.1, 0.2),
    c(0.2, 0.3),
    c(0.4, 0.5)
  ))
)

tbl <- lancedb_create_table(con, "example", data)

# Add more data
tbl_add(tbl, data)

# Vector search
res <- tbl_search(
  tbl,
  query = c(0.1, 0.2),
  k = 2
)

res
```

### Vector columns

Vector data must be provided as a **list-column of numeric vectors with a
fixed length per row**. This is required for vector indexing and search
in LanceDB.

When a table is created or data are added from an R `data.frame`,
`lancedbr` ingests vector columns using Apache Arrow with a fixed-size
list type. This conversion happens automatically; users do not need to
construct Arrow objects themselves.

## Arrow output

Search results can be returned as an Arrow table if the `arrow` package
is installed:

```r
res_arrow <- tbl_search(
  tbl,
  query = c(0.1, 0.2),
  k = 2,
  as = "arrow"
)
```

Arrow output avoids unnecessary data copying and is the recommended
format for larger result sets.

Note that Arrow is also used internally when ingesting vector data,
even if results are returned as data frames.

## Python configuration

By default, `lancedbr` manages its own Python environment using
`reticulate`. Advanced users can override this behavior via environment
variables:

* `LANCEDBR_PYTHON`: path to a specific Python executable
* `LANCEDBR_CONDA_ENV`: name of a conda environment to use

Example:

```r
Sys.setenv(LANCEDBR_CONDA_ENV = "myenv")
```

These variables should be set **before** loading the package.

## Diagnostics

To inspect the active backend, Python configuration, and installed
Python package versions:

```r
lancedbr_info()
```

This is useful when reporting issues.

## Backend design

The package is intentionally backend-agnostic. All user-facing
functions dispatch to an internal backend layer. The current backend
uses `reticulate`; a future version may use native Rust bindings without
changing the R API.

## License

Apache License 2.0

```



