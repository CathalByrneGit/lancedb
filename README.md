# lancedb

R package for [LanceDB](https://lancedb.com/) — an embedded vector database for AI applications.

Built on **Rust bindings** via [extendr](https://extendr.github.io/), with a **lazy query API** that interoperates with **dplyr verbs** (`filter`, `select`, `arrange`, `slice_head`, `collect`). Queries build a plan; nothing executes until `collect()`.

## Installation

Requires Rust toolchain (cargo >= 1.70).

```r
# Install from source (development)
# install.packages("remotes")
remotes::install_github("lancedb/lancedb", subdir = "r")
```

## Quick Start

```r
library(lancedb)
library(dplyr)

# Connect to a local database
con <- lancedb_connect("/tmp/my_lancedb")

# Create a table from a data.frame
df <- data.frame(
  id    = 1:100,
  name  = paste0("item_", 1:100),
  score = runif(100),
  vec   = I(lapply(1:100, function(i) rnorm(4)))
)
tbl <- lancedb_create_table(con, "items", df)

# Full table scan with dplyr verbs
results <- lancedb_scan(tbl) %>%
  filter(score > 0.5) %>%
  select(id, name, score) %>%
  slice_head(n = 10) %>%
  collect()

print(results)
```

## Vector Search

```r
# Search by vector similarity
query_vec <- rnorm(4)

results <- lancedb_search(tbl, query_vec) %>%
  filter(score > 0.3) %>%
  select(id, name, score) %>%
  slice_head(n = 5) %>%
  collect()
```

## API Reference

### Connection

| Function | Description |
|---|---|
| `lancedb_connect(uri)` | Connect to a LanceDB database (local path or cloud URI) |
| `lancedb_list_tables(con)` | List all table names in the database |
| `lancedb_drop_table(con, name)` | Permanently drop a table from the database |

### Table Creation & Opening

| Function | Description |
|---|---|
| `lancedb_create_table(con, name, data, mode)` | Create a new table from a data.frame or Arrow Table |
| `lancedb_open_table(con, name)` | Open an existing table |

### Table Info

| Function | Description |
|---|---|
| `lancedb_count_rows(table, filter)` | Count rows (optionally with a filter) |
| `lancedb_schema(table)` | Get table schema as a data.frame (name, type, nullable) |
| `lancedb_version(table)` | Get current table version number |
| `lancedb_head(table, n)` | Preview first n rows as a data.frame |
| `lancedb_to_arrow(table)` | Read entire table as an Arrow Table |

### Data Modification

| Function | Description |
|---|---|
| `lancedb_add(table, data, mode)` | Add (append/overwrite) data to an existing table |
| `lancedb_delete(table, predicate)` | Delete rows matching a SQL predicate |
| `lancedb_update(table, values, where)` | Update column values using SQL expressions |
| `lancedb_merge_insert(table, data, on, ...)` | Upsert / insert-if-not-exists / replace-range |

### Indexing

| Function | Description |
|---|---|
| `lancedb_create_index(table, columns, index_type, ...)` | Create scalar, vector, or FTS index |
| `lancedb_list_indices(table)` | List all indices on a table |
| `lancedb_drop_index(table, name)` | Drop an index by name |

### Schema Evolution

| Function | Description |
|---|---|
| `lancedb_add_columns(table, transforms)` | Add new computed columns via SQL expressions |
| `lancedb_alter_columns(table, alterations)` | Rename columns or change nullability |
| `lancedb_drop_columns(table, columns)` | Permanently remove columns |

### Versioning (Time Travel)

| Function | Description |
|---|---|
| `lancedb_list_versions(table)` | List all versions with timestamps |
| `lancedb_checkout(table, version)` | Checkout a specific past version (read-only) |
| `lancedb_checkout_latest(table)` | Return to the latest version |
| `lancedb_restore(table)` | Restore the checked-out version as a new version |

### Optimization

| Function | Description |
|---|---|
| `lancedb_compact_files(table)` | Merge small files for better read performance |
| `lancedb_cleanup_old_versions(table, older_than_days)` | Prune old versions |

### Query Constructors

| Function | Description |
|---|---|
| `lancedb_search(table, query_vector, ...)` | Start a lazy vector similarity search (supports `nprobes`, `refine_factor`, `ef`, `column`, `distance_type`, `bypass_vector_index`, `distance_range`) |
| `lancedb_fts_search(table, query_text, columns)` | Start a lazy full-text search (BM25) |
| `lancedb_hybrid_search(table, query_vector, query_text, ...)` | Start a lazy hybrid search (vector + FTS with reranking) |
| `lancedb_scan(table)` | Start a lazy full table scan |

### Query Modifiers (on `lancedb_lazy` objects)

| Function | Description |
|---|---|
| `lancedb_offset(.data, n)` | Skip the first n rows (pagination) |
| `lancedb_postfilter(.data)` | Apply filters after search (ensures full index usage) |
| `lancedb_fast_search(.data)` | Enable approximate faster search |
| `lancedb_with_row_id(.data)` | Include internal `_rowid` column in results |
| `lancedb_select_exprs(.data, ...)` | Project computed columns using SQL expressions (DataFusion) |

### dplyr Verbs (on `lancedb_lazy` objects)

| Verb | Description |
|---|---|
| `filter(.data, ...)` | Add a WHERE clause (R expressions or strings) |
| `select(.data, ...)` | Choose columns to return |
| `slice_head(.data, n)` | Limit number of results |
| `arrange(.data, ...)` | Not yet supported (errors with guidance) |
| `collect(x, as)` | Execute the query and return results |
| `pull(.data, var)` | Execute and extract a single column |
| `head(x, n)` | Alias for `slice_head(n = ...)` |

### Utilities

| Function | Description |
|---|---|
| `show_query(x)` | Display the query plan |

## Examples

### Update Rows

```r
# Set all scores to 0
lancedb_update(tbl, values = list(score = "0"))

# Update matching rows with a computed value
lancedb_update(tbl, values = list(score = "score * 2"), where = "category = 'A'")
```

### Upsert (Merge Insert)

```r
new_data <- data.frame(id = c(1, 999), name = c("updated", "new_item"))

# Upsert: update existing, insert new
lancedb_merge_insert(tbl, new_data, on = "id")

# Insert only if not exists
lancedb_merge_insert(tbl, new_data, on = "id", when_matched_update_all = FALSE)
```

### Indexing

```r
# Scalar index for fast filtering
lancedb_create_index(tbl, "category", index_type = "btree")

# Vector index with custom parameters
lancedb_create_index(tbl, "vec", index_type = "ivf_pq",
                     metric = "cosine", num_partitions = 256,
                     num_sub_vectors = 16)

# HNSW vector index (high recall)
lancedb_create_index(tbl, "vec", index_type = "ivf_hnsw_sq",
                     metric = "cosine", m = 20, ef_construction = 300)

# Full-text search with stemming + phrase support
lancedb_create_index(tbl, "text", index_type = "fts",
                     with_position = TRUE, language = "English",
                     stem = TRUE, remove_stop_words = TRUE)

# FTS with n-gram tokenizer for substring matching
lancedb_create_index(tbl, "text", index_type = "fts",
                     base_tokenizer = "ngram")

# List and drop indices
lancedb_list_indices(tbl)
lancedb_drop_index(tbl, "my_index")
```

### Full-Text Search

```r
# Create FTS index first
lancedb_create_index(tbl, "content", index_type = "fts",
                     with_position = TRUE, stem = TRUE)

# Search for terms
results <- lancedb_fts_search(tbl, "machine learning") %>%
  select(title, content) %>%
  slice_head(n = 10) %>%
  collect()

# Search specific columns
results <- lancedb_fts_search(tbl, "neural networks",
                               columns = c("title", "abstract")) %>%
  collect()
```

### Tuned Vector Search

```r
# Search with more IVF probes for higher recall
results <- lancedb_search(tbl, query_vec,
                          nprobes = 50,
                          refine_factor = 10) %>%
  slice_head(n = 20) %>%
  collect()

# Search specific vector column with HNSW ef parameter
results <- lancedb_search(tbl, query_vec,
                          column = "embedding",
                          ef = 64,
                          distance_type = "cosine") %>%
  collect()

# Exhaustive flat search (no index)
results <- lancedb_search(tbl, query_vec,
                          bypass_vector_index = TRUE) %>%
  collect()

# Distance range filtering
results <- lancedb_search(tbl, query_vec,
                          distance_range = c(0.0, 0.5)) %>%
  collect()
```

### Hybrid Search (Vector + Full-Text)

```r
# Create both vector and FTS indices
lancedb_create_index(tbl, "vec", index_type = "ivf_pq", metric = "cosine")
lancedb_create_index(tbl, "content", index_type = "fts",
                     with_position = TRUE, stem = TRUE)

# Hybrid search: combines vector similarity and text relevance
results <- lancedb_hybrid_search(tbl, query_vec, "machine learning") %>%
  select(title, content) %>%
  slice_head(n = 10) %>%
  collect()

# Score-based normalization (instead of default RRF)
results <- lancedb_hybrid_search(tbl, query_vec, "neural networks",
                                  norm = "score",
                                  column = "embedding",
                                  distance_type = "cosine") %>%
  collect()

# Restrict FTS to specific columns
results <- lancedb_hybrid_search(tbl, query_vec, "deep learning",
                                  fts_columns = c("title", "abstract")) %>%
  collect()
```

### Query Modifiers

```r
# Pagination with offset
page2 <- lancedb_scan(tbl) %>%
  lancedb_offset(10) %>%
  slice_head(n = 10) %>%
  collect()

# Post-filtering: search first, then filter
results <- lancedb_search(tbl, query_vec) %>%
  filter(category == "science") %>%
  lancedb_postfilter() %>%
  slice_head(n = 10) %>%
  collect()

# Fast approximate search
results <- lancedb_search(tbl, query_vec) %>%
  lancedb_fast_search() %>%
  slice_head(n = 10) %>%
  collect()

# Include internal row IDs
results <- lancedb_scan(tbl) %>%
  lancedb_with_row_id() %>%
  slice_head(n = 10) %>%
  collect()
```

### Computed Column Projections (SQL Expressions)

Unlike `select()` which picks existing columns by name, `lancedb_select_exprs()`
projects new columns computed by SQL expressions evaluated by DataFusion.

```r
# Compute derived columns from a scan
results <- lancedb_scan(tbl) %>%
  lancedb_select_exprs(
    score_pct  = "score * 100",
    name_upper = "upper(name)",
    id_str     = "CAST(id AS VARCHAR)"
  ) %>%
  slice_head(n = 10) %>%
  collect()

# Normalise the _distance column after a vector search
results <- lancedb_search(tbl, query_vec) %>%
  lancedb_select_exprs(
    name      = "name",
    category  = "category",
    norm_dist = "_distance / 100"
  ) %>%
  slice_head(n = 5) %>%
  collect()

# Conditional band classification
results <- lancedb_scan(tbl) %>%
  filter(score > 0) %>%
  lancedb_select_exprs(
    name  = "name",
    band  = "CASE WHEN score > 0.8 THEN 'high' WHEN score > 0.5 THEN 'mid' ELSE 'low' END"
  ) %>%
  collect()
```

### Schema Evolution

```r
# Add a computed column
lancedb_add_columns(tbl, list(score_doubled = "score * 2"))

# Rename a column
lancedb_alter_columns(tbl, list(list(path = "old_name", rename = "new_name")))

# Drop columns
lancedb_drop_columns(tbl, c("temp_col", "debug_info"))
```

### Versioning (Time Travel)

```r
# Check current version
lancedb_version(tbl)

# List version history
lancedb_list_versions(tbl)

# Travel back in time
lancedb_checkout(tbl, version = 1)
old_data <- lancedb_head(tbl)

# Return to latest
lancedb_checkout_latest(tbl)

# Or restore old version as new current version
lancedb_checkout(tbl, version = 1)
lancedb_restore(tbl)
```

### Table Optimization

```r
# Compact small files after many appends
lancedb_compact_files(tbl)

# Remove old versions older than 30 days
lancedb_cleanup_old_versions(tbl, older_than_days = 30)
```

## Filter Expressions

Filters can be R expressions or raw strings:

```r
# R expression — translated automatically
lancedb_scan(tbl) %>% filter(age > 30, name == "Alice")

# String filter — passed directly to LanceDB
lancedb_scan(tbl) %>% filter("age > 30 AND name = 'Alice'")
```

### Supported R expression syntax

| R | LanceDB |
|---|---|
| `==` | `=` |
| `!=` | `!=` |
| `>`, `>=`, `<`, `<=` | same |
| `&` | `AND` |
| `|` | `OR` |
| `!` | `NOT` |
| `%in% c(...)` | `IN (...)` |
| `is.na(x)` | `x IS NULL` |
| `between(x, lo, hi)` | `x BETWEEN lo AND hi` |
| `stats$strength` | `stats.strength` |

## Python Comparison

The R API mirrors the Python LanceDB SDK:

**Python:**
```python
table.search(qvec)
  .where("stats.strength > 3")
  .select(["name", "role", "description"])
  .limit(5)
  .to_pandas()
```

**R:**
```r
lancedb_search(tbl, qvec) %>%
  filter(stats$strength > 3) %>%
  select(name, role, description) %>%
  slice_head(n = 5) %>%
  collect()
```

## Architecture

```
R/                    S3 classes + dplyr methods
src/rust/             Rust bindings (extendr + lancedb crate)
tests/testthat/       Unit tests
man/                  Documentation (generated by roxygen2)
```

The package uses a lazy evaluation model inspired by dbplyr:

1. `lancedb_search()`, `lancedb_fts_search()`, `lancedb_hybrid_search()`, or `lancedb_scan()` creates a `lancedb_lazy` object
2. dplyr verbs (`filter`, `select`, `slice_head`) and query modifiers (`lancedb_offset`, `lancedb_postfilter`, etc.) append operations to the plan
3. `collect()` serializes the plan, sends it to Rust, executes against LanceDB, and returns results via Arrow IPC

All verb calls return **new** `lancedb_lazy` objects — the original is never mutated.

## Return Formats

```r
# Default: data.frame
df <- lancedb_scan(tbl) %>% collect()

# Arrow Table
arrow_tbl <- lancedb_scan(tbl) %>% collect(as = "arrow")
```

## Requirements

- R >= 4.0
- Rust >= 1.70 — install via [rustup.rs](https://rustup.rs/)
- `protoc` (Protocol Buffers compiler) — required by LanceDB's Rust crate:
  - Debian/Ubuntu: `sudo apt-get install -y protobuf-compiler`
  - macOS: `brew install protobuf`
  - Windows: `choco install protoc`
- System dependencies: same as the `arrow` R package

## License

Apache License 2.0
