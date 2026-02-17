use arrow_ipc::writer::StreamWriter;
use extendr_api::prelude::*;
use futures::TryStreamExt;

// ExecutableQuery and QueryBase are required as trait bounds for
// .execute(), .only_if(), .limit(), and .select() on query builders.
use lancedb::query::{ExecutableQuery, FullTextSearchQuery, QueryBase, Select};

mod runtime;
use runtime::get_runtime;

// ---------------------------------------------------------------------------
// Connection wrapper
// ---------------------------------------------------------------------------

/// An opaque handle to a LanceDB connection.
struct LanceConnection {
    inner: lancedb::Connection,
}

impl LanceConnection {
    fn uri(&self) -> String {
        self.inner.uri().to_string()
    }
}

/// Connect to a LanceDB database at the given URI (directory path or cloud URI).
#[extendr]
fn rust_connect(uri: &str) -> ExternalPtr<LanceConnection> {
    let rt = get_runtime();
    let conn = rt
        .block_on(lancedb::connect(uri).execute())
        .expect("Failed to connect to LanceDB");
    ExternalPtr::new(LanceConnection { inner: conn })
}

/// Return the URI of an open connection.
#[extendr]
fn rust_connection_uri(conn: &LanceConnection) -> String {
    conn.uri()
}

/// List table names in a connection.
#[extendr]
fn rust_table_names(conn: &LanceConnection) -> Vec<String> {
    let rt = get_runtime();
    rt.block_on(conn.inner.table_names().execute())
        .expect("Failed to list table names")
}

/// Drop a table from the database.
#[extendr]
fn rust_drop_table(conn: &LanceConnection, name: &str) {
    let rt = get_runtime();
    rt.block_on(conn.inner.drop_table(name, &[]))
        .expect("Failed to drop table");
}

// ---------------------------------------------------------------------------
// Table wrapper
// ---------------------------------------------------------------------------

struct LanceTable {
    inner: lancedb::Table,
}

/// Open an existing table by name.
#[extendr]
fn rust_open_table(conn: &LanceConnection, name: &str) -> ExternalPtr<LanceTable> {
    let rt = get_runtime();
    let table = rt
        .block_on(conn.inner.open_table(name).execute())
        .expect("Failed to open table");
    ExternalPtr::new(LanceTable { inner: table })
}

/// Create a table from Arrow IPC bytes (serialised RecordBatch stream from R).
#[extendr]
fn rust_create_table(
    conn: &LanceConnection,
    name: &str,
    ipc_bytes: Raw,
    mode: &str,
) -> ExternalPtr<LanceTable> {
    let rt = get_runtime();
    let (schema, batches) = ipc_to_batches(ipc_bytes.as_slice());
    let batch_reader =
        arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    let mut builder = conn.inner.create_table(name, Box::new(batch_reader));
    if mode == "overwrite" {
        builder = builder.mode(lancedb::connection::CreateTableMode::Overwrite);
    }

    let table = rt
        .block_on(builder.execute())
        .expect("Failed to create table");
    ExternalPtr::new(LanceTable { inner: table })
}

/// Return the table name.
#[extendr]
fn rust_table_name(table: &LanceTable) -> String {
    table.inner.name().to_string()
}

/// Return the schema as a JSON string.
#[extendr]
fn rust_table_schema_json(table: &LanceTable) -> String {
    let rt = get_runtime();
    let schema = rt.block_on(table.inner.schema()).expect("Failed to get schema");
    let fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable()
            })
        })
        .collect();
    serde_json::to_string(&fields).unwrap()
}

/// Count rows in the table, optionally with a filter.
#[extendr]
fn rust_count_rows(table: &LanceTable, filter: Nullable<String>) -> i64 {
    let rt = get_runtime();
    let f = match filter {
        Nullable::NotNull(s) => Some(s),
        _ => None,
    };
    rt.block_on(table.inner.count_rows(f))
        .expect("Failed to count rows") as i64
}

// ---------------------------------------------------------------------------
// Data modification
// ---------------------------------------------------------------------------

/// Add data to an existing table from Arrow IPC bytes.
#[extendr]
fn rust_add_data(table: &LanceTable, ipc_bytes: Raw, mode: &str) {
    let rt = get_runtime();
    let (schema, batches) = ipc_to_batches(ipc_bytes.as_slice());
    let batch_reader =
        arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    let mut builder = table.inner.add(Box::new(batch_reader));
    if mode == "overwrite" {
        builder = builder.mode(lancedb::table::AddDataMode::Overwrite);
    }

    rt.block_on(builder.execute())
        .expect("Failed to add data to table");
}

/// Delete rows matching a predicate.
#[extendr]
fn rust_delete_rows(table: &LanceTable, predicate: &str) {
    let rt = get_runtime();
    rt.block_on(table.inner.delete(predicate))
        .expect("Failed to delete rows");
}

/// Update rows matching an optional predicate with SQL value expressions.
/// `where_clause`: SQL WHERE filter (empty string = all rows)
/// `columns_json`: JSON object mapping column names to SQL expressions
#[extendr]
fn rust_update_rows(table: &LanceTable, where_clause: &str, columns_json: &str) {
    let rt = get_runtime();
    let columns: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(columns_json).expect("Invalid columns JSON");

    let mut builder = table.inner.update();

    if !where_clause.is_empty() {
        builder = builder.only_if(where_clause);
    }

    for (col, val) in &columns {
        let expr = val.as_str().unwrap();
        builder = builder.column(col, expr);
    }

    rt.block_on(builder.execute())
        .expect("Failed to update rows");
}

/// Merge insert (upsert) data into a table.
/// `on_columns`: column names to join on
/// `ipc_bytes`: new data as Arrow IPC stream
/// `when_matched_update_all`: update all columns on match
/// `when_not_matched_insert_all`: insert new rows when not matched
/// `when_not_matched_by_source_delete`: delete target rows not in source
#[extendr]
fn rust_merge_insert(
    table: &LanceTable,
    on_columns: Vec<String>,
    ipc_bytes: Raw,
    when_matched_update_all: bool,
    when_not_matched_insert_all: bool,
    when_not_matched_by_source_delete: bool,
) {
    let rt = get_runtime();
    let (schema, batches) = ipc_to_batches(ipc_bytes.as_slice());
    let batch_reader =
        arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    let on_refs: Vec<&str> = on_columns.iter().map(|s| s.as_str()).collect();
    let mut builder = table.inner.merge_insert(&on_refs);

    if when_matched_update_all {
        builder = builder.when_matched_update_all(None);
    }
    if when_not_matched_insert_all {
        builder = builder.when_not_matched_insert_all();
    }
    if when_not_matched_by_source_delete {
        builder = builder.when_not_matched_by_source_delete(None);
    }

    rt.block_on(builder.execute(Box::new(batch_reader)))
        .expect("Failed to merge insert");
}

// ---------------------------------------------------------------------------
// Indexing
// ---------------------------------------------------------------------------

/// Helper: parse distance metric string to DistanceType.
fn parse_distance_type(metric: &str) -> lancedb::DistanceType {
    match metric.to_lowercase().as_str() {
        "cosine" => lancedb::DistanceType::Cosine,
        "dot" => lancedb::DistanceType::Dot,
        _ => lancedb::DistanceType::L2,
    }
}

/// Create an index on the table with full configuration.
///
/// `index_type`: "auto", "btree", "bitmap", "label_list", "fts",
///               "ivf_pq", "ivf_flat", "ivf_hnsw_pq", "ivf_hnsw_sq"
/// `config_json`: JSON object with type-specific configuration:
///   Vector indices: metric, num_partitions, num_sub_vectors, num_bits,
///                   sample_rate, max_iterations, m, ef_construction
///   FTS indices:    with_position, base_tokenizer, language, stem,
///                   remove_stop_words, ascii_folding, max_token_length,
///                   lower_case
#[extendr]
fn rust_create_index(
    table: &LanceTable,
    columns: Vec<String>,
    index_type: &str,
    replace: bool,
    config_json: &str,
) {
    let rt = get_runtime();
    let cfg: serde_json::Value =
        serde_json::from_str(config_json).unwrap_or(serde_json::json!({}));

    let metric_str = cfg
        .get("metric")
        .and_then(|v| v.as_str())
        .unwrap_or("l2");
    let dist = parse_distance_type(metric_str);

    // Helper closures for reading optional config values
    let get_u32 = |key: &str| -> Option<u32> {
        cfg.get(key).and_then(|v| v.as_u64()).map(|n| n as u32)
    };

    let index = match index_type.to_lowercase().as_str() {
        "btree" => lancedb::index::Index::BTree(Default::default()),
        "bitmap" => lancedb::index::Index::Bitmap(Default::default()),
        "label_list" => lancedb::index::Index::LabelList(Default::default()),
        "fts" => {
            let mut fts = lancedb::index::scalar::FtsIndexBuilder::default();
            if let Some(wp) = cfg.get("with_position").and_then(|v| v.as_bool()) {
                fts = fts.with_position(wp);
            }
            if let Some(bt) = cfg.get("base_tokenizer").and_then(|v| v.as_str()) {
                fts = fts.base_tokenizer(bt.to_string());
            }
            if let Some(lang) = cfg.get("language").and_then(|v| v.as_str()) {
                fts = fts.language(lang.to_string());
            }
            if let Some(s) = cfg.get("stem").and_then(|v| v.as_bool()) {
                fts = fts.stem(s);
            }
            if let Some(rsw) = cfg.get("remove_stop_words").and_then(|v| v.as_bool()) {
                fts = fts.remove_stop_words(rsw);
            }
            if let Some(af) = cfg.get("ascii_folding").and_then(|v| v.as_bool()) {
                fts = fts.ascii_folding(af);
            }
            if let Some(mtl) = get_u32("max_token_length") {
                fts = fts.max_token_length(mtl);
            }
            if let Some(lc) = cfg.get("lower_case").and_then(|v| v.as_bool()) {
                fts = fts.lower_case(lc);
            }
            lancedb::index::Index::FTS(fts)
        }
        "ivf_pq" => {
            let mut b =
                lancedb::index::vector::IvfPqIndexBuilder::default().distance_type(dist);
            if let Some(np) = get_u32("num_partitions") {
                b = b.num_partitions(np);
            }
            if let Some(nsv) = get_u32("num_sub_vectors") {
                b = b.num_sub_vectors(nsv);
            }
            if let Some(nb) = get_u32("num_bits") {
                b = b.num_bits(nb);
            }
            if let Some(sr) = get_u32("sample_rate") {
                b = b.sample_rate(sr);
            }
            if let Some(mi) = get_u32("max_iterations") {
                b = b.max_iterations(mi);
            }
            lancedb::index::Index::IvfPq(b)
        }
        "ivf_flat" => {
            let mut b =
                lancedb::index::vector::IvfFlatIndexBuilder::default().distance_type(dist);
            if let Some(np) = get_u32("num_partitions") {
                b = b.num_partitions(np);
            }
            if let Some(sr) = get_u32("sample_rate") {
                b = b.sample_rate(sr);
            }
            if let Some(mi) = get_u32("max_iterations") {
                b = b.max_iterations(mi);
            }
            lancedb::index::Index::IvfFlat(b)
        }
        "ivf_hnsw_pq" => {
            let mut b = lancedb::index::vector::IvfHnswPqIndexBuilder::default()
                .distance_type(dist);
            if let Some(np) = get_u32("num_partitions") {
                b = b.num_partitions(np);
            }
            if let Some(nsv) = get_u32("num_sub_vectors") {
                b = b.num_sub_vectors(nsv);
            }
            if let Some(nb) = get_u32("num_bits") {
                b = b.num_bits(nb);
            }
            if let Some(sr) = get_u32("sample_rate") {
                b = b.sample_rate(sr);
            }
            if let Some(mi) = get_u32("max_iterations") {
                b = b.max_iterations(mi);
            }
            if let Some(m) = get_u32("m") {
                b = b.num_edges(m);
            }
            if let Some(efc) = get_u32("ef_construction") {
                b = b.ef_construction(efc);
            }
            lancedb::index::Index::IvfHnswPq(b)
        }
        "ivf_hnsw_sq" => {
            let mut b = lancedb::index::vector::IvfHnswSqIndexBuilder::default()
                .distance_type(dist);
            if let Some(np) = get_u32("num_partitions") {
                b = b.num_partitions(np);
            }
            if let Some(sr) = get_u32("sample_rate") {
                b = b.sample_rate(sr);
            }
            if let Some(mi) = get_u32("max_iterations") {
                b = b.max_iterations(mi);
            }
            if let Some(m) = get_u32("m") {
                b = b.num_edges(m);
            }
            if let Some(efc) = get_u32("ef_construction") {
                b = b.ef_construction(efc);
            }
            lancedb::index::Index::IvfHnswSq(b)
        }
        _ => lancedb::index::Index::Auto,
    };

    let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
    let mut builder = table.inner.create_index(&col_refs, index);
    if replace {
        builder = builder.replace(true);
    }

    rt.block_on(builder.execute())
        .expect("Failed to create index");
}

/// List all indices on the table. Returns JSON array.
#[extendr]
fn rust_list_indices(table: &LanceTable) -> String {
    let rt = get_runtime();
    let indices = rt
        .block_on(table.inner.list_indices())
        .expect("Failed to list indices");

    let result: Vec<serde_json::Value> = indices
        .iter()
        .map(|idx| {
            serde_json::json!({
                "name": idx.name,
                "index_type": format!("{:?}", idx.index_type),
                "columns": idx.columns,
            })
        })
        .collect();
    serde_json::to_string(&result).unwrap()
}

/// Drop an index by name.
#[extendr]
fn rust_drop_index(table: &LanceTable, name: &str) {
    let rt = get_runtime();
    rt.block_on(table.inner.drop_index(name))
        .expect("Failed to drop index");
}

// ---------------------------------------------------------------------------
// Schema evolution
// ---------------------------------------------------------------------------

/// Add columns using SQL expressions.
/// `transforms_json`: JSON object {"col_name": "sql_expr", ...}
#[extendr]
fn rust_add_columns(table: &LanceTable, transforms_json: &str) {
    let rt = get_runtime();
    let transforms: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(transforms_json).expect("Invalid transforms JSON");

    let pairs: Vec<(String, String)> = transforms
        .iter()
        .map(|(name, expr)| (name.clone(), expr.as_str().unwrap().to_string()))
        .collect();

    let pair_refs: Vec<(&str, &str)> = pairs
        .iter()
        .map(|(n, e)| (n.as_str(), e.as_str()))
        .collect();

    let transform = lancedb::table::NewColumnTransform::SqlExpressions(pair_refs);

    rt.block_on(table.inner.add_columns(transform, None))
        .expect("Failed to add columns");
}

/// Alter columns (rename, change nullability).
/// `alterations_json`: JSON array [{"path":"col","rename":"new_name","nullable":true}]
#[extendr]
fn rust_alter_columns(table: &LanceTable, alterations_json: &str) {
    let rt = get_runtime();
    let alts: Vec<serde_json::Value> =
        serde_json::from_str(alterations_json).expect("Invalid alterations JSON");

    let alterations: Vec<lancedb::table::ColumnAlteration> = alts
        .iter()
        .map(|a| {
            let path = a["path"].as_str().unwrap().to_string();
            let mut alt = lancedb::table::ColumnAlteration::new(path);
            if let Some(rename) = a.get("rename").and_then(|v| v.as_str()) {
                alt = alt.rename(rename.to_string());
            }
            if let Some(nullable) = a.get("nullable").and_then(|v| v.as_bool()) {
                alt = alt.set_nullable(nullable);
            }
            alt
        })
        .collect();

    let alt_refs: Vec<&lancedb::table::ColumnAlteration> = alterations.iter().collect();

    rt.block_on(table.inner.alter_columns(&alt_refs))
        .expect("Failed to alter columns");
}

/// Drop columns from the table.
#[extendr]
fn rust_drop_columns(table: &LanceTable, columns: Vec<String>) {
    let rt = get_runtime();
    let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
    rt.block_on(table.inner.drop_columns(&col_refs))
        .expect("Failed to drop columns");
}

// ---------------------------------------------------------------------------
// Versioning
// ---------------------------------------------------------------------------

/// Get the current version of the table.
#[extendr]
fn rust_table_version(table: &LanceTable) -> i64 {
    let rt = get_runtime();
    rt.block_on(table.inner.version())
        .expect("Failed to get table version") as i64
}

/// List all versions of the table. Returns JSON array.
#[extendr]
fn rust_list_versions(table: &LanceTable) -> String {
    let rt = get_runtime();
    let versions = rt
        .block_on(table.inner.list_versions())
        .expect("Failed to list versions");

    let result: Vec<serde_json::Value> = versions
        .iter()
        .map(|v| {
            serde_json::json!({
                "version": v.version,
                "timestamp": v.timestamp.to_string(),
                "metadata": v.metadata,
            })
        })
        .collect();
    serde_json::to_string(&result).unwrap()
}

/// Checkout a specific version by number.
#[extendr]
fn rust_checkout(table: &LanceTable, version: i64) {
    let rt = get_runtime();
    rt.block_on(table.inner.checkout(version as u64))
        .expect("Failed to checkout version");
}

/// Checkout the latest version.
#[extendr]
fn rust_checkout_latest(table: &LanceTable) {
    let rt = get_runtime();
    rt.block_on(table.inner.checkout_latest())
        .expect("Failed to checkout latest version");
}

/// Restore the currently checked-out version as a new version.
#[extendr]
fn rust_restore(table: &LanceTable) {
    let rt = get_runtime();
    rt.block_on(table.inner.restore())
        .expect("Failed to restore version");
}

// ---------------------------------------------------------------------------
// Optimization
// ---------------------------------------------------------------------------

/// Compact files in the table.
#[extendr]
fn rust_compact_files(table: &LanceTable) -> String {
    let rt = get_runtime();
    let stats = rt
        .block_on(table.inner.optimize(lancedb::table::OptimizeAction::Compact {
            options: Default::default(),
            remap_options: None,
        }))
        .expect("Failed to compact files");
    format!("{:?}", stats)
}

/// Prune old versions.
#[extendr]
fn rust_cleanup_old_versions(table: &LanceTable, older_than_days: i64) {
    let rt = get_runtime();
    let duration = if older_than_days > 0 {
        std::time::Duration::from_secs(older_than_days as u64 * 86400)
    } else {
        std::time::Duration::from_secs(7 * 86400)
    };

    rt.block_on(table.inner.optimize(lancedb::table::OptimizeAction::Prune {
        older_than: duration,
        delete_unverified: Some(false),
    }))
    .expect("Failed to cleanup old versions");
}

// ---------------------------------------------------------------------------
// Query execution: the heart of lazy collect
// ---------------------------------------------------------------------------

/// Apply common QueryBase operations to any query builder.
/// Handles: where, select, limit, offset, postfilter, fast_search, with_row_id.
fn apply_common_ops<Q: QueryBase>(mut builder: Q, ops: &[serde_json::Value]) -> Q {
    for op in ops {
        let op_type = op["op"].as_str().unwrap();
        match op_type {
            "where" => {
                let expr = op["expr"].as_str().unwrap();
                builder = builder.only_if(expr);
            }
            "select" => {
                let cols: Vec<String> = op["cols"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect();
                builder = builder.select(Select::columns(
                    &cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                ));
            }
            "limit" => {
                let n = op["n"].as_u64().unwrap() as usize;
                builder = builder.limit(n);
            }
            "offset" => {
                let n = op["n"].as_u64().unwrap() as usize;
                builder = builder.offset(n);
            }
            "postfilter" => {
                builder = builder.postfilter();
            }
            "fast_search" => {
                builder = builder.fast_search();
            }
            "with_row_id" => {
                builder = builder.with_row_id();
            }
            other => {
                panic!(
                    "Unsupported operation '{}'. \
                     Supported ops: where, select, limit, offset, postfilter, fast_search, with_row_id.",
                    other
                );
            }
        }
    }
    builder
}

/// Build a FullTextSearchQuery from the search config.
fn build_fts_query(search_cfg: &serde_json::Value) -> Option<FullTextSearchQuery> {
    search_cfg
        .get("fts_query")
        .and_then(|v| v.as_str())
        .map(|query_text| {
            let mut fts = FullTextSearchQuery::new(query_text.to_string());
            if let Some(cols) = search_cfg.get("fts_columns").and_then(|v| v.as_array()) {
                let col_strings: Vec<String> = cols
                    .iter()
                    .filter_map(|c| c.as_str().map(|s| s.to_string()))
                    .collect();
                if !col_strings.is_empty() {
                    fts = fts.columns(col_strings);
                }
            }
            fts
        })
}

/// Execute a query plan and return Arrow IPC bytes.
///
/// Supports four modes:
///   - "search": vector similarity search
///   - "scan": full table scan (optionally with FTS)
///   - "fts": full-text search query
///   - "hybrid": combined vector + FTS with reranking
///
/// `search_config_json`: JSON object with search-time parameters:
///   Vector: nprobes, refine_factor, ef, column, distance_type, bypass_vector_index,
///           distance_range_lower, distance_range_upper
///   FTS: fts_query, fts_columns
///   Hybrid: fts_query, fts_columns, norm ("rank" or "score")
#[extendr]
fn rust_execute_query(
    table: &LanceTable,
    mode: &str,
    qvec: Nullable<Vec<f64>>,
    ops_json: &str,
    search_config_json: &str,
) -> Raw {
    let rt = get_runtime();
    let ops: Vec<serde_json::Value> =
        serde_json::from_str(ops_json).expect("Invalid ops JSON");
    let search_cfg: serde_json::Value =
        serde_json::from_str(search_config_json).unwrap_or(serde_json::json!({}));

    rt.block_on(async {
        match mode {
            "search" => {
                let query_vec: Vec<f32> = match qvec {
                    Nullable::NotNull(v) => v.iter().map(|x| *x as f32).collect(),
                    _ => panic!("search mode requires a query vector"),
                };

                let mut builder = table
                    .inner
                    .vector_search(query_vec)
                    .expect("Failed to create vector search");

                // Apply vector search-time parameters
                if let Some(np) = search_cfg.get("nprobes").and_then(|v| v.as_u64()) {
                    builder = builder.nprobes(np as usize);
                }
                if let Some(rf) = search_cfg.get("refine_factor").and_then(|v| v.as_u64()) {
                    builder = builder.refine_factor(rf as u32);
                }
                if let Some(ef) = search_cfg.get("ef").and_then(|v| v.as_u64()) {
                    builder = builder.ef(ef as usize);
                }
                if let Some(col) = search_cfg.get("column").and_then(|v| v.as_str()) {
                    builder = builder.column(col);
                }
                if let Some(dt) = search_cfg.get("distance_type").and_then(|v| v.as_str()) {
                    builder = builder.distance_type(parse_distance_type(dt));
                }
                if search_cfg
                    .get("bypass_vector_index")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    builder = builder.bypass_vector_index();
                }

                // Distance range filtering
                let dr_lower = search_cfg
                    .get("distance_range_lower")
                    .and_then(|v| v.as_f64())
                    .map(|v| v as f32);
                let dr_upper = search_cfg
                    .get("distance_range_upper")
                    .and_then(|v| v.as_f64())
                    .map(|v| v as f32);
                if dr_lower.is_some() || dr_upper.is_some() {
                    builder = builder.distance_range(dr_lower, dr_upper);
                }

                // Apply common ops (where, select, limit, offset, postfilter, etc.)
                builder = apply_common_ops(builder, &ops);

                let stream = builder.execute().await.expect("Failed to execute search");
                stream_to_ipc_bytes(stream).await
            }
            "hybrid" => {
                // Hybrid search: vector + FTS combined
                let query_vec: Vec<f32> = match qvec {
                    Nullable::NotNull(v) => v.iter().map(|x| *x as f32).collect(),
                    _ => panic!("hybrid mode requires a query vector"),
                };

                let mut builder = table.inner.query();

                // Add vector search component
                builder = builder.nearest_to(query_vec).expect("Failed to set vector query");

                // Apply vector search parameters
                if let Some(np) = search_cfg.get("nprobes").and_then(|v| v.as_u64()) {
                    builder = builder.nprobes(np as usize);
                }
                if let Some(rf) = search_cfg.get("refine_factor").and_then(|v| v.as_u64()) {
                    builder = builder.refine_factor(rf as u32);
                }
                if let Some(ef) = search_cfg.get("ef").and_then(|v| v.as_u64()) {
                    builder = builder.ef(ef as usize);
                }
                if let Some(col) = search_cfg.get("column").and_then(|v| v.as_str()) {
                    builder = builder.column(col);
                }
                if let Some(dt) = search_cfg.get("distance_type").and_then(|v| v.as_str()) {
                    builder = builder.distance_type(parse_distance_type(dt));
                }

                // Add FTS component
                if let Some(fts) = build_fts_query(&search_cfg) {
                    builder = builder.full_text_search(fts);
                }

                // Normalization method for hybrid score merging
                if let Some(norm_str) = search_cfg.get("norm").and_then(|v| v.as_str()) {
                    let norm = match norm_str.to_lowercase().as_str() {
                        "score" => lancedb::query::NormalizeMethod::Score,
                        _ => lancedb::query::NormalizeMethod::Rank,
                    };
                    builder = builder.norm(norm);
                }

                // Apply common ops
                builder = apply_common_ops(builder, &ops);

                let stream = builder.execute().await.expect("Failed to execute hybrid search");
                stream_to_ipc_bytes(stream).await
            }
            "fts" | "scan" => {
                let mut builder = table.inner.query();

                // Apply full-text search if present
                if let Some(fts) = build_fts_query(&search_cfg) {
                    builder = builder.full_text_search(fts);
                }

                // Apply common ops
                builder = apply_common_ops(builder, &ops);

                let stream = builder.execute().await.expect("Failed to execute scan/fts");
                stream_to_ipc_bytes(stream).await
            }
            _ => panic!("Unknown mode: {}", mode),
        }
    })
    .into()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Deserialize Arrow IPC stream bytes into schema + record batches.
fn ipc_to_batches(
    bytes: &[u8],
) -> (
    std::sync::Arc<arrow_schema::Schema>,
    Vec<arrow_array::RecordBatch>,
) {
    let cursor = std::io::Cursor::new(bytes);
    let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
        .expect("Failed to read Arrow IPC stream");
    let schema = reader.schema();
    let batches: Vec<arrow_array::RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to collect record batches");
    (schema, batches)
}

/// Convert a SendableRecordBatchStream into IPC bytes.
async fn stream_to_ipc_bytes(
    stream: impl futures::Stream<Item = Result<arrow_array::RecordBatch, lancedb::Error>>
        + Unpin
        + Send,
) -> Vec<u8> {
    let batches: Vec<arrow_array::RecordBatch> = stream
        .try_collect()
        .await
        .expect("Failed to collect query results");

    if batches.is_empty() {
        return Vec::new();
    }

    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .expect("Failed to create IPC writer");
        for batch in &batches {
            writer.write(batch).expect("Failed to write batch");
        }
        writer.finish().expect("Failed to finish IPC stream");
    }
    buf
}

// Macro to generate wrappers
extendr_module! {
    mod lancedb;
    fn rust_connect;
    fn rust_connection_uri;
    fn rust_table_names;
    fn rust_drop_table;
    fn rust_open_table;
    fn rust_create_table;
    fn rust_table_name;
    fn rust_table_schema_json;
    fn rust_count_rows;
    fn rust_add_data;
    fn rust_delete_rows;
    fn rust_update_rows;
    fn rust_merge_insert;
    fn rust_create_index;
    fn rust_list_indices;
    fn rust_drop_index;
    fn rust_add_columns;
    fn rust_alter_columns;
    fn rust_drop_columns;
    fn rust_table_version;
    fn rust_list_versions;
    fn rust_checkout;
    fn rust_checkout_latest;
    fn rust_restore;
    fn rust_compact_files;
    fn rust_cleanup_old_versions;
    fn rust_execute_query;
}
