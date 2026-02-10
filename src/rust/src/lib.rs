use arrow_ipc::writer::StreamWriter;
use extendr_api::prelude::*;
use futures::TryStreamExt;

// ExecutableQuery and QueryBase are required as trait bounds for
// .execute(), .only_if(), .limit(), and .select() on query builders.
use lancedb::query::{ExecutableQuery, QueryBase, Select};

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

/// Create an index on the table.
/// `index_type`: "auto", "btree", "bitmap", "label_list", "fts",
///               "ivf_pq", "ivf_flat", "ivf_hnsw_pq", "ivf_hnsw_sq"
/// `metric`: distance metric for vector indices ("l2", "cosine", "dot")
#[extendr]
fn rust_create_index(
    table: &LanceTable,
    columns: Vec<String>,
    index_type: &str,
    replace: bool,
    metric: &str,
) {
    let rt = get_runtime();

    let dist = match metric.to_lowercase().as_str() {
        "cosine" => lancedb::DistanceType::Cosine,
        "dot" => lancedb::DistanceType::Dot,
        _ => lancedb::DistanceType::L2,
    };

    let index = match index_type.to_lowercase().as_str() {
        "btree" => lancedb::index::Index::BTree(Default::default()),
        "bitmap" => lancedb::index::Index::Bitmap(Default::default()),
        "label_list" => lancedb::index::Index::LabelList(Default::default()),
        "fts" => lancedb::index::Index::FTS(Default::default()),
        "ivf_pq" => lancedb::index::Index::IvfPq(
            lancedb::index::vector::IvfPqIndexBuilder::default().distance_type(dist),
        ),
        "ivf_flat" => lancedb::index::Index::IvfFlat(
            lancedb::index::vector::IvfFlatIndexBuilder::default().distance_type(dist),
        ),
        "ivf_hnsw_pq" => lancedb::index::Index::IvfHnswPq(
            lancedb::index::vector::IvfHnswPqIndexBuilder::default().distance_type(dist),
        ),
        "ivf_hnsw_sq" => lancedb::index::Index::IvfHnswSq(
            lancedb::index::vector::IvfHnswSqIndexBuilder::default().distance_type(dist),
        ),
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

/// Execute a query plan and return Arrow IPC bytes.
#[extendr]
fn rust_execute_query(
    table: &LanceTable,
    mode: &str,
    qvec: Nullable<Vec<f64>>,
    ops_json: &str,
) -> Raw {
    let rt = get_runtime();
    let ops: Vec<serde_json::Value> =
        serde_json::from_str(ops_json).expect("Invalid ops JSON");

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

                for op in &ops {
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
                        other => {
                            panic!(
                                "Unsupported operation '{}' for vector search mode. \
                                 Supported ops: where, select, limit.",
                                other
                            );
                        }
                    }
                }

                let stream = builder.execute().await.expect("Failed to execute search");
                stream_to_ipc_bytes(stream).await
            }
            "scan" => {
                let mut builder = table.inner.query();

                for op in &ops {
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
                        other => {
                            panic!(
                                "Unsupported operation '{}' for scan mode. \
                                 Supported ops: where, select, limit.",
                                other
                            );
                        }
                    }
                }

                let stream = builder.execute().await.expect("Failed to execute scan");
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
