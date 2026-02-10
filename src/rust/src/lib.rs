use arrow_ipc::writer::StreamWriter;
use extendr_api::prelude::*;
use futures::TryStreamExt;
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
/// Returns an external pointer wrapping a `LanceConnection`.
/// @export
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

    // Deserialize IPC stream bytes → RecordBatch iterator
    let cursor = std::io::Cursor::new(ipc_bytes.as_slice());
    let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
        .expect("Failed to read Arrow IPC stream");

    let schema = reader.schema();
    let batches: Vec<arrow_array::RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to collect record batches");

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
    // Serialize Arrow schema to JSON for R consumption
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

/// Add data to an existing table from Arrow IPC bytes.
#[extendr]
fn rust_add_data(table: &LanceTable, ipc_bytes: Raw, mode: &str) {
    let rt = get_runtime();

    let cursor = std::io::Cursor::new(ipc_bytes.as_slice());
    let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
        .expect("Failed to read Arrow IPC stream");

    let schema = reader.schema();
    let batches: Vec<arrow_array::RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to collect record batches");

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

// ---------------------------------------------------------------------------
// Query execution: the heart of lazy collect
// ---------------------------------------------------------------------------

/// Execute a query plan and return Arrow IPC bytes.
///
/// Parameters:
/// - table: external pointer to LanceTable
/// - mode: "search" or "scan"
/// - qvec: numeric vector for search, or NULL for scan
/// - ops_json: JSON-encoded array of operations
///   Each op: { "op": "where"|"select"|"limit", ... }
///
/// Returns: raw vector of Arrow IPC stream bytes
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

                // Apply operations
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
                        _ => {
                            // Unsupported op — skip with warning
                        }
                    }
                }

                let stream = builder.execute().await.expect("Failed to execute search");
                stream_to_ipc_bytes(stream).await
            }
            "scan" => {
                let mut builder = table.inner.query();

                // Apply operations
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
                        _ => {
                            // Unsupported op — skip
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

/// Convert a SendableRecordBatchStream into IPC bytes.
async fn stream_to_ipc_bytes(
    stream: impl futures::Stream<Item = Result<arrow_array::RecordBatch, lancedb::Error>>
        + Unpin
        + Send,
) -> Vec<u8> {
    // Collect batches
    let batches: Vec<arrow_array::RecordBatch> = stream
        .try_collect()
        .await
        .expect("Failed to collect query results");

    if batches.is_empty() {
        // Return empty IPC stream with no schema — R side will handle this
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
        writer
            .finish()
            .expect("Failed to finish IPC stream");
    }
    buf
}

// Macro to generate wrappers
extendr_module! {
    mod lancedb;
    fn rust_connect;
    fn rust_connection_uri;
    fn rust_table_names;
    fn rust_open_table;
    fn rust_create_table;
    fn rust_table_name;
    fn rust_table_schema_json;
    fn rust_count_rows;
    fn rust_add_data;
    fn rust_delete_rows;
    fn rust_execute_query;
}
