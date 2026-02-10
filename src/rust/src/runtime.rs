use std::sync::OnceLock;
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Get or create the shared Tokio runtime.
/// LanceDB's Rust API is async; we need a runtime to block on futures from
/// synchronous R/extendr calls.
pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Runtime::new().expect("Failed to create Tokio runtime")
    })
}
