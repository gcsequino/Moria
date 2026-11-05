pub mod agents;
pub mod app_host;
pub mod attestation;
pub mod authority;
pub mod authorization;
pub mod automations;
pub mod canonical;
pub mod catalog;
pub mod changes;
pub mod database;
pub mod dataset_retention;
pub mod dataset_writer;
pub mod delivery;
pub mod directory;
pub(crate) mod engine_io;
mod engine_ipc;
pub mod evaluations;
pub mod execution;
pub mod filesystem;
pub mod filesystem_gc;
pub(crate) mod full_object_source;
pub mod functions;
pub mod history;
mod http_admission;
mod http_auth;
pub mod identity;
pub mod indexing_service;
pub mod io;
pub mod jobs;
pub(crate) mod lifecycle;
pub(crate) mod mcp;
mod model_http;
mod model_protocol;
pub mod models;
pub mod pipelines;
pub mod platform;
pub mod projection;
pub mod recovery;
pub mod recovery_content;
pub(crate) mod recovery_control;
pub mod recovery_keys;
pub(crate) mod recovery_metadata;
pub mod retention;
pub mod retrieval;
pub mod runtime;
pub mod sandbox;
pub mod scratch;
pub mod search;
pub(crate) mod secrets;
pub mod serving;
pub mod snapshot;
pub mod source_runtime;
pub(crate) mod source_store;
pub mod storage;
pub mod transport;
pub mod values;
pub mod wire;
pub mod worker;

pub fn initialize_reader_limits() -> anyhow::Result<()> {
    let maximum = 64 * 1024 * 1024;
    anyhow::ensure!(
        apache_avro::util::max_allocation_bytes(maximum) <= maximum,
        "Native reader allocation limit was initialized too broadly"
    );
    Ok(())
}

pub mod authenticated_file;
pub mod dataset_graph;
pub mod dataset_graph_read;
pub mod native_snapshot_v3;
pub mod persistent_directory;
