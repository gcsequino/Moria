use sha2::{Digest, Sha256};
use std::{fs, path::Path};

fn walk(directory: &str, suffix: &str, paths: &mut Vec<String>) -> std::io::Result<()> {
    println!("cargo:rerun-if-changed={directory}");
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = format!(
            "{directory}/{}",
            entry
                .file_name()
                .to_str()
                .ok_or_else(|| std::io::Error::other("Invalid source name"))?
        );
        if kind.is_symlink() {
            return Err(std::io::Error::other("Native inputs cannot be symlinks"));
        }
        if kind.is_dir() {
            walk(&path, suffix, paths)?;
        } else if path.ends_with(suffix) {
            paths.push(path);
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut paths = vec![
        "Cargo.toml".to_owned(),
        "Cargo.lock".to_owned(),
        "build.rs".to_owned(),
        "schema.json".to_owned(),
    ];
    walk("src", ".rs", &mut paths)?;
    walk("protocol", ".proto", &mut paths)?;
    for directory in fs::read_dir("../../tests/fixtures")? {
        let directory = directory?;
        let name = directory
            .file_name()
            .into_string()
            .map_err(|_| "Invalid fixture name")?;
        if name.starts_with("native-") && directory.file_type()?.is_dir() {
            walk(&format!("../../tests/fixtures/{name}"), ".rs", &mut paths)?;
        }
    }
    println!("cargo:rerun-if-changed=../../tests/fixtures");
    for file in fs::read_dir("../../packages/contracts")? {
        let file = file?;
        let name = file
            .file_name()
            .into_string()
            .map_err(|_| "Invalid conformance name")?;
        if name.ends_with("-conformance.json") {
            paths.push(format!("../../packages/contracts/{name}"));
        }
    }
    for name in [
        "manifest.schema.json",
        "resources.schema.json",
        "values.schema.json",
    ] {
        paths.push(format!("../../packages/contracts/schemas/{name}"));
    }
    paths.sort();
    let mut digest = Sha256::new();
    for name in paths {
        println!("cargo:rerun-if-changed={name}");
        let bytes = fs::read(Path::new(&name))?;
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name.as_bytes());
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    println!(
        "cargo:rustc-env=MORIA_INDEXING_SOURCE_SHA256={:x}",
        digest.finalize()
    );
    println!("cargo:rerun-if-changed=protocol/indexing.proto");
    prost_build::Config::new()
        .protoc_executable(protoc_bin_vendored::protoc_bin_path()?)
        .compile_protos(&["protocol/indexing.proto"], &["protocol"])?;
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(
        config,
        &["protocol/runtime.proto"],
        &["protocol"],
    )?;
    Ok(())
}
