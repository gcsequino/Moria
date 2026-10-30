use sha2::{Digest, Sha256};
use std::{fs, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut paths = vec![
        "Cargo.toml".to_owned(),
        "Cargo.lock".to_owned(),
        "build.rs".to_owned(),
        "schema.json".to_owned(),
        "protocol/indexing.proto".to_owned(),
        "../../packages/contracts/value-conformance.json".to_owned(),
    ];
    for file in fs::read_dir("src")? {
        let file = file?;
        if file.file_type()?.is_file() && file.path().extension().is_some_and(|e| e == "rs") {
            paths.push(format!(
                "src/{}",
                file.file_name().to_str().ok_or("Invalid source name")?
            ));
        }
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
    Ok(())
}
