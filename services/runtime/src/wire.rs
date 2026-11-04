use anyhow::{Result, ensure};
use iceberg::scan::FileScanTask;
use prost::Message;

include!(concat!(env!("OUT_DIR"), "/moria.indexing.v1.rs"));

pub mod runtime {
    tonic::include_proto!("moria.runtime.v1");
}

pub fn scan_plan(plan: FileScanTask) -> Result<Vec<u8>> {
    let plan = ScanPlan {
        protocol_version: 1,
        reader_abi: "iceberg-rust/0.10.1".into(),
        native_plan_json: serde_json::to_vec(&plan)?,
    };
    let bytes = plan.encode_to_vec();
    ensure!(bytes.len() <= 1024 * 1024, "Scan plan byte budget exceeded");
    Ok(bytes)
}

pub fn read_scan(bytes: &[u8]) -> Result<FileScanTask> {
    ensure!(bytes.len() <= 1024 * 1024, "Scan plan byte budget exceeded");
    let plan = ScanPlan::decode(bytes)?;
    ensure!(
        plan.protocol_version == 1 && plan.reader_abi == "iceberg-rust/0.10.1",
        "Unsupported scan protocol or reader ABI"
    );
    Ok(serde_json::from_slice(&plan.native_plan_json)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compiled_source_matches_the_quality_gate() {
        let expected = std::env::var("MORIA_INDEXING_EXPECTED_SOURCE_SHA256")
            .unwrap_or_else(|_| env!("MORIA_INDEXING_SOURCE_SHA256").into());
        assert_eq!(expected, env!("MORIA_INDEXING_SOURCE_SHA256"));
    }
    #[test]
    fn lease_golden_encoding_preserves_large_fences() {
        let lease = Lease {
            protocol_version: 1,
            incarnation: String::new(),
            build_id: String::new(),
            task_id: String::new(),
            owner_id: String::new(),
            fence: 9_007_199_254_740_993,
        };
        let golden = vec![8, 1, 48, 129, 128, 128, 128, 128, 128, 128, 16];
        assert_eq!(lease.encode_to_vec(), golden);
        assert_eq!(Lease::decode(golden.as_slice()).unwrap(), lease);
    }
    #[test]
    fn incompatible_reader_is_rejected_before_decoding_its_payload() {
        let bytes = ScanPlan {
            protocol_version: 2,
            reader_abi: "other".into(),
            native_plan_json: b"not json".to_vec(),
        }
        .encode_to_vec();
        assert!(
            read_scan(&bytes)
                .unwrap_err()
                .to_string()
                .contains("Unsupported scan protocol")
        );
    }
}
