#[test]
#[ignore = "invoked mandatorily by scripts/check-indexing.ts before integration fixtures"]
fn native_artifact_metadata() {
    let expected = std::env::var("MORIA_INDEXING_EXPECTED_SOURCE_SHA256")
        .expect("the quality gate must provide its complete native source digest");
    assert_eq!(env!("MORIA_INDEXING_SOURCE_SHA256"), expected);
}
