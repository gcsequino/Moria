use moria_runtime::authorization::{
    CompiledExpression, ExpressionTypes, ExpressionValues, SqlParameter, conformance, postgres_type,
};
use serde_json::Value;
include!("../native-common/artifact.rs");
use std::collections::BTreeMap;

fn text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => panic!("fixture scalar must be a validated scalar"),
    }
}

#[test]
#[ignore = "invoked mandatorily by tests/native-policy.integration.test.ts with real PostgreSQL"]
fn sql_policy_parity() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let url = std::env::var("MORIA_POLICY_TEST_DATABASE_URL")
                .expect("real PostgreSQL fixture configuration is required");
            let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .unwrap_or_else(|_| panic!("policy parity database connection failed"));
            let connection = tokio::spawn(async move {
                connection
                    .await
                    .unwrap_or_else(|_| panic!("policy parity database transport failed"));
            });
            client
                .batch_execute("SET statement_timeout='5s'; SET lock_timeout='5s'")
                .await
                .unwrap();
            let vectors: Value = serde_json::from_str(include_str!(
                "../../../packages/contracts/policy-conformance.json"
            ))
            .unwrap();
            let cases = vectors["valid"].as_array().unwrap();
            assert!(!cases.is_empty(), "policy parity must execute real cases");
            for case in cases {
                let request = &case["request"];
                let types: ExpressionTypes =
                    serde_json::from_value(request["types"].clone()).unwrap();
                let values: ExpressionValues =
                    serde_json::from_value(request["values"].clone()).unwrap();
                let mut parameters = Vec::new();
                let mut columns = BTreeMap::new();
                let mut fields = Vec::new();
                for (name, schema) in &types.fields {
                    let column = format!("f{}", columns.len());
                    columns.insert(name.clone(), column.clone());
                    let value = values.fields.get(name).unwrap_or(&Value::Null);
                    let array = schema["type"] == "array";
                    parameters.push(if array {
                        SqlParameter::Array(if value.is_null() {
                            None
                        } else {
                            Some(value.as_array().unwrap().iter().map(text).collect())
                        })
                    } else {
                        SqlParameter::Scalar(text(value))
                    });
                    fields.push(format!(
                        "${}::{}::{} AS \"{column}\"",
                        parameters.len(),
                        if array { "text[]" } else { "text" },
                        postgres_type(schema).unwrap()
                    ));
                }
                let compiled =
                    CompiledExpression::compile(&request["expression"], types).unwrap();
                let evaluated = compiled.evaluate(&values).unwrap();
                assert_eq!(
                    serde_json::to_value(evaluated).unwrap(),
                    case["result"],
                    "{}: native reference evaluation",
                    case["name"]
                );
                let sql = compiled.sql(&values, &columns, parameters.len()).unwrap();
                parameters.extend(sql.parameters);
                if fields.is_empty() {
                    fields.push("true AS unused".into());
                }
                let query = format!(
                    "SELECT {} AS result FROM (SELECT {}) AS policy_input",
                    sql.text,
                    fields.join(",")
                );
                let references: Vec<_> = parameters.iter().map(SqlParameter::pg).collect();
                let actual: Option<bool> = client
                    .query_one(&query, &references)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("{}: PostgreSQL policy query failed: {error}", case["name"])
                    })
                    .get(0);
                assert_eq!(actual, evaluated, "{}: SQL/reference parity", case["name"]);
            }
            let invalid = vectors["invalid"].as_array().unwrap();
            assert!(!invalid.is_empty());
            for case in invalid {
                assert_eq!(
                    conformance(&case["request"]).unwrap_err().code,
                    case["code"].as_str().unwrap(),
                    "{}: rejected input",
                    case["name"]
                );
            }
            println!(
                "Verified {} valid policies through native evaluation and real PostgreSQL; {} invalid policies rejected.",
                cases.len(), invalid.len()
            );
            drop(client);
            connection.await.unwrap();
        });
}
