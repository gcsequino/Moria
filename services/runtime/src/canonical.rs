use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub fn canonical_json(value: &Value) -> Result<String> {
    fn visit(value: &Value, depth: usize, nodes: &mut usize, out: &mut String) -> Result<()> {
        *nodes += 1;
        ensure!(depth <= 64 && *nodes <= 1_000_000, "INVALID_JSON");
        match value {
            Value::Null => out.push_str("null"),
            Value::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
            Value::Number(value) => {
                let value = value
                    .as_f64()
                    .filter(|n| n.is_finite())
                    .ok_or_else(|| anyhow::anyhow!("INVALID_JSON"))?;
                if value == 0.0 {
                    out.push('0');
                } else {
                    out.push_str(ryu_js::Buffer::new().format_finite(value));
                }
            }
            Value::String(value) => out.push_str(&serde_json::to_string(value)?),
            Value::Array(values) => {
                out.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    visit(value, depth + 1, nodes, out)?;
                }
                out.push(']');
            }
            Value::Object(values) => {
                let mut keys: Vec<&String> = values.keys().collect();
                keys.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
                out.push('{');
                for (index, key) in keys.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::to_string(key)?);
                    out.push(':');
                    visit(&values[*key], depth + 1, nodes, out)?;
                }
                out.push('}');
            }
        }
        Ok(())
    }
    let mut text = String::new();
    visit(value, 0, &mut 0, &mut text)?;
    Ok(text)
}

pub fn fingerprint(value: &Value) -> Result<String> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(canonical_json(value)?.as_bytes())
    ))
}

pub fn canonical_value(value: &Value, schema: &Value, definitions: &Value) -> Result<String> {
    fn typed(value: Value, schema: &Value) -> Result<Value> {
        if value.is_null() {
            return Ok(value);
        }
        Ok(match schema["type"].as_str().unwrap_or("") {
            "float64" => {
                json!({"$float64":format!("{:016x}",value.as_f64().ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?.to_bits())})
            }
            "array" => Value::Array(
                value
                    .as_array()
                    .unwrap()
                    .iter()
                    .cloned()
                    .map(|value| typed(value, &schema["items"]))
                    .collect::<Result<_>>()?,
            ),
            "object" => {
                let properties = value.as_object().unwrap();
                let mut result = serde_json::Map::new();
                for (name, value) in properties {
                    result.insert(
                        name.clone(),
                        typed(value.clone(), &schema["properties"][name])?,
                    );
                }
                Value::Object(result)
            }
            _ => value,
        })
    }
    let schema = crate::values::descriptor(schema, definitions, 0)?;
    let value = crate::values::normalize(value, &schema)?;
    canonical_json(&json!({"version":1,"schema":schema,"value":typed(value,&schema)?}))
}

pub fn value_fingerprint(value: &Value, schema: &Value, definitions: &Value) -> Result<String> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(canonical_value(value, schema, definitions)?.as_bytes())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn javascript_number_and_utf16_ordering() {
        assert_eq!(
            canonical_json(&json!([1.0, -0.0, 1e-7, 1e-6, 1e20, 1e21])).unwrap(),
            "[1,0,1e-7,0.000001,100000000000000000000,1e+21]"
        );
        assert_eq!(
            canonical_json(&json!({"\u{e000}":1,"😀":2})).unwrap(),
            "{\"😀\":2,\"\u{e000}\":1}"
        );
    }
    #[test]
    fn typed_identity_preserves_normalized_envelope() {
        let schema = json!({"type":"float64","nullable":false});
        let definitions = json!({});
        assert_eq!(
            canonical_value(&json!(-0.0), &schema, &definitions).unwrap(),
            canonical_value(&json!(0.0), &schema, &definitions).unwrap()
        );
        assert!(
            canonical_value(&json!(1.0), &schema, &definitions)
                .unwrap()
                .contains("$float64")
        );
    }
    #[test]
    fn canonical_cross_runtime_vectors() {
        let cases: Value = serde_json::from_str(include_str!(
            "../../../packages/contracts/canonical-conformance.json"
        ))
        .unwrap();
        for case in cases["canonical"].as_array().unwrap() {
            assert_eq!(
                canonical_json(&case["value"]).unwrap(),
                case["canonical"].as_str().unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(
                fingerprint(&case["value"]).unwrap(),
                case["fingerprint"].as_str().unwrap(),
                "{}",
                case["name"]
            );
        }
        for case in cases["typed"].as_array().unwrap() {
            assert_eq!(
                value_fingerprint(&case["value"], &case["schema"], &case["definitions"]).unwrap(),
                case["fingerprint"].as_str().unwrap(),
                "{}",
                case["name"]
            );
        }
    }
}
