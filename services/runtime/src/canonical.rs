use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Default)]
pub(crate) struct JsonSize(pub(crate) usize);

impl std::io::Write for JsonSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn write_canonical(value: &Value, out: &mut impl std::io::Write) -> Result<()> {
    fn visit(
        value: &Value,
        depth: usize,
        nodes: &mut usize,
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        *nodes += 1;
        ensure!(depth <= 64 && *nodes <= 1_000_000, "INVALID_JSON");
        match value {
            Value::Null => out.write_all(b"null")?,
            Value::Bool(value) => out.write_all(if *value { b"true" } else { b"false" })?,
            Value::Number(value) => {
                let value = value
                    .as_f64()
                    .filter(|n| n.is_finite())
                    .ok_or_else(|| anyhow::anyhow!("INVALID_JSON"))?;
                if value == 0.0 {
                    out.write_all(b"0")?;
                } else {
                    out.write_all(ryu_js::Buffer::new().format_finite(value).as_bytes())?;
                }
            }
            Value::String(value) => serde_json::to_writer(&mut *out, value)?,
            Value::Array(values) => {
                out.write_all(b"[")?;
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        out.write_all(b",")?;
                    }
                    visit(value, depth + 1, nodes, out)?;
                }
                out.write_all(b"]")?;
            }
            Value::Object(values) => {
                let mut entries = values.iter().collect::<Vec<_>>();
                entries
                    .sort_by(|(left, _), (right, _)| left.encode_utf16().cmp(right.encode_utf16()));
                out.write_all(b"{")?;
                for (index, (key, value)) in entries.into_iter().enumerate() {
                    if index > 0 {
                        out.write_all(b",")?;
                    }
                    serde_json::to_writer(&mut *out, key)?;
                    out.write_all(b":")?;
                    visit(value, depth + 1, nodes, out)?;
                }
                out.write_all(b"}")?;
            }
        }
        Ok(())
    }
    visit(value, 0, &mut 0, out)
}

pub fn canonical_json(value: &Value) -> Result<String> {
    let mut bytes = Vec::new();
    write_canonical(value, &mut bytes)?;
    Ok(String::from_utf8(bytes)?)
}

pub fn fingerprint(value: &Value) -> Result<String> {
    let mut hash = Sha256::new();
    write_canonical(value, &mut hash)?;
    Ok(format!("sha256:{:x}", hash.finalize()))
}

fn typed_envelope(value: &Value, schema: &Value, definitions: &Value) -> Result<Value> {
    fn typed(value: Value, schema: &Value) -> Result<Value> {
        if value.is_null() {
            return Ok(value);
        }
        Ok(match schema["type"].as_str().unwrap_or("") {
            "float64" => {
                json!({"$float64":format!("{:016x}",value.as_f64().ok_or_else(|| anyhow::anyhow!("VALUE_INVALID"))?.to_bits())})
            }
            "array" => {
                let Value::Array(items) = value else {
                    anyhow::bail!("VALUE_INVALID")
                };
                Value::Array(
                    items
                        .into_iter()
                        .map(|value| typed(value, &schema["items"]))
                        .collect::<Result<_>>()?,
                )
            }
            "object" => {
                let Value::Object(properties) = value else {
                    anyhow::bail!("VALUE_INVALID")
                };
                Value::Object(
                    properties
                        .into_iter()
                        .map(|(name, value)| {
                            let value = typed(value, &schema["properties"][&name])?;
                            Ok((name, value))
                        })
                        .collect::<Result<_>>()?,
                )
            }
            _ => value,
        })
    }
    let schema = crate::values::descriptor(schema, definitions, 0)?;
    let value = crate::values::normalize(value, &schema)?;
    let value = typed(value, &schema)?;
    Ok(json!({"version":1,"schema":schema,"value":value}))
}

pub fn canonical_value(value: &Value, schema: &Value, definitions: &Value) -> Result<String> {
    canonical_json(&typed_envelope(value, schema, definitions)?)
}

pub fn value_fingerprint(value: &Value, schema: &Value, definitions: &Value) -> Result<String> {
    fingerprint(&typed_envelope(value, schema, definitions)?)
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
