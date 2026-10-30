use anyhow::{Result, bail, ensure};
use arrow_array::*;
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

macro_rules! pattern {
    ($text:expr,$expression:literal) => {{
        static PATTERN: std::sync::LazyLock<regex::Regex> =
            std::sync::LazyLock::new(|| regex::Regex::new($expression).unwrap());
        PATTERN.is_match($text)
    }};
}

pub fn descriptor(schema: &Value, definitions: &Value, depth: usize) -> Result<Value> {
    ensure!(depth <= 20, "Descriptor depth exceeded");
    let object = schema
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("Invalid descriptor"))?;
    if let Some(name) = object.get("ref") {
        ensure!(object.len() == 1, "Invalid reference");
        let referenced = definitions
            .get(
                name.as_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid reference"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("Unresolved reference"))?;
        return descriptor(referenced, definitions, depth + 1);
    }
    let kind = schema["type"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing type"))?;
    let nullable = match schema.get("nullable") {
        Some(n) => n
            .as_bool()
            .ok_or_else(|| anyhow::anyhow!("Invalid nullability"))?,
        None if ["object", "array", "list"].contains(&kind) => false,
        _ => bail!("Missing nullability"),
    };
    let mut out = json!({"type":kind,"nullable":nullable});
    let allowed: &[&str] = match kind {
        "string" | "boolean" | "int64" | "float64" | "date" | "timestamp" | "uid" => {
            &["type", "nullable"]
        }
        "decimal" => {
            let precision = schema["precision"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("Invalid precision"))?;
            let scale = schema["scale"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("Invalid scale"))?;
            ensure!(
                (1..=38).contains(&precision) && scale <= precision,
                "Invalid decimal type"
            );
            out["precision"] = precision.into();
            out["scale"] = scale.into();
            &["type", "nullable", "precision", "scale"]
        }
        "ontologyObject" => {
            let id = schema["objectType"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing object identity"))?;
            ensure!(
                Uuid::parse_str(id)?.to_string() == id,
                "Invalid object type identity"
            );
            out["objectType"] = id.into();
            &["type", "nullable", "objectType"]
        }
        "array" | "list" => {
            out["type"] = "array".into();
            out["items"] = descriptor(&schema["items"], definitions, depth + 1)?;
            &["type", "nullable", "items"]
        }
        "object" => {
            let properties = schema["properties"]
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("Missing properties"))?;
            ensure!(properties.len() <= 10000, "Too many properties");
            let mut mapped = serde_json::Map::new();
            for (name, value) in properties {
                ensure!(
                    pattern!(name, r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
                        && !["__proto__", "prototype", "constructor"].contains(&name.as_str()),
                    "Invalid field name"
                );
                mapped.insert(name.clone(), descriptor(value, definitions, depth + 1)?);
            }
            out["properties"] = mapped.into();
            if let Some(optional) = schema.get("optional") {
                let names = optional
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("Invalid optional fields"))?;
                let mut seen = std::collections::HashSet::new();
                for name in names {
                    let name = name
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("Invalid optional field"))?;
                    ensure!(
                        properties.contains_key(name) && seen.insert(name),
                        "Invalid optional field"
                    );
                }
                out["optional"] = optional.clone();
            }
            &["type", "nullable", "properties", "optional"]
        }
        _ => bail!("Unsupported descriptor"),
    };
    ensure!(
        object.keys().all(|k| allowed.contains(&k.as_str())),
        "Unknown descriptor field"
    );
    Ok(out)
}

pub fn normalize(value: &Value, schema: &Value) -> Result<Value> {
    normalize_in(value, schema, 0, &mut 0)
}

fn normalize_in(value: &Value, schema: &Value, depth: usize, nodes: &mut usize) -> Result<Value> {
    *nodes += 1;
    ensure!(depth <= 20 && *nodes <= 100000, "Value complexity exceeded");
    if value.is_null() {
        ensure!(schema["nullable"] == true, "Unexpected null");
        return Ok(Value::Null);
    }
    let text = || {
        value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Expected string"))
    };
    match schema["type"].as_str().unwrap_or("") {
        "string" => {
            ensure!(!text()?.contains('\0'), "NUL is forbidden");
            Ok(value.clone())
        }
        "boolean" => {
            ensure!(value.is_boolean(), "Expected boolean");
            Ok(value.clone())
        }
        "float64" => {
            let n = value
                .as_f64()
                .filter(|x| x.is_finite())
                .ok_or_else(|| anyhow::anyhow!("Expected finite float"))?;
            Ok(if n == 0.0 { json!(0) } else { json!(n) })
        }
        "int64" => {
            let s = text()?;
            ensure!(pattern!(s, r"^-?(0|[1-9][0-9]*)$"), "Invalid integer");
            Ok(s.parse::<i64>()?.to_string().into())
        }
        "uid" => {
            let s = text()?;
            ensure!(Uuid::parse_str(s)?.to_string() == s, "Noncanonical UID");
            Ok(value.clone())
        }
        "decimal" => {
            let s = text()?;
            ensure!(
                pattern!(s, r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$"),
                "Invalid decimal"
            );
            let negative = s.starts_with('-');
            let s = s.trim_start_matches('-');
            let (whole, fraction) = s.split_once('.').unwrap_or((s, ""));
            let precision = schema["precision"].as_u64().unwrap() as usize;
            let scale = schema["scale"].as_u64().unwrap() as usize;
            ensure!(
                fraction.len() <= scale && whole.trim_start_matches('0').len() <= precision - scale,
                "Decimal out of range"
            );
            let zero = s.bytes().all(|c| c == b'0' || c == b'.');
            let mut result = if negative && !zero {
                format!("-{whole}")
            } else {
                whole.to_string()
            };
            if scale > 0 {
                result.push('.');
                result.push_str(fraction);
                result.push_str(&"0".repeat(scale - fraction.len()));
            }
            Ok(result.into())
        }
        "date" => {
            let s = text()?;
            ensure!(pattern!(s, r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"), "Invalid date");
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
            ensure!((1..=9999).contains(&date.year()), "Date out of range");
            Ok(value.clone())
        }
        "timestamp" => {
            let s = text()?;
            ensure!(
                pattern!(
                    s,
                    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-5][0-9](\.[0-9]{1,6})?Z$"
                ),
                "Invalid timestamp"
            );
            let time = DateTime::parse_from_rfc3339(s)?;
            ensure!((1..=9999).contains(&time.year()), "Timestamp out of range");
            Ok(time.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string().into())
        }
        "ontologyObject" => {
            let object = value
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("Invalid object identity"))?;
            ensure!(
                object.len() == 2 && value["objectType"] == schema["objectType"],
                "Wrong object identity"
            );
            let uid = value["uid"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing UID"))?;
            ensure!(Uuid::parse_str(uid)?.to_string() == uid, "Invalid UID");
            Ok(value.clone())
        }
        "array" => Ok(value
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Expected array"))?
            .iter()
            .map(|v| normalize_in(v, &schema["items"], depth + 1, nodes))
            .collect::<Result<Vec<_>>>()?
            .into()),
        "object" => {
            let input = value
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("Expected record"))?;
            let properties = schema["properties"].as_object().unwrap();
            ensure!(
                input.keys().all(|k| properties.contains_key(k)),
                "Unknown property"
            );
            let mut result = serde_json::Map::new();
            for (key, property) in properties {
                if let Some(v) = input.get(key) {
                    result.insert(key.clone(), normalize_in(v, property, depth + 1, nodes)?);
                } else {
                    ensure!(
                        schema["optional"]
                            .as_array()
                            .is_some_and(|a| a.iter().any(|v| v == key)),
                        "Missing property"
                    );
                }
            }
            Ok(result.into())
        }
        _ => bail!("Unsupported type"),
    }
}

pub fn arrow_value(array: &dyn Array, row: usize, schema: &Value) -> Result<Value> {
    if array.is_null(row) {
        return normalize(&Value::Null, schema);
    }
    macro_rules! val {
        ($t:ty) => {
            array
                .as_any()
                .downcast_ref::<$t>()
                .ok_or_else(|| anyhow::anyhow!("Physical type mismatch"))?
                .value(row)
        };
    }
    let value = match array.data_type() {
        DataType::Utf8 => json!(val!(StringArray)),
        DataType::LargeUtf8 => json!(val!(LargeStringArray)),
        DataType::Utf8View => json!(val!(StringViewArray)),
        DataType::Boolean => json!(val!(BooleanArray)),
        DataType::Int32 if schema["type"] == "int64" => json!(val!(Int32Array).to_string()),
        DataType::Int64 if schema["type"] == "int64" => json!(val!(Int64Array).to_string()),
        DataType::Float32 => {
            let n = val!(Float32Array);
            ensure!(n.is_finite(), "Nonfinite physical float");
            json!(n as f64)
        }
        DataType::Float64 => {
            let n = val!(Float64Array);
            ensure!(n.is_finite(), "Nonfinite physical float");
            json!(n)
        }
        DataType::FixedSizeBinary(16) if schema["type"] == "uid" => {
            json!(Uuid::from_slice(val!(FixedSizeBinaryArray))?.to_string())
        }
        DataType::Date32 => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(val!(Date32Array) as i64))
                .ok_or_else(|| anyhow::anyhow!("Date overflow"))?;
            json!(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            let time = DateTime::<Utc>::from_timestamp_micros(val!(TimestampMicrosecondArray))
                .ok_or_else(|| anyhow::anyhow!("Timestamp overflow"))?;
            json!(time.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        }
        DataType::Decimal128(_, scale) if *scale >= 0 => {
            let n = val!(Decimal128Array);
            let scale = *scale as usize;
            let digits = format!("{:0>width$}", n.unsigned_abs(), width = scale + 1);
            let (whole, fraction) = digits.split_at(digits.len() - scale);
            json!(format!(
                "{}{}{}{}",
                if n < 0 { "-" } else { "" },
                whole,
                if scale > 0 { "." } else { "" },
                fraction
            ))
        }
        DataType::List(_) => {
            let list = val!(ListArray);
            Value::Array(
                (0..list.len())
                    .map(|i| arrow_value(list.as_ref(), i, &schema["items"]))
                    .collect::<Result<_>>()?,
            )
        }
        DataType::LargeList(_) => {
            let list = val!(LargeListArray);
            Value::Array(
                (0..list.len())
                    .map(|i| arrow_value(list.as_ref(), i, &schema["items"]))
                    .collect::<Result<_>>()?,
            )
        }
        _ => bail!("Unsupported physical type"),
    };
    normalize(&value, schema)
}

pub fn partition(environment: Uuid, target: Uuid, uid: Uuid, count: u32) -> u32 {
    let mut hash = Sha256::new();
    hash.update(b"moria/index-shard/v1\0");
    hash.update(environment.as_bytes());
    hash.update(target.as_bytes());
    hash.update(uid.as_bytes());
    (u64::from_be_bytes(hash.finalize()[..8].try_into().unwrap()) % u64::from(count)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    fn equivalent(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Number(_), Value::Number(_)) => a.as_f64() == b.as_f64(),
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equivalent(a, b))
            }
            (Value::Object(a), Value::Object(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(k, v)| b.get(k).is_some_and(|other| equivalent(v, other)))
            }
            _ => a == b,
        }
    }
    #[test]
    fn shared_value_conformance() {
        #[derive(serde::Deserialize)]
        struct Cases {
            valid: Vec<Value>,
            invalid: Vec<Box<serde_json::value::RawValue>>,
            #[serde(rename = "invalidTypes")]
            invalid_types: Vec<Value>,
        }
        let cases: Cases = serde_json::from_str(include_str!(
            "../../../packages/contracts/value-conformance.json"
        ))
        .unwrap();
        for case in &cases.valid {
            let schema = descriptor(&case["schema"], &case["definitions"], 0)
                .unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
            let normalized = normalize(&case["value"], &schema)
                .unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
            assert!(
                equivalent(&normalized, &case["normalized"]),
                "{}: {normalized}",
                case["name"]
            );
        }
        for raw in cases.invalid {
            let Ok(case) = serde_json::from_str::<Value>(raw.get()) else {
                continue;
            };
            let schema = descriptor(&case["schema"], &case["definitions"], 0).unwrap();
            assert!(
                normalize(&case["value"], &schema).is_err(),
                "{}",
                case["name"]
            );
        }
        for case in &cases.invalid_types {
            assert!(
                descriptor(&case["schema"], &case["definitions"], 0).is_err(),
                "{}",
                case["name"]
            );
        }
    }
}
