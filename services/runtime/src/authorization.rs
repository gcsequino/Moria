use crate::values;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::types::ToSql;

pub type ValueMap = BTreeMap<String, Value>;
pub type Result<T> = std::result::Result<T, PolicyError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PolicyError {
    pub code: &'static str,
}
impl std::fmt::Display for PolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code)
    }
}
impl std::error::Error for PolicyError {}
fn error(code: &'static str) -> PolicyError {
    PolicyError { code }
}
fn require(allowed: bool, code: &'static str) -> Result<()> {
    if allowed { Ok(()) } else { Err(error(code)) }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpressionTypes {
    #[serde(default)]
    pub fields: ValueMap,
    #[serde(default)]
    pub parameters: ValueMap,
    #[serde(default)]
    pub attributes: ValueMap,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpressionValues {
    #[serde(default)]
    pub fields: ValueMap,
    #[serde(default)]
    pub parameters: ValueMap,
    #[serde(default)]
    pub attributes: ValueMap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Kind {
    Null,
    String,
    Boolean,
    Int64,
    Float64,
    Decimal { precision: u64, scale: u64 },
    Uid,
    Date,
    Timestamp,
    Array(Box<Kind>),
}
impl Kind {
    fn schema(&self, nullable: bool) -> Value {
        match self {
            Self::Decimal { precision, scale } => {
                json!({"type":"decimal","precision":precision,"scale":scale,"nullable":nullable})
            }
            Self::Array(items) => {
                json!({"type":"array","items":items.schema(false),"nullable":nullable})
            }
            _ => {
                json!({"type":match self { Self::Null=>"string",Self::String=>"string",Self::Boolean=>"boolean",Self::Int64=>"int64",Self::Float64=>"float64",Self::Uid=>"uid",Self::Date=>"date",Self::Timestamp=>"timestamp",_=>unreachable!() },"nullable":nullable})
            }
        }
    }
    fn postgres(&self) -> String {
        match self {
            Self::Null | Self::String => "text".into(),
            Self::Boolean => "boolean".into(),
            Self::Int64 => "bigint".into(),
            Self::Float64 => "double precision".into(),
            Self::Decimal { .. } => "numeric".into(),
            Self::Uid => "uuid".into(),
            Self::Date => "date".into(),
            Self::Timestamp => "timestamptz".into(),
            Self::Array(items) => format!("{}[]", items.postgres()),
        }
    }
}
fn column(schema: &Value) -> Result<Kind> {
    let normalized =
        values::descriptor(schema, &Value::Null, 0).map_err(|_| error("POLICY_INVALID"))?;
    let kind = match normalized["type"].as_str() {
        Some("string") => Kind::String,
        Some("boolean") => Kind::Boolean,
        Some("int64") => Kind::Int64,
        Some("float64") => Kind::Float64,
        Some("decimal") => Kind::Decimal {
            precision: normalized["precision"].as_u64().unwrap(),
            scale: normalized["scale"].as_u64().unwrap(),
        },
        Some("uid") => Kind::Uid,
        Some("date") => Kind::Date,
        Some("timestamp") => Kind::Timestamp,
        Some("array") if schema["type"] == "array" => {
            require(normalized["items"]["nullable"] == false, "POLICY_INVALID")?;
            let items = column(&normalized["items"])?;
            require(!matches!(items, Kind::Array(_)), "POLICY_INVALID")?;
            Kind::Array(Box::new(items))
        }
        _ => return Err(error("POLICY_INVALID")),
    };
    Ok(kind)
}
pub fn postgres_type(schema: &Value) -> Result<String> {
    Ok(column(schema)?.postgres())
}
fn same(left: &Kind, right: &Kind) -> bool {
    match (left, right) {
        (Kind::Null, _) | (_, Kind::Null) => true,
        (Kind::Decimal { .. }, Kind::Decimal { .. }) => true,
        (Kind::Array(left), Kind::Array(right)) => same(left, right),
        _ => left == right,
    }
}
fn common(left: &Kind, right: &Kind) -> Kind {
    match (left, right) {
        (Kind::Null, _) => right.clone(),
        (_, Kind::Null) => left.clone(),
        (
            Kind::Decimal { precision, scale },
            Kind::Decimal {
                precision: other_precision,
                scale: other_scale,
            },
        ) => Kind::Decimal {
            precision: *precision.max(other_precision),
            scale: *scale.max(other_scale),
        },
        (Kind::Array(left), Kind::Array(right)) => Kind::Array(Box::new(common(left, right))),
        _ => left.clone(),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Group {
    Field,
    Parameter,
    Attribute,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operator {
    And,
    Or,
    Not,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    Contains,
    IsNull,
}
#[derive(Clone, Debug)]
enum Expression {
    Literal(Value),
    Lookup(Group, String),
    Apply(Operator, Vec<Node>),
}
#[derive(Clone, Debug)]
struct Node {
    kind: Kind,
    expression: Expression,
}

fn literal_kind(value: &Value) -> Result<Kind> {
    match value {
        Value::Null => Ok(Kind::Null),
        Value::Bool(_) => Ok(Kind::Boolean),
        Value::Number(number) => {
            require(
                number.as_f64().is_some_and(f64::is_finite),
                "POLICY_INVALID",
            )?;
            Ok(Kind::Float64)
        }
        Value::String(_) => {
            values::normalize(value, &Kind::String.schema(false))
                .map_err(|_| error("POLICY_INVALID"))?;
            Ok(Kind::String)
        }
        Value::Array(items) => {
            require(items.len() <= 100, "POLICY_INVALID")?;
            let kind = items
                .iter()
                .find(|item| !item.is_null())
                .map(literal_kind)
                .transpose()?
                .unwrap_or(Kind::Null);
            require(!matches!(kind, Kind::Array(_)), "POLICY_INVALID")?;
            for item in items {
                require(same(&kind, &literal_kind(item)?), "POLICY_INVALID")?;
            }
            Ok(Kind::Array(Box::new(kind)))
        }
        _ => Err(error("POLICY_INVALID")),
    }
}
fn compatible_literal(node: &Node, kind: &Kind) -> bool {
    match &node.expression {
        Expression::Literal(Value::Null) => true,
        Expression::Literal(value @ Value::String(_))
            if matches!(
                kind,
                Kind::Int64 | Kind::Decimal { .. } | Kind::Uid | Kind::Date | Kind::Timestamp
            ) =>
        {
            values::normalize(value, &kind.schema(false)).is_ok()
        }
        _ => false,
    }
}
fn comparison_kind(left: &Node, right: &Node) -> Kind {
    if matches!(left.expression, Expression::Literal(_)) && !same(&left.kind, &right.kind) {
        right.kind.clone()
    } else {
        common(&left.kind, &right.kind)
    }
}

#[derive(Clone, Debug)]
pub struct CompiledExpression {
    root: Node,
    types: ExpressionTypes,
}
impl CompiledExpression {
    pub fn compile(expression: &Value, types: ExpressionTypes) -> Result<Self> {
        let expression = Self::compile_root(expression, types)?;
        require(expression.root.kind == Kind::Boolean, "POLICY_INVALID")?;
        Ok(expression)
    }
    fn compile_root(expression: &Value, types: ExpressionTypes) -> Result<Self> {
        for group in [&types.fields, &types.parameters, &types.attributes] {
            require(group.len() <= 10000, "POLICY_INVALID")?;
            for schema in group.values() {
                column(schema)?;
            }
        }
        fn inspect(
            expression: &Value,
            types: &ExpressionTypes,
            depth: usize,
            count: &mut usize,
        ) -> Result<Node> {
            *count += 1;
            require(*count <= 256 && depth <= 16, "POLICY_INVALID")?;
            let object = expression
                .as_object()
                .ok_or_else(|| error("POLICY_INVALID"))?;
            if object.len() == 1 {
                if let Some(value) = object.get("literal") {
                    return Ok(Node {
                        kind: literal_kind(value)?,
                        expression: Expression::Literal(value.clone()),
                    });
                }
                for (name, group, declarations) in [
                    ("field", Group::Field, &types.fields),
                    ("parameter", Group::Parameter, &types.parameters),
                    ("principalAttribute", Group::Attribute, &types.attributes),
                ] {
                    if let Some(value) = object.get(name) {
                        let name = value.as_str().ok_or_else(|| error("POLICY_INVALID"))?;
                        let schema = declarations
                            .get(name)
                            .ok_or_else(|| error("POLICY_INVALID"))?;
                        return Ok(Node {
                            kind: column(schema)?,
                            expression: Expression::Lookup(group, name.into()),
                        });
                    }
                }
            }
            require(
                object.len() == 2 && object.contains_key("op") && object.contains_key("args"),
                "POLICY_INVALID",
            )?;
            let operator = match expression["op"].as_str() {
                Some("and") => Operator::And,
                Some("or") => Operator::Or,
                Some("not") => Operator::Not,
                Some("eq") => Operator::Eq,
                Some("ne") => Operator::Ne,
                Some("lt") => Operator::Lt,
                Some("lte") => Operator::Lte,
                Some("gt") => Operator::Gt,
                Some("gte") => Operator::Gte,
                Some("in") => Operator::In,
                Some("contains") => Operator::Contains,
                Some("isNull") => Operator::IsNull,
                _ => return Err(error("POLICY_INVALID")),
            };
            let input = expression["args"]
                .as_array()
                .ok_or_else(|| error("POLICY_INVALID"))?;
            require(
                match operator {
                    Operator::Not | Operator::IsNull => input.len() == 1,
                    Operator::And | Operator::Or => input.len() >= 2,
                    _ => input.len() == 2,
                },
                "POLICY_INVALID",
            )?;
            let mut args = input
                .iter()
                .map(|arg| inspect(arg, types, depth + 1, count))
                .collect::<Result<Vec<_>>>()?;
            match operator {
                Operator::And | Operator::Or | Operator::Not => require(
                    args.iter()
                        .all(|arg| matches!(arg.kind, Kind::Boolean | Kind::Null)),
                    "POLICY_INVALID",
                )?,
                Operator::IsNull => {}
                Operator::In | Operator::Contains => {
                    let (list_index, value_index) = if operator == Operator::In {
                        (1, 0)
                    } else {
                        (0, 1)
                    };
                    require(
                        matches!(args[list_index].kind, Kind::Array(_))
                            && !matches!(args[value_index].kind, Kind::Array(_)),
                        "POLICY_INVALID",
                    )?;
                    if matches!(args[list_index].expression, Expression::Literal(_))
                        && args[value_index].kind != Kind::Null
                    {
                        let Expression::Literal(Value::Array(items)) = &args[list_index].expression
                        else {
                            return Err(error("POLICY_INVALID"));
                        };
                        for item in items {
                            values::normalize(item, &args[value_index].kind.schema(true))
                                .map_err(|_| error("POLICY_INVALID"))?;
                        }
                        args[list_index].kind =
                            Kind::Array(Box::new(args[value_index].kind.clone()));
                    }
                    let Kind::Array(items) = &args[list_index].kind else {
                        return Err(error("POLICY_INVALID"));
                    };
                    if let Expression::Literal(value) = &args[value_index].expression
                        && **items != Kind::Null
                    {
                        values::normalize(value, &items.schema(true))
                            .map_err(|_| error("POLICY_INVALID"))?;
                        args[value_index].kind = *items.clone();
                    }
                    let Kind::Array(items) = &args[list_index].kind else {
                        return Err(error("POLICY_INVALID"));
                    };
                    require(same(items, &args[value_index].kind), "POLICY_INVALID")?;
                }
                _ => {
                    require(
                        same(&args[0].kind, &args[1].kind)
                            || compatible_literal(&args[0], &args[1].kind)
                            || compatible_literal(&args[1], &args[0].kind),
                        "POLICY_INVALID",
                    )?;
                    require(
                        args.iter().all(|arg| arg.kind != Kind::Null),
                        "POLICY_INVALID",
                    )?;
                    if matches!(
                        operator,
                        Operator::Lt | Operator::Lte | Operator::Gt | Operator::Gte
                    ) {
                        require(
                            args.iter()
                                .all(|arg| !matches!(arg.kind, Kind::Array(_) | Kind::Boolean)),
                            "POLICY_INVALID",
                        )?;
                    }
                }
            }
            Ok(Node {
                kind: Kind::Boolean,
                expression: Expression::Apply(operator, args),
            })
        }
        let root = inspect(expression, &types, 0, &mut 0)?;
        Ok(Self { root, types })
    }
    fn validate(
        &self,
        fields: &ValueMap,
        parameters: &ValueMap,
        attributes: &ValueMap,
    ) -> Result<()> {
        for (input, types, attributes) in [
            (fields, &self.types.fields, false),
            (parameters, &self.types.parameters, false),
            (attributes, &self.types.attributes, true),
        ] {
            require(input.len() <= 10000, "VALUE_INVALID")?;
            for (name, value) in input {
                if let Some(schema) = types.get(name)
                    && !(attributes && value.is_null())
                {
                    values::normalize(value, schema).map_err(|_| error("VALUE_INVALID"))?;
                }
            }
        }
        Ok(())
    }
    fn evaluate_maps(
        &self,
        fields: &ValueMap,
        parameters: &ValueMap,
        attributes: &ValueMap,
    ) -> Result<Option<bool>> {
        self.validate(fields, parameters, attributes)?;
        let value = evaluate(&self.root, fields, parameters, attributes)?;
        if value.is_null() {
            Ok(None)
        } else {
            value
                .as_bool()
                .map(Some)
                .ok_or_else(|| error("POLICY_INVALID"))
        }
    }
    pub fn evaluate(&self, values: &ExpressionValues) -> Result<Option<bool>> {
        self.evaluate_maps(&values.fields, &values.parameters, &values.attributes)
    }
    pub fn allows(&self, values: &ExpressionValues) -> Result<bool> {
        Ok(self.evaluate(values)? == Some(true))
    }
    pub(crate) fn allows_fields(&self, fields: &ValueMap) -> Result<bool> {
        Ok(self.evaluate_maps(fields, &ValueMap::new(), &ValueMap::new())? == Some(true))
    }
    pub fn sql(
        &self,
        values: &ExpressionValues,
        field_columns: &BTreeMap<String, String>,
        parameter_offset: usize,
    ) -> Result<SqlFragment> {
        self.validate(&BTreeMap::new(), &values.parameters, &values.attributes)?;
        require(parameter_offset <= 65535, "POLICY_INVALID")?;
        let mut result = SqlFragment {
            text: String::new(),
            parameters: Vec::new(),
        };
        fn emit(
            node: &Node,
            expected: Option<&Kind>,
            values: &ExpressionValues,
            columns: &BTreeMap<String, String>,
            offset: usize,
            parameters: &mut Vec<SqlParameter>,
        ) -> Result<String> {
            let kind = expected.unwrap_or(&node.kind);
            let mut bind = |value: &Value| -> Result<String> {
                require(offset + parameters.len() < 65535, "POLICY_INVALID")?;
                let (parameter, source) = if matches!(kind, Kind::Array(_)) {
                    let value = if value.is_null() {
                        None
                    } else {
                        Some(
                            value
                                .as_array()
                                .ok_or_else(|| error("VALUE_INVALID"))?
                                .iter()
                                .map(text_value)
                                .collect::<Result<_>>()?,
                        )
                    };
                    (SqlParameter::Array(value), "text[]")
                } else {
                    (SqlParameter::Scalar(text_value(value)?), "text")
                };
                parameters.push(parameter);
                Ok(format!(
                    "${}::{source}::{}",
                    offset + parameters.len(),
                    kind.postgres()
                ))
            };
            match &node.expression {
                Expression::Literal(value) => bind(value),
                Expression::Lookup(Group::Parameter, name) => {
                    bind(values.parameters.get(name).unwrap_or(&Value::Null))
                }
                Expression::Lookup(Group::Attribute, name) => {
                    bind(values.attributes.get(name).unwrap_or(&Value::Null))
                }
                Expression::Lookup(Group::Field, name) => {
                    let column = columns.get(name).ok_or_else(|| error("POLICY_INVALID"))?;
                    require(identifier(column), "POLICY_INVALID")?;
                    Ok(format!("\"{column}\""))
                }
                Expression::Apply(operator, args) => {
                    let mut child =
                        |index, kind| emit(&args[index], kind, values, columns, offset, parameters);
                    match operator {
                        Operator::And | Operator::Or => {
                            let delimiter = if *operator == Operator::And {
                                " AND "
                            } else {
                                " OR "
                            };
                            let children = (0..args.len())
                                .map(|index| child(index, None))
                                .collect::<Result<Vec<_>>>()?;
                            Ok(format!("({})", children.join(delimiter)))
                        }
                        Operator::Not => Ok(format!("(NOT {})", child(0, None)?)),
                        Operator::IsNull => Ok(format!("({} IS NULL)", child(0, None)?)),
                        Operator::In | Operator::Contains => {
                            let (val, list) = if *operator == Operator::In {
                                (0, 1)
                            } else {
                                (1, 0)
                            };
                            let Kind::Array(items) = &args[list].kind else {
                                return Err(error("POLICY_INVALID"));
                            };
                            let common = common(items, &args[val].kind);
                            let left = child(val, Some(&common))?;
                            let right = child(list, Some(&Kind::Array(Box::new(common.clone()))))?;
                            if common == Kind::String {
                                Ok(format!("(({left} COLLATE \"C\") = ANY({right}))"))
                            } else {
                                Ok(format!("({left} = ANY({right}))"))
                            }
                        }
                        _ => {
                            let kind = comparison_kind(&args[0], &args[1]);
                            let left = child(0, Some(&kind))?;
                            let right = child(1, Some(&kind))?;
                            let op = match operator {
                                Operator::Eq => "=",
                                Operator::Ne => "<>",
                                Operator::Lt => "<",
                                Operator::Lte => "<=",
                                Operator::Gt => ">",
                                Operator::Gte => ">=",
                                _ => unreachable!(),
                            };
                            if kind == Kind::String {
                                Ok(format!(
                                    "(({left} COLLATE \"C\") {op} ({right} COLLATE \"C\"))"
                                ))
                            } else {
                                Ok(format!("({left} {op} {right})"))
                            }
                        }
                    }
                }
            }
        }
        result.text = emit(
            &self.root,
            None,
            values,
            field_columns,
            parameter_offset,
            &mut result.parameters,
        )?;
        Ok(result)
    }
}

/// Typed action values share the policy AST and its bounds without requiring a Boolean root.
#[derive(Clone, Debug)]
pub struct CompiledValueExpression {
    expression: CompiledExpression,
    schema: Value,
}
impl CompiledValueExpression {
    pub fn compile_inferred(expression: &Value, types: ExpressionTypes) -> Result<Self> {
        let expression = CompiledExpression::compile_root(expression, types)?;
        require(expression.root.kind != Kind::Null, "POLICY_INVALID")?;
        let schema = expression.root.kind.schema(false);
        Ok(Self { expression, schema })
    }
    pub fn schema(&self) -> &Value {
        &self.schema
    }
    pub fn compile(expression: &Value, types: ExpressionTypes, schema: &Value) -> Result<Self> {
        let schema =
            values::descriptor(schema, &Value::Null, 0).map_err(|_| error("POLICY_INVALID"))?;
        let kind = column(&schema)?;
        let expression = CompiledExpression::compile_root(expression, types)?;
        require(
            same(&expression.root.kind, &kind)
                || matches!(&expression.root.expression,Expression::Literal(value) if values::normalize(value,&schema).is_ok()),
            "POLICY_INVALID",
        )?;
        if let Expression::Literal(value) = &expression.root.expression {
            values::normalize(value, &schema).map_err(|_| error("VALUE_INVALID"))?;
        }
        Ok(Self { expression, schema })
    }
    pub fn evaluate(&self, values: &ExpressionValues) -> Result<Value> {
        self.expression
            .validate(&values.fields, &values.parameters, &values.attributes)?;
        let value = evaluate(
            &self.expression.root,
            &values.fields,
            &values.parameters,
            &values.attributes,
        )?;
        values::normalize(&value, &self.schema).map_err(|_| error("VALUE_INVALID"))
    }
}
fn identifier(text: &str) -> bool {
    let mut chars = text.bytes();
    chars
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == b'_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == b'_')
}
fn text_value(value: &Value) -> Result<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::String(value) => Ok(Some(value.clone())),
        Value::Bool(value) => Ok(Some(value.to_string())),
        Value::Number(value) => Ok(Some(value.to_string())),
        _ => Err(error("VALUE_INVALID")),
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum SqlParameter {
    Scalar(Option<String>),
    Array(Option<Vec<Option<String>>>),
}
impl SqlParameter {
    pub fn pg(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Scalar(value) => value,
            Self::Array(value) => value,
        }
    }
    pub fn value(&self) -> Value {
        match self {
            Self::Scalar(value) => json!(value),
            Self::Array(value) => json!(value),
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct SqlFragment {
    pub text: String,
    pub parameters: Vec<SqlParameter>,
}

fn decimal_compare(left: &str, right: &str) -> Ordering {
    fn parts(value: &str) -> (bool, &str, &str) {
        let (whole, fraction) = value
            .trim_start_matches('-')
            .split_once('.')
            .unwrap_or((value.trim_start_matches('-'), ""));
        let whole = whole.trim_start_matches('0');
        let fraction = fraction.trim_end_matches('0');
        (
            value.starts_with('-') && !(whole.is_empty() && fraction.is_empty()),
            whole,
            fraction,
        )
    }
    let (left_negative, left_whole, left_fraction) = parts(left);
    let (right_negative, right_whole, right_fraction) = parts(right);
    if left_negative != right_negative {
        return if left_negative {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let order = left_whole
        .len()
        .cmp(&right_whole.len())
        .then_with(|| left_whole.cmp(right_whole))
        .then_with(|| {
            let length = left_fraction.len().max(right_fraction.len());
            left_fraction
                .bytes()
                .chain(std::iter::repeat(b'0'))
                .take(length)
                .cmp(
                    right_fraction
                        .bytes()
                        .chain(std::iter::repeat(b'0'))
                        .take(length),
                )
        });
    if left_negative {
        order.reverse()
    } else {
        order
    }
}
fn compare(left: &Value, right: &Value, kind: &Kind) -> Result<Ordering> {
    if left.is_null() || right.is_null() {
        return Ok(if left == right {
            Ordering::Equal
        } else if left.is_null() {
            Ordering::Greater
        } else {
            Ordering::Less
        });
    }
    fn text(value: &Value) -> Result<&str> {
        value.as_str().ok_or_else(|| error("VALUE_INVALID"))
    }
    match kind {
        Kind::Timestamp => Ok(chrono::DateTime::parse_from_rfc3339(text(left)?)
            .map_err(|_| error("VALUE_INVALID"))?
            .cmp(
                &chrono::DateTime::parse_from_rfc3339(text(right)?)
                    .map_err(|_| error("VALUE_INVALID"))?,
            )),
        Kind::Int64 => Ok(text(left)?
            .parse::<i64>()
            .map_err(|_| error("VALUE_INVALID"))?
            .cmp(
                &text(right)?
                    .parse::<i64>()
                    .map_err(|_| error("VALUE_INVALID"))?,
            )),
        Kind::Decimal { .. } => Ok(decimal_compare(text(left)?, text(right)?)),
        Kind::Array(items) => {
            let left = left.as_array().ok_or_else(|| error("VALUE_INVALID"))?;
            let right = right.as_array().ok_or_else(|| error("VALUE_INVALID"))?;
            if left.len() != right.len() {
                return Ok(Ordering::Greater);
            }
            for (left, right) in left.iter().zip(right) {
                if compare(left, right, items)? != Ordering::Equal {
                    return Ok(Ordering::Greater);
                }
            }
            Ok(Ordering::Equal)
        }
        Kind::Float64 => left
            .as_f64()
            .and_then(|left| right.as_f64().and_then(|right| left.partial_cmp(&right)))
            .ok_or_else(|| error("VALUE_INVALID")),
        Kind::Boolean => Ok(left
            .as_bool()
            .ok_or_else(|| error("VALUE_INVALID"))?
            .cmp(&right.as_bool().ok_or_else(|| error("VALUE_INVALID"))?)),
        _ => Ok(text(left)?.cmp(text(right)?)),
    }
}
fn evaluate(
    node: &Node,
    fields: &ValueMap,
    parameters: &ValueMap,
    attributes: &ValueMap,
) -> Result<Value> {
    match &node.expression {
        Expression::Literal(value) => Ok(value.clone()),
        Expression::Lookup(group, name) => Ok(match group {
            Group::Field => fields,
            Group::Parameter => parameters,
            Group::Attribute => attributes,
        }
        .get(name)
        .cloned()
        .unwrap_or(Value::Null)),
        Expression::Apply(operator, args) => {
            let values = args
                .iter()
                .map(|arg| evaluate(arg, fields, parameters, attributes))
                .collect::<Result<Vec<_>>>()?;
            let truth = |value: &Value| value.as_bool();
            match operator {
                Operator::And => Ok(if values.iter().any(|value| truth(value) == Some(false)) {
                    false.into()
                } else if values.iter().any(Value::is_null) {
                    Value::Null
                } else {
                    true.into()
                }),
                Operator::Or => Ok(if values.iter().any(|value| truth(value) == Some(true)) {
                    true.into()
                } else if values.iter().any(Value::is_null) {
                    Value::Null
                } else {
                    false.into()
                }),
                Operator::Not => Ok(truth(&values[0])
                    .map(|value| json!(!value))
                    .unwrap_or(Value::Null)),
                Operator::IsNull => Ok(values[0].is_null().into()),
                Operator::In | Operator::Contains => {
                    let (val, list) = if *operator == Operator::In {
                        (0, 1)
                    } else {
                        (1, 0)
                    };
                    if values[list].is_null() {
                        return Ok(Value::Null);
                    }
                    let items = values[list]
                        .as_array()
                        .ok_or_else(|| error("VALUE_INVALID"))?;
                    if items.is_empty() {
                        return Ok(false.into());
                    }
                    if values[val].is_null() {
                        return Ok(Value::Null);
                    }
                    let Kind::Array(kind) = &args[list].kind else {
                        return Err(error("POLICY_INVALID"));
                    };
                    let kind = common(kind, &args[val].kind);
                    for item in items {
                        if !item.is_null() && compare(item, &values[val], &kind)? == Ordering::Equal
                        {
                            return Ok(true.into());
                        }
                    }
                    Ok(if items.iter().any(Value::is_null) {
                        Value::Null
                    } else {
                        false.into()
                    })
                }
                _ => {
                    if values.iter().any(Value::is_null) {
                        return Ok(Value::Null);
                    }
                    let order =
                        compare(&values[0], &values[1], &comparison_kind(&args[0], &args[1]))?;
                    Ok(match operator {
                        Operator::Eq => order == Ordering::Equal,
                        Operator::Ne => order != Ordering::Equal,
                        Operator::Lt => order == Ordering::Less,
                        Operator::Lte => order != Ordering::Greater,
                        Operator::Gt => order == Ordering::Greater,
                        Operator::Gte => order != Ordering::Less,
                        _ => unreachable!(),
                    }
                    .into())
                }
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Identity {
    pub principal: String,
    pub application: String,
    pub environment: String,
    pub granted_operations: BTreeSet<String>,
    pub application_scopes: BTreeSet<String>,
    pub markings: BTreeSet<String>,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub delegated: Option<Box<Identity>>,
}
fn identities(identity: &Identity) -> Result<Vec<&Identity>> {
    let mut chain = Vec::new();
    let mut current = Some(identity);
    while let Some(member) = current {
        require(
            chain.len() < 9 && !member.disabled && member.environment == identity.environment,
            "FORBIDDEN",
        )?;
        chain.push(member);
        current = member.delegated.as_deref();
    }
    Ok(chain)
}
pub fn authorize(
    identity: &Identity,
    resource: &str,
    operation: &str,
    markings: &[String],
) -> Result<()> {
    let permission = format!("{resource}:{operation}");
    for member in identities(identity)? {
        require(
            member.granted_operations.contains(&permission)
                && member.application_scopes.contains(&permission)
                && markings.iter().all(|label| member.markings.contains(label)),
            "FORBIDDEN",
        )?;
    }
    Ok(())
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DestinationPolicy {
    pub allowed_destinations: BTreeSet<String>,
    pub allowed_markings: BTreeSet<String>,
}
pub fn authorize_destination(
    identity: &Identity,
    labels: &BTreeSet<String>,
    policy: Option<&DestinationPolicy>,
    destination: &str,
) -> Result<()> {
    let policy = policy.ok_or_else(|| error("DESTINATION_FORBIDDEN"))?;
    require(
        policy.allowed_destinations.contains(destination)
            && labels.is_subset(&policy.allowed_markings),
        "DESTINATION_FORBIDDEN",
    )?;
    for member in identities(identity).map_err(|_| error("DESTINATION_FORBIDDEN"))? {
        require(labels.is_subset(&member.markings), "DESTINATION_FORBIDDEN")?;
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectPolicies {
    pub resource: String,
    pub markings: Vec<String>,
    pub properties: ValueMap,
    pub source_columns: ValueMap,
    pub attributes: ValueMap,
    pub row: Option<Value>,
    pub object_read: Option<Value>,
    #[serde(default)]
    pub property_read: ValueMap,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRow {
    pub source: ValueMap,
    pub effective: ValueMap,
}
#[derive(Clone, Debug)]
pub struct ObjectReader(std::sync::Arc<CompiledObjectReader>);

#[derive(Debug)]
struct CompiledObjectReader {
    policy: ObjectPolicies,
    row: Option<CompiledExpression>,
    object: Option<CompiledExpression>,
    properties: BTreeMap<String, CompiledExpression>,
}
impl ObjectReader {
    pub fn compile(policy: ObjectPolicies) -> Result<Self> {
        for fields in [
            &policy.properties,
            &policy.source_columns,
            &policy.attributes,
        ] {
            require(fields.len() <= 10000, "POLICY_INVALID")?;
            for schema in fields.values() {
                column(schema)?;
            }
        }
        let row = policy
            .row
            .as_ref()
            .map(|expression| {
                CompiledExpression::compile(
                    expression,
                    ExpressionTypes {
                        fields: policy.source_columns.clone(),
                        attributes: policy.attributes.clone(),
                        ..Default::default()
                    },
                )
            })
            .transpose()?;
        let object = policy
            .object_read
            .as_ref()
            .map(|expression| {
                CompiledExpression::compile(
                    expression,
                    ExpressionTypes {
                        fields: policy.properties.clone(),
                        attributes: policy.attributes.clone(),
                        ..Default::default()
                    },
                )
            })
            .transpose()?;
        let mut properties = BTreeMap::new();
        for (name, expression) in &policy.property_read {
            require(policy.properties.contains_key(name), "POLICY_INVALID")?;
            properties.insert(
                name.clone(),
                CompiledExpression::compile(
                    expression,
                    ExpressionTypes {
                        attributes: policy.attributes.clone(),
                        ..Default::default()
                    },
                )?,
            );
        }
        Ok(Self(std::sync::Arc::new(CompiledObjectReader {
            policy,
            row,
            object,
            properties,
        })))
    }
    pub fn prepare(
        &self,
        identity: &Identity,
        attributes: ValueMap,
        projection: Option<Vec<String>>,
        predicate_properties: &[String],
        delegated_attributes: Vec<ValueMap>,
    ) -> Result<PreparedProjection> {
        let reader = &self.0;
        authorize(
            identity,
            &reader.policy.resource,
            "read",
            &reader.policy.markings,
        )?;
        require(
            identities(identity)?.len() == delegated_attributes.len() + 1,
            "FORBIDDEN",
        )?;
        let mut contexts = vec![attributes];
        contexts.extend(delegated_attributes);
        let permitted = |name: &str| -> Result<bool> {
            if let Some(policy) = reader.properties.get(name) {
                for attributes in &contexts {
                    if policy.evaluate_maps(&ValueMap::new(), &ValueMap::new(), attributes)?
                        != Some(true)
                    {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        };
        for name in projection.iter().flatten().chain(predicate_properties) {
            require(
                reader.policy.properties.contains_key(name),
                "PROPERTY_UNKNOWN",
            )?;
            require(permitted(name)?, "PROPERTY_FORBIDDEN")?;
        }
        let selected = if let Some(projection) = projection {
            projection
        } else {
            let mut selected = Vec::new();
            for name in reader.policy.properties.keys() {
                if permitted(name)? {
                    selected.push(name.clone());
                }
            }
            selected
        };
        Ok(PreparedProjection {
            reader: self.clone(),
            contexts,
            selected,
        })
    }
}
#[derive(Clone, Debug)]
pub struct PreparedProjection {
    reader: ObjectReader,
    contexts: Vec<ValueMap>,
    selected: Vec<String>,
}
impl PreparedProjection {
    pub fn selected(&self) -> &[String] {
        &self.selected
    }
    pub fn project(
        &self,
        pinned: Option<&ObjectRow>,
        current: Option<&ObjectRow>,
    ) -> Result<Option<ValueMap>> {
        let (Some(pinned), Some(current)) = (pinned, current) else {
            return Ok(None);
        };
        for state in [pinned, current] {
            for attributes in &self.contexts {
                if let Some(policy) = &self.reader.0.row
                    && policy.evaluate_maps(&state.source, &ValueMap::new(), attributes)?
                        != Some(true)
                {
                    return Ok(None);
                }
                if let Some(policy) = &self.reader.0.object
                    && policy.evaluate_maps(&state.effective, &ValueMap::new(), attributes)?
                        != Some(true)
                {
                    return Ok(None);
                }
            }
        }
        Ok(Some(
            self.selected
                .iter()
                .filter_map(|name| {
                    pinned
                        .effective
                        .get(name)
                        .map(|value| (name.clone(), value.clone()))
                })
                .collect(),
        ))
    }
}

pub fn conformance(request: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Request {
        expression: Value,
        #[serde(default)]
        types: ExpressionTypes,
        #[serde(default)]
        values: ExpressionValues,
        #[serde(default)]
        field_columns: BTreeMap<String, String>,
        #[serde(default)]
        parameter_offset: usize,
    }
    let request: Request =
        serde_json::from_value(request.clone()).map_err(|_| error("POLICY_INVALID"))?;
    let expression = CompiledExpression::compile(&request.expression, request.types)?;
    let result = expression.evaluate(&request.values)?;
    let sql = expression.sql(
        &request.values,
        &request.field_columns,
        request.parameter_offset,
    )?;
    Ok(
        json!({"result":result,"sql":{"text":sql.text,"parameters":sql.parameters.iter().map(SqlParameter::value).collect::<Vec<_>>()}}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn policy_shared_conformance_vectors() {
        let vectors: Value = serde_json::from_str(include_str!(
            "../../../packages/contracts/policy-conformance.json"
        ))
        .unwrap();
        for case in vectors["valid"].as_array().unwrap() {
            let result = conformance(&case["request"])
                .unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
            assert_eq!(result["result"], case["result"], "{}", case["name"]);
            assert!(!result["sql"]["text"].as_str().unwrap().contains("DROP"));
        }
        for case in vectors["invalid"].as_array().unwrap() {
            assert_eq!(
                conformance(&case["request"]).unwrap_err().code,
                case["code"].as_str().unwrap(),
                "{}",
                case["name"]
            );
        }
    }
    fn identity() -> Identity {
        Identity {
            principal: "person".into(),
            application: "application".into(),
            environment: "environment".into(),
            granted_operations: BTreeSet::from(["objects:read".into()]),
            application_scopes: BTreeSet::from(["objects:read".into()]),
            markings: BTreeSet::from(["private".into()]),
            disabled: false,
            delegated: None,
        }
    }
    fn policies() -> ObjectPolicies {
        serde_json::from_value(json!({"resource":"objects","markings":["private"],"properties":{"region":{"type":"string","nullable":false},"secret":{"type":"string","nullable":false}},"sourceColumns":{"region":{"type":"string","nullable":false}},"attributes":{"region":{"type":"string","nullable":false},"admin":{"type":"boolean","nullable":false}},"row":{"op":"eq","args":[{"field":"region"},{"principalAttribute":"region"}]},"objectRead":null,"propertyRead":{"secret":{"principalAttribute":"admin"}}})).unwrap()
    }
    #[test]
    fn projection_intersects_delegation_and_pinned_current_visibility_before_data() {
        let reader = ObjectReader::compile(policies()).unwrap();
        let mut identity = identity();
        let attrs = BTreeMap::from([
            ("region".into(), json!("West")),
            ("admin".into(), json!(false)),
        ]);
        assert_eq!(
            reader
                .prepare(
                    &identity,
                    attrs.clone(),
                    Some(vec!["secret".into()]),
                    &[],
                    vec![]
                )
                .unwrap_err()
                .code,
            "PROPERTY_FORBIDDEN"
        );
        assert_eq!(
            reader
                .prepare(&identity, attrs.clone(), None, &["secret".into()], vec![])
                .unwrap_err()
                .code,
            "PROPERTY_FORBIDDEN"
        );
        let projection = reader
            .prepare(&identity, attrs.clone(), None, &[], vec![])
            .unwrap();
        let visible = ObjectRow {
            source: BTreeMap::from([("region".into(), json!("West"))]),
            effective: BTreeMap::from([
                ("region".into(), json!("West")),
                ("secret".into(), json!("private")),
            ]),
        };
        let hidden = ObjectRow {
            source: BTreeMap::from([("region".into(), json!("East"))]),
            effective: visible.effective.clone(),
        };
        assert_eq!(
            projection
                .project(Some(&visible), Some(&visible))
                .unwrap()
                .unwrap(),
            BTreeMap::from([("region".into(), json!("West"))])
        );
        assert_eq!(
            projection.project(Some(&visible), Some(&hidden)).unwrap(),
            None
        );
        assert_eq!(projection.project(Some(&visible), None).unwrap(), None);
        identity.delegated = Some(Box::new(identity.clone()));
        assert_eq!(
            reader
                .prepare(&identity, attrs.clone(), None, &[], vec![])
                .unwrap_err()
                .code,
            "FORBIDDEN"
        );
        let delegated = reader
            .prepare(
                &identity,
                attrs,
                None,
                &[],
                vec![BTreeMap::from([
                    ("region".into(), json!("East")),
                    ("admin".into(), json!(false)),
                ])],
            )
            .unwrap();
        assert_eq!(
            delegated.project(Some(&visible), Some(&visible)).unwrap(),
            None
        );
    }
    #[test]
    fn grants_markings_destinations_and_environment_are_intersections() {
        let mut identity = identity();
        let labels = BTreeSet::from(["private".into()]);
        let destination = DestinationPolicy {
            allowed_destinations: BTreeSet::from(["local".into()]),
            allowed_markings: labels.clone(),
        };
        authorize(&identity, "objects", "read", &["private".into()]).unwrap();
        authorize_destination(&identity, &labels, Some(&destination), "local").unwrap();
        let mut delegate = identity.clone();
        delegate.application_scopes.clear();
        identity.delegated = Some(Box::new(delegate));
        assert_eq!(
            authorize(&identity, "objects", "read", &[])
                .unwrap_err()
                .code,
            "FORBIDDEN"
        );
        identity.delegated.as_mut().unwrap().environment = "foreign".into();
        assert_eq!(
            authorize_destination(&identity, &labels, Some(&destination), "local")
                .unwrap_err()
                .code,
            "DESTINATION_FORBIDDEN"
        );
        identity.delegated = None;
        identity.disabled = true;
        assert!(authorize(&identity, "objects", "read", &[]).is_err());
        assert!(authorize_destination(&identity, &labels, Some(&destination), "local").is_err());
    }
    #[test]
    fn strict_complexity_and_sql_identifiers_are_enforced() {
        let mut expression = json!({"literal":true});
        for _ in 0..17 {
            expression = json!({"op":"not","args":[expression]});
        }
        assert_eq!(
            CompiledExpression::compile(&expression, ExpressionTypes::default())
                .unwrap_err()
                .code,
            "POLICY_INVALID"
        );
        let expression = json!({"op":"and","args":vec![json!({"literal":true});256]});
        assert!(CompiledExpression::compile(&expression, ExpressionTypes::default()).is_err());
        let request = json!({"expression":{"op":"eq","args":[{"field":"name"},{"literal":"x'; DROP TABLE moria.identities; --"}]},"types":{"fields":{"name":{"type":"string","nullable":false}}},"values":{"fields":{"name":"ordinary"}},"fieldColumns":{"name":"safe"},"parameterOffset":3});
        let plan = conformance(&request).unwrap();
        assert!(
            plan["sql"]["text"]
                .as_str()
                .unwrap()
                .contains("$4::text::text")
        );
        assert_eq!(
            plan["sql"]["parameters"][0],
            "x'; DROP TABLE moria.identities; --"
        );
        let mut invalid = request;
        invalid["fieldColumns"]["name"] = "safe; DROP TABLE moria.identities".into();
        assert!(conformance(&invalid).is_err());
    }
    #[test]
    fn action_value_expressions_preserve_types_nullability_and_boolean_policy_roots() {
        let schema = json!({"type":"int64","nullable":false});
        let types = ExpressionTypes {
            parameters: BTreeMap::from([("amount".into(), schema.clone())]),
            ..Default::default()
        };
        let expression = CompiledValueExpression::compile(
            &json!({"parameter":"amount"}),
            types.clone(),
            &schema,
        )
        .unwrap();
        assert_eq!(
            expression
                .evaluate(&ExpressionValues {
                    parameters: BTreeMap::from([("amount".into(), json!("9223372036854775807"))]),
                    ..Default::default()
                })
                .unwrap(),
            json!("9223372036854775807")
        );
        assert!(
            CompiledValueExpression::compile(&json!({"literal":null}), types.clone(), &schema)
                .is_err()
        );
        assert!(
            CompiledValueExpression::compile(&json!({"literal":1}), types.clone(), &schema)
                .is_err()
        );
        assert!(
            CompiledExpression::compile(&json!({"parameter":"amount"}), types.clone()).is_err()
        );
        let comparison = CompiledValueExpression::compile(
            &json!({"op":"eq","args":[{"parameter":"amount"},{"literal":"5"}]}),
            types,
            &json!({"type":"boolean","nullable":false}),
        )
        .unwrap();
        assert_eq!(
            comparison
                .evaluate(&ExpressionValues {
                    parameters: BTreeMap::from([("amount".into(), json!("5"))]),
                    ..Default::default()
                })
                .unwrap(),
            json!(true)
        );
    }
}
