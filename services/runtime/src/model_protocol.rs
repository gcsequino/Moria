use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct ChatPlan {
    pub payload: Value,
    pub tools: BTreeMap<String, jsonschema::Validator>,
    pub output: Option<jsonschema::Validator>,
}
fn name(value: &Value) -> Result<&str> {
    let name = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
    ensure!(
        !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "MODEL_REQUEST_INVALID"
    );
    Ok(name)
}

fn tree(value: &Value, schema: bool) -> Result<()> {
    let mut pending = vec![(value, 0)];
    let mut nodes = 0;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        ensure!(
            depth <= 24 && nodes <= if schema { 256 } else { 8192 },
            "MODEL_SCHEMA_LIMIT"
        );
        match value {
            Value::Object(fields) => {
                for (key, value) in fields {
                    if schema {
                        ensure!(
                            !["$ref", "$dynamicRef", "$recursiveRef"].contains(&key.as_str()),
                            "UNSUPPORTED_CAPABILITY"
                        );
                    }
                    pending.push((value, depth + 1));
                }
            }
            Value::Array(values) => {
                pending.extend(values.iter().map(|v| (v, depth + 1)));
            }
            Value::String(text) => ensure!(
                text.len() <= if schema { 8192 } else { 131072 },
                "MODEL_SCHEMA_LIMIT"
            ),
            _ => {}
        }
    }
    Ok(())
}
fn validator(schema: &Value) -> Result<jsonschema::Validator> {
    ensure!(
        serde_json::to_vec(schema)?.len() <= 65536,
        "MODEL_SCHEMA_LIMIT"
    );
    tree(schema, true)?;
    jsonschema::options()
        .with_pattern_options(jsonschema::PatternOptions::regex())
        .build(schema)
        .map_err(|_| anyhow::anyhow!("MODEL_SCHEMA_INVALID"))
}

impl ChatPlan {
    pub fn parse(input: &Value) -> Result<Self> {
        let object = input
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
        ensure!(object.keys().all(|k|["messages","maxTokens","tools","responseFormat"].contains(&k.as_str())),"MODEL_REQUEST_INVALID");
        let max = input["maxTokens"]
            .as_u64()
            .filter(|v| (1..=4096).contains(v))
            .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
        let messages = input["messages"]
            .as_array()
            .filter(|v| !v.is_empty() && v.len() <= 100)
            .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
        let mut wire = Vec::new();
        for message in messages {
            let fields = message
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
            let role = message["role"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
            ensure!(
                ["system", "user", "assistant", "tool"].contains(&role),
                "MODEL_REQUEST_INVALID"
            );
            if role == "tool" {
                ensure!(
                    fields.len() == 3
                        && fields.contains_key("role")
                        && message["content"].is_string()
                        && message["toolCallId"]
                            .as_str()
                            .is_some_and(|s| !s.is_empty() && s.len() <= 128),
                    "MODEL_REQUEST_INVALID"
                );
                wire.push(json!({"role":role,"content":message["content"],"tool_call_id":message["toolCallId"]}));
            } else {
                ensure!(
                    fields
                        .keys()
                        .all(|k| ["role", "content", "toolCalls"].contains(&k.as_str())),
                    "MODEL_REQUEST_INVALID"
                );
                ensure!(
                    message["content"].is_string()
                        || (role == "assistant"
                            && message["content"].is_null()
                            && fields.contains_key("toolCalls")),
                    "MODEL_REQUEST_INVALID"
                );
                let mut item = json!({"role":role,"content":message["content"]});
                if let Some(calls) = message.get("toolCalls") {
                    ensure!(role == "assistant", "MODEL_REQUEST_INVALID");
                    let calls = calls
                        .as_array()
                        .filter(|v| !v.is_empty() && v.len() <= 16)
                        .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
                    let mut ids = BTreeSet::new();
                    let mut normalized = Vec::new();
                    for call in calls {
                        ensure!(
                            call.as_object()
                                .is_some_and(|o| o.len() == 3 && o.contains_key("arguments")),
                            "MODEL_REQUEST_INVALID"
                        );
                        let id = call["id"]
                            .as_str()
                            .filter(|s| !s.is_empty() && s.len() <= 128)
                            .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
                        ensure!(ids.insert(id), "MODEL_REQUEST_INVALID");
                        tree(&call["arguments"], false)?;
                        normalized.push(json!({"id":id,"type":"function","function":{"name":name(&call["name"])? ,"arguments":crate::canonical::canonical_json(&call["arguments"])?}}));
                    }
                    item["tool_calls"] = json!(normalized);
                }
                wire.push(item);
            }
        }
        let mut tools = BTreeMap::new();
        let mut payload = json!({"messages":wire,"max_tokens":max,"temperature":0,"stream":false});
        if let Some(definitions) = input.get("tools") {
            let definitions = definitions
                .as_array()
                .filter(|v| !v.is_empty() && v.len() <= 16)
                .ok_or_else(|| anyhow::anyhow!("MODEL_REQUEST_INVALID"))?;
            let mut normalized = Vec::new();
            for tool in definitions {
                ensure!(
                    tool.as_object().is_some_and(|o| o.keys().all(|k| [
                        "name",
                        "description",
                        "parameters"
                    ]
                    .contains(&k.as_str()))),
                    "MODEL_REQUEST_INVALID"
                );
                let name = name(&tool["name"])?;
                ensure!(!tools.contains_key(name), "MODEL_REQUEST_INVALID");
                let check = validator(&tool["parameters"])?;
                let mut function = json!({"name":name,"parameters":tool["parameters"]});
                if let Some(description) = tool.get("description") {
                    ensure!(
                        description.as_str().is_some_and(|s| s.len() <= 4096),
                        "MODEL_REQUEST_INVALID"
                    );
                    function["description"] = description.clone();
                }
                normalized.push(json!({"type":"function","function":function}));
                tools.insert(name.into(), check);
            }
            payload["tools"] = json!(normalized);
            payload["tool_choice"] = json!("auto");
        }
        let output = if let Some(format) = input.get("responseFormat") {
            ensure!(
                format
                    .as_object()
                    .is_some_and(|o| o.len() == 2 && o.contains_key("schema")),
                "MODEL_REQUEST_INVALID"
            );
            let name = name(&format["name"])?;
            let check = validator(&format["schema"])?;
            payload["response_format"] = json!({"type":"json_schema","json_schema":{"name":name,"schema":format["schema"],"strict":true}});
            Some(check)
        } else {
            None
        };
        ensure!(
            tools.is_empty() || output.is_none(),
            "UNSUPPORTED_CAPABILITY"
        );
        Ok(Self {
            payload,
            tools,
            output,
        })
    }

    pub fn normalize(
        &self,
        body: Value,
        id: uuid::Uuid,
        model: &str,
        revision: &str,
    ) -> Result<Value> {
        let choices = body["choices"]
            .as_array()
            .filter(|c| c.len() == 1)
            .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
        let choice = &choices[0];
        let message = &choice["message"];
        let reason = choice["finish_reason"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
        ensure!(["stop", "tool_calls"].contains(&reason), "MODEL_INCOMPLETE");
        let mut result = json!({"id":id,"model":model,"modelRevision":revision,"content":message["content"],"finishReason":reason,"usage":usage(&body["usage"])?});
        if reason == "tool_calls" {
            ensure!(
                message["content"].is_string() || message["content"].is_null(),
                "MODEL_PROTOCOL"
            );
            let calls = message["tool_calls"]
                .as_array()
                .filter(|v| !v.is_empty() && v.len() <= 16)
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
            let mut ids = BTreeSet::new();
            let mut output = Vec::new();
            for call in calls {
                let id = call["id"]
                    .as_str()
                    .filter(|s| !s.is_empty() && s.len() <= 128)
                    .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
                ensure!(
                    ids.insert(id) && call["type"] == "function",
                    "MODEL_PROTOCOL"
                );
                let name = call["function"]["name"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
                let check = self
                    .tools
                    .get(name)
                    .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
                let text = call["function"]["arguments"]
                    .as_str()
                    .filter(|s| s.len() <= 65536)
                    .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
                let args: Value =
                    serde_json::from_str(text).map_err(|_| anyhow::anyhow!("MODEL_PROTOCOL"))?;
                tree(&args, false)?;
                ensure!(check.is_valid(&args), "MODEL_PROTOCOL");
                output.push(json!({"id":id,"name":name,"arguments":args}));
            }
            result["toolCalls"] = json!(output);
        } else {
            let content = message["content"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
            ensure!(
                message
                    .get("tool_calls")
                    .is_none_or(|v| v.is_null() || v.as_array().is_some_and(Vec::is_empty)),
                "MODEL_PROTOCOL"
            );
            if let Some(check) = &self.output {
                let value: Value =
                    serde_json::from_str(content).map_err(|_| anyhow::anyhow!("MODEL_PROTOCOL"))?;
                tree(&value, false)?;
                ensure!(check.is_valid(&value), "MODEL_PROTOCOL");
                result["structured"] = value;
            }
        }
        result["metadata"] = json!({"provider":provider_metadata(body, false)});
        Ok(result)
    }
}

pub(crate) fn provider_metadata(mut body: Value, stream: bool) -> Value {
    if let Some(data) = body.get_mut("data").and_then(Value::as_array_mut) {
        for item in data {
            if let Some(fields) = item.as_object_mut() {
                fields.remove("embedding");
            }
        }
    }
    if let Some(choices) = body.get_mut("choices").and_then(Value::as_array_mut) {
        for choice in choices {
            if let Some(message) = choice
                .get_mut(if stream { "delta" } else { "message" })
                .and_then(Value::as_object_mut)
            {
                message.remove("content");
                if let Some(calls) = message.get_mut("tool_calls").and_then(Value::as_array_mut) {
                    for call in calls {
                        if let Some(function) =
                            call.get_mut("function").and_then(Value::as_object_mut)
                        {
                            function.remove("arguments");
                        }
                    }
                }
            }
        }
    }
    body
}

pub(crate) fn usage(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    ensure!(value.is_object(), "MODEL_PROTOCOL");
    let mut result = json!({});
    for (provider, normalized) in [
        ("prompt_tokens", "inputTokens"),
        ("completion_tokens", "outputTokens"),
        ("total_tokens", "totalTokens"),
    ] {
        if let Some(value) = value.get(provider) {
            let value = value
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
            result[normalized] = json!(value.to_string());
        }
    }
    Ok(result)
}

#[derive(Default)]
struct ToolParts {
    id: String,
    name: String,
    arguments: String,
}
#[derive(Default)]
pub(crate) struct ChatStream {
    buffer: Vec<u8>,
    scanned: usize,
    data: Vec<String>,
    data_bytes: usize,
    content: String,
    tools: BTreeMap<usize, ToolParts>,
    reason: Option<String>,
    usage: Value,
    done: bool,
    bytes: usize,
    events: usize,
    provider_id: Option<String>,
    extensions: Vec<Value>,
}
impl ChatStream {
    pub fn terminal(&self) -> bool {
        self.done
    }
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<Value>> {
        self.bytes += bytes.len();
        ensure!(self.bytes <= 4 * 1024 * 1024, "MODEL_PROTOCOL");
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some(end) = self.buffer[self.scanned..]
            .iter()
            .position(|b| *b == b'\n')
            .map(|end| end + self.scanned)
        {
            ensure!(end <= 131072, "MODEL_PROTOCOL");
            let line = self.buffer.drain(..=end).collect::<Vec<_>>();
            self.scanned = 0;
            let line = std::str::from_utf8(&line[..line.len() - 1])
                .map_err(|_| anyhow::anyhow!("MODEL_PROTOCOL"))?
                .trim_end_matches('\r');
            if line.is_empty() {
                if !self.data.is_empty() {
                    let data = self.data.join("\n");
                    self.data.clear();
                    self.data_bytes = 0;
                    self.event(&data, &mut events)?;
                }
            } else if let Some(data) = line.strip_prefix("data:") {
                ensure!(!self.done, "MODEL_PROTOCOL");
                let data = data.strip_prefix(' ').unwrap_or(data);
                self.data_bytes += data.len();
                ensure!(
                    self.data_bytes <= 131072 && self.data.len() < 256,
                    "MODEL_PROTOCOL"
                );
                self.data.push(data.into());
            } else {
                ensure!(
                    line.starts_with(':')
                        || line.starts_with("event:")
                        || line.starts_with("id:")
                        || line.starts_with("retry:"),
                    "MODEL_PROTOCOL"
                );
            }
        }
        self.scanned = self.buffer.len();
        ensure!(self.buffer.len() <= 131072, "MODEL_PROTOCOL");
        Ok(events)
    }
    fn event(&mut self, data: &str, events: &mut Vec<Value>) -> Result<()> {
        self.events += 1;
        ensure!(self.events <= 10000 && !self.done, "MODEL_PROTOCOL");
        if data == "[DONE]" {
            ensure!(self.reason.is_some(), "MODEL_INCOMPLETE");
            self.done = true;
            return Ok(());
        }
        let chunk: Value =
            serde_json::from_str(data).map_err(|_| anyhow::anyhow!("MODEL_PROTOCOL"))?;
        if let Some(id) = chunk["id"].as_str() {
            ensure!(
                id.len() <= 256 && self.provider_id.as_ref().is_none_or(|p| p == id),
                "MODEL_PROTOCOL"
            );
            self.provider_id = Some(id.into());
        }
        if !chunk["usage"].is_null() {
            usage(&chunk["usage"])?;
            self.usage = chunk["usage"].clone();
            events.push(json!({"kind":"usage","usage":usage(&self.usage)?}));
        }
        let mut extension = provider_metadata(chunk.clone(), true);
        if let Some(fields) = extension.as_object_mut() {
            fields.remove("id");
            fields.remove("usage");
        }
        self.extensions.push(extension);
        let choices = chunk["choices"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
        if choices.is_empty() {
            ensure!(!chunk["usage"].is_null(), "MODEL_PROTOCOL");
            return Ok(());
        }
        ensure!(
            choices.len() == 1 && choices[0]["index"] == 0,
            "MODEL_PROTOCOL"
        );
        let choice = &choices[0];
        let delta = &choice["delta"];
        if self.reason.is_some() {
            ensure!(
                delta.as_object().is_some_and(|o| o.is_empty())
                    && choice["finish_reason"].is_null(),
                "MODEL_PROTOCOL"
            );
            return Ok(());
        }
        ensure!(
            delta.is_object() && delta["role"].as_str().is_none_or(|r| r == "assistant"),
            "MODEL_PROTOCOL"
        );
        if let Some(text) = delta.get("content").filter(|v| !v.is_null()) {
            let text = text
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
            self.content.push_str(text);
            ensure!(self.content.len() <= 1024 * 1024, "MODEL_PROTOCOL");
            events.push(json!({"kind":"text","text":text}));
        }
        if let Some(calls) = delta.get("tool_calls").filter(|v| !v.is_null()) {
            for call in calls
                .as_array()
                .filter(|v| v.len() <= 16)
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?
            {
                let index = call["index"]
                    .as_u64()
                    .filter(|v| *v < 16)
                    .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?
                    as usize;
                let parts = self.tools.entry(index).or_default();
                ensure!(
                    call["type"].is_null() || call["type"] == "function",
                    "MODEL_PROTOCOL"
                );
                for (value, target, limit) in [
                    (&call["id"], &mut parts.id, 128),
                    (&call["function"]["name"], &mut parts.name, 64),
                    (&call["function"]["arguments"], &mut parts.arguments, 65536),
                ] {
                    if !value.is_null() {
                        let text = value
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
                        target.push_str(text);
                        ensure!(target.len() <= limit, "MODEL_PROTOCOL");
                    }
                }
                events.push(json!({"kind":"tool","index":index,"id":call["id"],"name":call["function"]["name"],"arguments":call["function"]["arguments"]}));
            }
        }
        if !choice["finish_reason"].is_null() {
            let reason = choice["finish_reason"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("MODEL_PROTOCOL"))?;
            ensure!(["stop", "tool_calls"].contains(&reason), "MODEL_INCOMPLETE");
            self.reason = Some(reason.into());
        }
        Ok(())
    }
    pub fn finish(self) -> Result<Value> {
        ensure!(
            self.done && self.data.is_empty() && self.buffer.iter().all(u8::is_ascii_whitespace),
            "MODEL_INCOMPLETE"
        );
        let mut message = json!({"role":"assistant","content":self.content});
        if !self.tools.is_empty() {
            ensure!(
                self.tools.keys().copied().eq(0..self.tools.len()),
                "MODEL_PROTOCOL"
            );
            message["tool_calls"]=json!(self.tools.into_values().map(|t|json!({"id":t.id,"type":"function","function":{"name":t.name,"arguments":t.arguments}})).collect::<Vec<_>>());
        }
        Ok(
            json!({"id":self.provider_id,"choices":[{"index":0,"message":message,"finish_reason":self.reason}],"usage":self.usage,"streamExtensions":self.extensions}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_schema_and_provider_outputs_are_validated() {
        let input = json!({"messages":[{"role":"user","content":"number"}],"maxTokens":50,"responseFormat":{"name":"answer","schema":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}}});
        let plan = ChatPlan::parse(&input).unwrap();
        let body = |content: &str| json!({"choices":[{"message":{"content":content},"finish_reason":"stop"}]});
        assert!(
            plan.normalize(body("{\"answer\":2}"), uuid::Uuid::nil(), "m", "r")
                .is_ok()
        );
        assert!(
            plan.normalize(body("{\"answer\":\"2\"}"), uuid::Uuid::nil(), "m", "r")
                .is_err()
        );
        let mut invalid = input.clone();
        invalid["responseFormat"]["schema"] = json!({"$ref":"file:///etc/passwd"});
        assert!(ChatPlan::parse(&invalid).is_err());
    }
    #[test]
    fn stream_requires_terminal_and_handles_split_unicode_and_tool_arguments() {
        let mut stream = ChatStream::default();
        let raw = "data: {\"id\":\"a\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"λ\"},\"finish_reason\":null}]}\r\n\r\ndata: {\"id\":\"a\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        let mut events = Vec::new();
        for byte in raw.as_bytes() {
            events.extend(stream.feed(&[*byte]).unwrap());
        }
        assert_eq!(events[0]["text"], "λ");
        assert_eq!(
            stream.finish().unwrap()["choices"][0]["message"]["content"],
            "λ"
        );
        let mut broken = ChatStream::default();
        broken
            .feed(b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n")
            .unwrap();
        assert!(broken.finish().is_err());
        let mut bad = ChatStream::default();
        assert!(bad.feed(b"data: [DONE]\n\n").is_err());
        let mut tool = ChatStream::default();
        for (delta, finish) in [
            (
                json!({"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"choose","arguments":"{\"value\":"}}]}),
                Value::Null,
            ),
            (
                json!({"tool_calls":[{"index":0,"function":{"arguments":"7}"}}]}),
                Value::Null,
            ),
            (json!({}), json!("tool_calls")),
        ] {
            let frame = format!(
                "data: {}\n\n",
                json!({"id":"tools","choices":[{"index":0,"delta":delta,"finish_reason":finish}]})
            );
            for byte in frame.as_bytes() {
                tool.feed(&[*byte]).unwrap();
            }
        }
        tool.feed(b"data: [DONE]\n\n").unwrap();
        let result = tool.finish().unwrap();
        assert_eq!(
            result["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
            "{\"value\":7}"
        );
    }
}
