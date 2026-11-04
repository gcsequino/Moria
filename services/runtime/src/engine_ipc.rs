use anyhow::{Result, ensure};
use arrow_array::RecordBatch;
use arrow_ipc::{MessageHeader, Type};
use std::io::Cursor;

pub(crate) const CHUNK_BYTES: usize = 1024 * 1024;
pub(crate) const MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const METADATA_BYTES: usize = 1024 * 1024;
const VALUE_NODES: usize = 100000;
pub(crate) const EOS: [u8; 8] = [255, 255, 255, 255, 0, 0, 0, 0];

#[derive(Clone)]
enum Kind {
    Fixed(usize),
    Bool,
    Text(usize),
    List(usize, Box<Field>),
}
#[derive(Clone)]
pub(crate) struct Field {
    nullable: bool,
    kind: Kind,
}
struct ArrayInfo {
    length: usize,
    validity: (usize, usize),
    kind: InfoKind,
}
enum InfoKind {
    Scalar,
    List {
        offset: usize,
        width: usize,
        child: Box<ArrayInfo>,
    },
}
fn number(value: i64) -> Result<usize> {
    usize::try_from(value).map_err(|_| anyhow::anyhow!("ENGINE_IPC_INVALID"))
}

/// Reads only bounded FlatBuffer metadata; no Arrow allocation occurs on guest lengths.
pub(crate) fn frame_length(bytes: &[u8]) -> Result<Option<(usize, usize)>> {
    if bytes.len() < 8 {
        return Ok(None);
    }
    ensure!(bytes[..4] == [255, 255, 255, 255], "ENGINE_IPC_INVALID");
    let metadata = u32::from_le_bytes(bytes[4..8].try_into()?) as usize;
    if metadata == 0 {
        return Ok(Some((8, 0)));
    }
    ensure!(
        metadata <= METADATA_BYTES && metadata.is_multiple_of(8),
        "ENGINE_IPC_INVALID"
    );
    if bytes.len() < 8 + metadata {
        return Ok(None);
    }
    let message = arrow_ipc::root_as_message(&bytes[8..8 + metadata])
        .map_err(|_| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    let body = number(message.bodyLength())?;
    let total = 8usize
        .checked_add(metadata)
        .and_then(|v| v.checked_add(body))
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    ensure!(total <= MESSAGE_BYTES, "ENGINE_FRAME_LIMIT");
    Ok((bytes.len() >= total).then_some((total, metadata)))
}
fn field(value: arrow_ipc::Field<'_>, depth: usize, nodes: &mut usize) -> Result<Field> {
    *nodes += 1;
    ensure!(
        depth <= 20 && *nodes <= 10000 && value.dictionary().is_none(),
        "ENGINE_IPC_UNSUPPORTED"
    );
    let children = value.children();
    let kind = match value.type_type() {
        Type::Int => {
            let v = value
                .type_as_int()
                .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
            ensure!(
                v.is_signed() && [32, 64].contains(&v.bitWidth()),
                "ENGINE_IPC_UNSUPPORTED"
            );
            Kind::Fixed(v.bitWidth() as usize / 8)
        }
        Type::FloatingPoint => {
            let v = value
                .type_as_floating_point()
                .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
            match v.precision() {
                arrow_ipc::Precision::SINGLE => Kind::Fixed(4),
                arrow_ipc::Precision::DOUBLE => Kind::Fixed(8),
                _ => anyhow::bail!("ENGINE_IPC_UNSUPPORTED"),
            }
        }
        Type::Bool => Kind::Bool,
        Type::Utf8 => Kind::Text(4),
        Type::LargeUtf8 => Kind::Text(8),
        Type::FixedSizeBinary => {
            ensure!(
                value
                    .type_as_fixed_size_binary()
                    .is_some_and(|v| v.byteWidth() == 16),
                "ENGINE_IPC_UNSUPPORTED"
            );
            Kind::Fixed(16)
        }
        Type::Date => {
            ensure!(
                value
                    .type_as_date()
                    .is_some_and(|v| v.unit() == arrow_ipc::DateUnit::DAY),
                "ENGINE_IPC_UNSUPPORTED"
            );
            Kind::Fixed(4)
        }
        Type::Timestamp => {
            ensure!(
                value
                    .type_as_timestamp()
                    .is_some_and(|v| v.unit() == arrow_ipc::TimeUnit::MICROSECOND),
                "ENGINE_IPC_UNSUPPORTED"
            );
            Kind::Fixed(8)
        }
        Type::Decimal => {
            ensure!(
                value.type_as_decimal().is_some_and(|v| v.bitWidth() == 128
                    && (1..=38).contains(&v.precision())
                    && (0..=v.precision()).contains(&v.scale())),
                "ENGINE_IPC_UNSUPPORTED"
            );
            Kind::Fixed(16)
        }
        Type::List | Type::LargeList => {
            let children = children.ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
            ensure!(children.len() == 1, "ENGINE_IPC_INVALID");
            Kind::List(
                if value.type_type() == Type::List {
                    4
                } else {
                    8
                },
                Box::new(field(children.get(0), depth + 1, nodes)?),
            )
        }
        _ => anyhow::bail!("ENGINE_IPC_UNSUPPORTED"),
    };
    if !matches!(kind, Kind::List(..)) {
        ensure!(
            children.is_none_or(|v| v.is_empty()),
            "ENGINE_IPC_UNSUPPORTED"
        );
    }
    Ok(Field {
        nullable: value.nullable(),
        kind,
    })
}
pub(crate) fn schema(bytes: &[u8], expected: &arrow_schema::SchemaRef) -> Result<Vec<Field>> {
    let (length, metadata) =
        frame_length(bytes)?.ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    ensure!(length == bytes.len() && metadata > 0, "ENGINE_IPC_INVALID");
    let message = arrow_ipc::root_as_message(&bytes[8..8 + metadata])
        .map_err(|_| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    ensure!(
        message.header_type() == MessageHeader::Schema && message.bodyLength() == 0,
        "ENGINE_IPC_INVALID"
    );
    let value = message
        .header_as_schema()
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    ensure!(
        value.endianness() == arrow_ipc::Endianness::Little,
        "ENGINE_IPC_UNSUPPORTED"
    );
    let fields = value
        .fields()
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    ensure!(
        !fields.is_empty() && fields.len() <= 256,
        "ENGINE_IPC_UNSUPPORTED"
    );
    let mut nodes = 0;
    let result = fields
        .iter()
        .map(|v| field(v, 0, &mut nodes))
        .collect::<Result<Vec<_>>>()?;
    let mut complete = bytes.to_vec();
    complete.extend(EOS);
    let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(complete), None)?;
    ensure!(reader.schema() == *expected, "SCHEMA_CHANGED");
    Ok(result)
}
struct Buffers<'a, 'b> {
    values: arrow_ipc::RecordBatch<'a>,
    body: &'b [u8],
    position: usize,
    end: usize,
}
impl<'a, 'b> Buffers<'a, 'b> {
    fn next(&mut self) -> Result<(usize, usize)> {
        let values = self
            .values
            .buffers()
            .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
        ensure!(self.position < values.len(), "ENGINE_IPC_INVALID");
        let value = values.get(self.position);
        self.position += 1;
        let offset = number(value.offset())?;
        let length = number(value.length())?;
        ensure!(
            offset % 8 == 0
                && offset >= self.end
                && offset
                    .checked_add(length)
                    .is_some_and(|v| v <= self.body.len()),
            "ENGINE_IPC_INVALID"
        );
        self.end = offset + length;
        Ok((offset, length))
    }
}
fn offset(body: &[u8], start: usize, width: usize, index: usize) -> Result<usize> {
    let start = start
        .checked_add(
            index
                .checked_mul(width)
                .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    let bytes = body
        .get(start..start + width)
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    number(if width == 4 {
        i64::from(i32::from_le_bytes(bytes.try_into()?))
    } else {
        i64::from_le_bytes(bytes.try_into()?)
    })
}
fn array_info(
    field: &Field,
    expected: usize,
    nodes: arrow_ipc::RecordBatch<'_>,
    node_index: &mut usize,
    buffers: &mut Buffers<'_, '_>,
) -> Result<ArrayInfo> {
    let values = nodes
        .nodes()
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    ensure!(*node_index < values.len(), "ENGINE_IPC_INVALID");
    let node = values.get(*node_index);
    *node_index += 1;
    let length = number(node.length())?;
    let nulls = number(node.null_count())?;
    ensure!(
        length == expected && nulls <= length && (field.nullable || nulls == 0),
        "ENGINE_IPC_INVALID"
    );
    let validity = buffers.next()?;
    ensure!(
        validity.1 == 0 && nulls == 0 || validity.1 >= length.div_ceil(8),
        "ENGINE_IPC_INVALID"
    );
    if validity.1 != 0 {
        let bits = &buffers.body[validity.0..validity.0 + length.div_ceil(8)];
        let present = bits
            .iter()
            .enumerate()
            .map(|(index, byte)| {
                let mask = if index + 1 == bits.len() && length % 8 != 0 {
                    (1u8 << (length % 8)) - 1
                } else {
                    255
                };
                (byte & mask).count_ones() as usize
            })
            .sum::<usize>();
        ensure!(length - present == nulls, "ENGINE_IPC_INVALID");
    }
    let kind = match &field.kind {
        Kind::Bool => {
            let data = buffers.next()?;
            ensure!(data.1 >= length.div_ceil(8), "ENGINE_IPC_INVALID");
            InfoKind::Scalar
        }
        Kind::Fixed(width) => {
            let data = buffers.next()?;
            ensure!(
                length.checked_mul(*width).is_some_and(|v| v <= data.1),
                "ENGINE_IPC_INVALID"
            );
            InfoKind::Scalar
        }
        Kind::Text(width) | Kind::List(width, _) => {
            let offsets = buffers.next()?;
            ensure!(
                length
                    .checked_add(1)
                    .and_then(|v| v.checked_mul(*width))
                    .is_some_and(|v| v <= offsets.1),
                "ENGINE_IPC_INVALID"
            );
            let mut previous = offset(buffers.body, offsets.0, *width, 0)?;
            ensure!(previous == 0, "ENGINE_IPC_INVALID");
            for index in 1..=length {
                let next = offset(buffers.body, offsets.0, *width, index)?;
                ensure!(next >= previous, "ENGINE_IPC_INVALID");
                previous = next;
            }
            if let Kind::List(_, child) = &field.kind {
                let child = array_info(child, previous, nodes, node_index, buffers)?;
                InfoKind::List {
                    offset: offsets.0,
                    width: *width,
                    child: Box::new(child),
                }
            } else {
                let data = buffers.next()?;
                ensure!(previous <= data.1, "ENGINE_IPC_INVALID");
                let text = std::str::from_utf8(&buffers.body[data.0..data.0 + previous])
                    .map_err(|_| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
                for index in 0..=length {
                    ensure!(
                        text.is_char_boundary(offset(buffers.body, offsets.0, *width, index)?),
                        "ENGINE_IPC_INVALID"
                    );
                }
                InfoKind::Scalar
            }
        }
    };
    Ok(ArrayInfo {
        length,
        validity,
        kind,
    })
}
fn complexity(
    info: &ArrayInfo,
    start: usize,
    end: usize,
    body: &[u8],
    nodes: &mut usize,
) -> Result<()> {
    ensure!(start <= end && end <= info.length, "ENGINE_IPC_INVALID");
    *nodes = nodes
        .checked_add(end - start)
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    ensure!(*nodes <= VALUE_NODES, "ENGINE_IPC_COMPLEXITY");
    if let InfoKind::List {
        offset: base,
        width,
        child,
    } = &info.kind
    {
        for row in start..end {
            if info.validity.1 != 0 && body[info.validity.0 + row / 8] & (1 << (row % 8)) == 0 {
                continue;
            }
            complexity(
                child,
                offset(body, *base, *width, row)?,
                offset(body, *base, *width, row + 1)?,
                body,
                nodes,
            )?;
        }
    }
    Ok(())
}
pub(crate) fn validate_batch(bytes: &[u8], fields: &[Field]) -> Result<()> {
    let (length, metadata) =
        frame_length(bytes)?.ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    ensure!(length == bytes.len() && metadata > 0, "ENGINE_IPC_INVALID");
    let message = arrow_ipc::root_as_message(&bytes[8..8 + metadata])
        .map_err(|_| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    ensure!(
        message.header_type() == MessageHeader::RecordBatch,
        "ENGINE_IPC_UNSUPPORTED"
    );
    let batch = message
        .header_as_record_batch()
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?;
    let rows = number(batch.length())?;
    ensure!(
        rows <= 1000
            && batch.compression().is_none()
            && batch.variadicBufferCounts().is_none_or(|v| v.is_empty()),
        "ENGINE_IPC_UNSUPPORTED"
    );
    let body = &bytes[8 + metadata..];
    let mut buffers = Buffers {
        values: batch,
        body,
        position: 0,
        end: 0,
    };
    let mut node_index = 0;
    for field in fields {
        let info = array_info(field, rows, batch, &mut node_index, &mut buffers)?;
        for row in 0..rows {
            complexity(&info, row, row + 1, body, &mut 0)?;
        }
    }
    ensure!(
        batch.nodes().is_some_and(|v| v.len() == node_index)
            && batch.buffers().is_some_and(|v| v.len() == buffers.position),
        "ENGINE_IPC_INVALID"
    );
    Ok(())
}
pub(crate) fn encode_schema(schema: &arrow_schema::SchemaRef) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, schema)?;
    writer.finish()?;
    drop(writer);
    let length = frame_length(&bytes)?
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?
        .0;
    bytes.truncate(length);
    self::schema(&bytes, schema)?;
    Ok(bytes)
}
pub(crate) fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    let schema_len = frame_length(&bytes)?
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?
        .0;
    let schema = bytes[..schema_len].to_vec();
    let fields = self::schema(&schema, &batch.schema())?;
    let batch_len = frame_length(&bytes[schema_len..])?
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))?
        .0;
    let result = bytes[schema_len..schema_len + batch_len].to_vec();
    validate_batch(&result, &fields)?;
    Ok(result)
}
pub(crate) fn decode_batch(schema: &[u8], batch: &[u8], fields: &[Field]) -> Result<RecordBatch> {
    validate_batch(batch, fields)?;
    let mut complete = Vec::with_capacity(schema.len() + batch.len() + 8);
    complete.extend(schema);
    complete.extend(batch);
    complete.extend(EOS);
    let mut reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(complete), None)?;
    let result = reader
        .next()
        .ok_or_else(|| anyhow::anyhow!("ENGINE_IPC_INVALID"))??;
    ensure!(reader.next().is_none(), "ENGINE_IPC_INVALID");
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use std::sync::Arc;
    #[test]
    fn metadata_lengths_and_offsets_are_checked_before_arrow_decode() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec![
                "hello", "世界",
            ]))],
        )
        .unwrap();
        let prefix = encode_schema(&schema).unwrap();
        let fields = self::schema(&prefix, &schema).unwrap();
        let message = encode_batch(&batch).unwrap();
        assert_eq!(decode_batch(&prefix, &message, &fields).unwrap(), batch);
        for length in 0..message.len() {
            assert!(frame_length(&message[..length]).unwrap().is_none());
        }
        let mut huge = message.clone();
        let node_offset = {
            let m = arrow_ipc::root_as_message(
                &huge[8..8 + u32::from_le_bytes(huge[4..8].try_into().unwrap()) as usize],
            )
            .unwrap();
            let node = m.header_as_record_batch().unwrap().nodes().unwrap().get(0);
            (node as *const arrow_ipc::FieldNode as usize) - (huge.as_ptr() as usize)
        };
        huge[node_offset..node_offset + 8].copy_from_slice(&i64::MAX.to_le_bytes());
        assert!(validate_batch(&huge, &fields).is_err());
        let mut bad = message.clone();
        bad[4..8].copy_from_slice(&((METADATA_BYTES + 8) as u32).to_le_bytes());
        assert!(frame_length(&bad).is_err());
        let metadata = u32::from_le_bytes(message[4..8].try_into().unwrap()) as usize;
        let mut bad = message.clone();
        let offsets = {
            let m = arrow_ipc::root_as_message(&bad[8..8 + metadata]).unwrap();
            m.header_as_record_batch()
                .unwrap()
                .buffers()
                .unwrap()
                .get(1)
                .offset() as usize
        };
        bad[8 + metadata + offsets + 4..8 + metadata + offsets + 8]
            .copy_from_slice(&i32::MAX.to_le_bytes());
        assert!(validate_batch(&bad, &fields).is_err());
    }
    #[test]
    fn scalar_arrays_have_a_preallocation_complexity_bound() {
        let array =
            arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>(
                vec![Some(vec![Some(1), None, Some(3)])],
            );
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "items",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let prefix = encode_schema(&schema).unwrap();
        let fields = self::schema(&prefix, &schema).unwrap();
        let message = encode_batch(&batch).unwrap();
        assert_eq!(decode_batch(&prefix, &message, &fields).unwrap(), batch);
        let large =
            arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>(
                vec![Some(vec![Some(1); VALUE_NODES])],
            );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(large)]).unwrap();
        assert!(encode_batch(&batch).is_err());
    }
}

/// Detach unused Parquet page backing before retaining small rows for a bounded wire batch.
pub(crate) struct CompactedBatch {
    pub batch: RecordBatch,
    pub retained_bytes: usize,
}
pub(crate) fn compact_batch(
    prefix: &[u8],
    fields: &[Field],
    batch: &RecordBatch,
) -> Result<CompactedBatch> {
    let encoded = encode_batch(batch)?;
    let decoded = decode_batch(prefix, &encoded, fields)?;
    let arrays = decoded
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(array, field)| {
            Ok(arrow_array::make_array(
                array
                    .to_data()
                    .into_builder()
                    .data_type(field.data_type().clone())
                    .build()?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    // IPC columns share one decoded body; Arrow's per-array capacity sums count it repeatedly.
    let structures = arrays
        .iter()
        .map(|array| {
            array
                .get_array_memory_size()
                .saturating_sub(array.get_buffer_memory_size())
        })
        .sum::<usize>();
    let retained_bytes = encoded
        .len()
        .checked_add(structures)
        .and_then(|size| {
            size.checked_add(
                std::mem::size_of::<RecordBatch>()
                    + decoded.num_columns() * std::mem::size_of::<arrow_array::ArrayRef>(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("DECODED_BATCH_LIMIT"))?;
    let batch = RecordBatch::try_new(batch.schema(), arrays)?;
    Ok(CompactedBatch {
        batch,
        retained_bytes,
    })
}
#[cfg(test)]
mod compact_tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field as ArrowField, Schema};
    use std::sync::Arc;
    #[test]
    fn pyarrow_uuid_transport_preserves_the_exact_canonical_metadata() {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        let metadata = |id: &str| {
            [("PARQUET:field_id".into(), id.into())]
                .into_iter()
                .collect()
        };
        let expected = Arc::new(Schema::new(vec![
            ArrowField::new("identity", DataType::FixedSizeBinary(16), false)
                .with_metadata(metadata("1"))
                .with_extension_type(arrow_schema::extension::Uuid),
            ArrowField::new(
                "items",
                DataType::List(Arc::new(
                    ArrowField::new("element", DataType::Int64, false).with_metadata(metadata("3")),
                )),
                false,
            )
            .with_metadata(metadata("2")),
        ]));
        // Actual PyArrow 26 schemas: logical UUID serialization adds empty metadata;
        // the physical transport preserves the accepted canonical UUID fields exactly.
        let logical = STANDARD.decode("/////xACAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAIAAAD0AAAABAAAACb///8AAAAMGAAAAFgAAAAIAAAAGAAAAAEAAABMAAAABQAAAGl0ZW1zAAAAAQAAAAQAAACg/v//EAAAAAQAAAABAAAAMgAAABAAAABQQVJRVUVUOmZpZWxkX2lkAAAAAAQABAAEAAAAjv///wAAAAIUAAAAWAAAAAgAAAAUAAAAAAAAAAcAAABlbGVtZW50AAEAAAAEAAAABP///xAAAAAEAAAAAQAAADMAAAAQAAAAUEFSUVVFVDpmaWVsZF9pZAAAAAAIAA4ACAAHAAgAAAAAAAABQAAAAAAAEgAYAAgAAAAHAAwAAAAQABQAEgAAAAAAAA8UAAAA1AAAAAgAAAAYAAAAAAAAAAgAAABpZGVudGl0eQAAAAADAAAAgAAAAEAAAAAEAAAAlP///xgAAAAEAAAACgAAAGFycm93LnV1aWQAABQAAABBUlJPVzpleHRlbnNpb246bmFtZQAAAADM////EAAAAAQAAAAAAAAAAAAAABgAAABBUlJPVzpleHRlbnNpb246bWV0YWRhdGEAAAAACAAMAAQACAAIAAAAEAAAAAQAAAABAAAAMQAAABAAAABQQVJRVUVUOmZpZWxkX2lkAAAGAAgABAAGAAAAEAAAAAAAAAA=").unwrap();
        assert_eq!(
            schema(&logical, &expected).err().unwrap().to_string(),
            "SCHEMA_CHANGED"
        );
        let physical = STANDARD.decode("/////9gBAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAIAAAD0AAAABAAAACb///8AAAAMGAAAAFgAAAAIAAAAGAAAAAEAAABMAAAABQAAAGl0ZW1zAAAAAQAAAAQAAADY/v//EAAAAAQAAAABAAAAMgAAABAAAABQQVJRVUVUOmZpZWxkX2lkAAAAAAQABAAEAAAAjv///wAAAAIUAAAAWAAAAAgAAAAUAAAAAAAAAAcAAABlbGVtZW50AAEAAAAEAAAAPP///xAAAAAEAAAAAQAAADMAAAAQAAAAUEFSUVVFVDpmaWVsZF9pZAAAAAAIAA4ACAAHAAgAAAAAAAABQAAAAAAAEgAYAAgAAAAHAAwAAAAQABQAEgAAAAAAAA8UAAAAnAAAAAgAAAAYAAAAAAAAAAgAAABpZGVudGl0eQAAAAACAAAASAAAAAQAAADI////GAAAAAQAAAAKAAAAYXJyb3cudXVpZAAAFAAAAEFSUk9XOmV4dGVuc2lvbjpuYW1lAAAAAAgADAAEAAgACAAAABAAAAAEAAAAAQAAADEAAAAQAAAAUEFSUVVFVDpmaWVsZF9pZAAABgAIAAQABgAAABAAAAAAAAAA").unwrap();
        assert_eq!(schema(&physical, &expected).unwrap().len(), 2);
    }
    #[test]
    fn sliced_rows_do_not_charge_or_retain_the_unused_page() {
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "x".repeat(2 * 1024 * 1024),
                "small".into(),
            ]))],
        )
        .unwrap()
        .slice(1, 1);
        assert!(batch.get_array_memory_size() > 1024 * 1024);
        let prefix = encode_schema(&schema).unwrap();
        let fields = super::schema(&prefix, &schema).unwrap();
        let compact = compact_batch(&prefix, &fields, &batch).unwrap();
        assert!(compact.retained_bytes < 4096);
        assert_eq!(
            compact
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "small"
        );
    }
    #[test]
    fn four_column_rows_charge_one_ipc_body_and_share_schema() {
        use arrow_array::Int64Array;
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("group", DataType::Utf8, false),
            ArrowField::new("amount", DataType::Int64, false),
            ArrowField::new("padding", DataType::Utf8, false),
        ]));
        let source = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 1])),
                Arc::new(StringArray::from(vec!["old", "g0"])),
                Arc::new(Int64Array::from(vec![0, 9007199254740993])),
                Arc::new(StringArray::from(vec![
                    "x".repeat(2 * 1024 * 1024),
                    "x".repeat(2048),
                ])),
            ],
        )
        .unwrap()
        .slice(1, 1);
        let prefix = encode_schema(&schema).unwrap();
        let fields = super::schema(&prefix, &schema).unwrap();
        let compact = compact_batch(&prefix, &fields, &source).unwrap();
        assert!(Arc::ptr_eq(&compact.batch.schema(), &schema));
        assert!(compact.retained_bytes < 8192);
        assert!(compact.batch.get_array_memory_size() > compact.retained_bytes);
        let count = (512 * 1024 / compact.retained_bytes).min(1000);
        assert!(
            count >= 100,
            "retained charge {} permits only {count} rows",
            compact.retained_bytes
        );
        assert!(count * compact.retained_bytes <= 512 * 1024);
        assert_eq!(
            compact
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            9007199254740993
        );
    }
    #[test]
    fn compact_list_reuses_the_accepted_item_field() {
        use arrow_array::builder::{Int64Builder, ListBuilder};
        let item = Arc::new(ArrowField::new("element", DataType::Int64, false));
        let mut values = ListBuilder::new(Int64Builder::new()).with_field(item.clone());
        values.values().append_value(9007199254740993);
        values.append(true);
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "items",
            DataType::List(item.clone()),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values.finish())]).unwrap();
        let prefix = encode_schema(&schema).unwrap();
        let fields = super::schema(&prefix, &schema).unwrap();
        let compact = compact_batch(&prefix, &fields, &batch).unwrap();
        let DataType::List(actual) = compact.batch.column(0).data_type() else {
            panic!("accepted list type changed");
        };
        assert!(Arc::ptr_eq(actual, &item));
        assert!(compact.retained_bytes < 4096);
    }
}
