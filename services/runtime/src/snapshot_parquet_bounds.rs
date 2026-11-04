//! Allocation bounds for the qualified flat Parquet profile, checked before Arrow decoding.
use anyhow::{Result, ensure};
use parquet::{
    basic::{Encoding, Type},
    file::metadata::ParquetMetaData,
};
use std::collections::BTreeSet;

const PAGE_BYTES: usize = 16 * 1024 * 1024;
const GROUP_BYTES: u64 = 64 * 1024 * 1024;
const VALUES: i64 = 1_000_000;
const HEADER_BYTES: usize = 64 * 1024;

struct Compact<'a> {
    bytes: &'a [u8],
    position: usize,
    remaining: usize,
}
impl<'a> Compact<'a> {
    fn new(bytes: &'a [u8], remaining: usize) -> Self {
        Self {
            bytes,
            position: 0,
            remaining,
        }
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
        ensure!(end <= self.bytes.len(), "PARQUET_ALLOCATION_LIMIT");
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn variable(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            ensure!(shift != 63 || byte <= 1, "PARQUET_ALLOCATION_LIMIT");
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        anyhow::bail!("PARQUET_ALLOCATION_LIMIT")
    }
    fn integer(&mut self, kind: u8) -> Result<i64> {
        ensure!((4..=6).contains(&kind), "PARQUET_ALLOCATION_LIMIT");
        let raw = self.variable()?;
        Ok((raw >> 1) as i64 ^ -((raw & 1) as i64))
    }
    fn item(&mut self) -> Result<()> {
        ensure!(self.remaining > 0, "PARQUET_ALLOCATION_LIMIT");
        self.remaining -= 1;
        Ok(())
    }
    fn field(&mut self, last: &mut i16, seen: &mut BTreeSet<i16>) -> Result<Option<(i16, u8)>> {
        self.item()?;
        let byte = self.byte()?;
        if byte == 0 {
            return Ok(None);
        }
        let kind = byte & 15;
        ensure!((1..=12).contains(&kind), "PARQUET_ALLOCATION_LIMIT");
        let id = if byte >> 4 == 0 {
            i16::try_from(self.integer(4)?)?
        } else {
            last.checked_add(i16::from(byte >> 4))
                .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?
        };
        ensure!(
            id > 0 && seen.len() < 256 && seen.insert(id),
            "PARQUET_ALLOCATION_LIMIT"
        );
        *last = id;
        Ok(Some((id, kind)))
    }
    fn list(&mut self, kind: u8) -> Result<(usize, u8)> {
        ensure!(kind == 9 || kind == 10, "PARQUET_ALLOCATION_LIMIT");
        let head = self.byte()?;
        let size = if head >> 4 == 15 {
            usize::try_from(self.variable()?)?
        } else {
            usize::from(head >> 4)
        };
        ensure!(
            size <= 4096 && (size == 0 || (1..=12).contains(&(head & 15))),
            "PARQUET_ALLOCATION_LIMIT"
        );
        Ok((size, head & 15))
    }
    fn skip(&mut self, kind: u8, depth: usize) -> Result<()> {
        self.item()?;
        ensure!(depth <= 16, "PARQUET_ALLOCATION_LIMIT");
        match kind {
            1 | 2 => {}
            3 => {
                self.take(1)?;
            }
            4..=6 => {
                self.variable()?;
            }
            7 => {
                self.take(8)?;
            }
            8 => {
                let size = usize::try_from(self.variable()?)?;
                self.take(size)?;
            }
            9 | 10 => {
                let (size, element) = self.list(kind)?;
                for _ in 0..size {
                    if element == 1 || element == 2 {
                        ensure!([1, 2].contains(&self.byte()?), "PARQUET_ALLOCATION_LIMIT");
                    } else {
                        self.skip(element, depth + 1)?;
                    }
                }
            }
            11 => {
                let size = usize::try_from(self.variable()?)?;
                ensure!(size <= 4096, "PARQUET_ALLOCATION_LIMIT");
                if size > 0 {
                    let kinds = self.byte()?;
                    for _ in 0..size {
                        for element in [kinds >> 4, kinds & 15] {
                            if element == 1 || element == 2 {
                                ensure!([1, 2].contains(&self.byte()?), "PARQUET_ALLOCATION_LIMIT");
                            } else {
                                self.skip(element, depth + 1)?;
                            }
                        }
                    }
                }
            }
            12 => {
                let mut last = 0;
                let mut seen = BTreeSet::new();
                while let Some((_, kind)) = self.field(&mut last, &mut seen)? {
                    self.skip(kind, depth + 1)?;
                }
            }
            _ => anyhow::bail!("PARQUET_ALLOCATION_LIMIT"),
        }
        Ok(())
    }
}

/// Validate raw footer collection claims and schema depth before the library constructs vectors.
pub(super) fn footer(bytes: &[u8]) -> Result<()> {
    ensure!(bytes.len() <= 8 * 1024 * 1024, "PARQUET_ALLOCATION_LIMIT");
    let mut input = Compact::new(bytes, 262144);
    let mut last = 0;
    let mut seen = BTreeSet::new();
    let mut schema = false;
    while let Some((id, kind)) = input.field(&mut last, &mut seen)? {
        if id != 2 {
            input.skip(kind, 0)?;
            continue;
        }
        ensure!(!schema, "PARQUET_ALLOCATION_LIMIT");
        schema = true;
        let (size, element) = input.list(kind)?;
        ensure!(
            (1..=1024).contains(&size) && element == 12,
            "PARQUET_ALLOCATION_LIMIT"
        );
        let mut remaining = vec![1usize];
        for index in 0..size {
            while remaining.last() == Some(&0) {
                remaining.pop();
            }
            ensure!(
                !remaining.is_empty() && remaining.len() <= 4,
                "PARQUET_ALLOCATION_LIMIT"
            );
            *remaining.last_mut().unwrap() -= 1;
            let mut child_count = 0;
            let mut field = 0;
            let mut fields = BTreeSet::new();
            while let Some((id, kind)) = input.field(&mut field, &mut fields)? {
                if id == 5 {
                    child_count = usize::try_from(input.integer(kind)?)?;
                    ensure!(child_count <= 256, "PARQUET_ALLOCATION_LIMIT");
                } else {
                    input.skip(kind, 0)?;
                }
            }
            ensure!(index != 0 || child_count > 0, "PARQUET_ALLOCATION_LIMIT");
            if child_count > 0 {
                remaining.push(child_count);
            }
        }
        ensure!(
            remaining.into_iter().all(|n| n == 0),
            "PARQUET_ALLOCATION_LIMIT"
        );
    }
    ensure!(
        schema && input.position == bytes.len(),
        "PARQUET_ALLOCATION_LIMIT"
    );
    Ok(())
}

/// Inspect the bounded IPC schema before the Parquet adapter converts embedded Arrow metadata.
pub(super) fn arrow_metadata(metadata: &ParquetMetaData) -> Result<()> {
    use arrow_ipc::Type as T;
    use base64::Engine;
    fn validate_field(field: arrow_ipc::Field<'_>, depth: usize, nodes: &mut usize) -> Result<()> {
        *nodes += 1;
        ensure!(
            depth <= 2
                && *nodes <= 512
                && field.dictionary().is_none()
                && field.name().is_some_and(|n| n.len() <= 1024),
            "PARQUET_ALLOCATION_LIMIT"
        );
        if let Some(metadata) = field.custom_metadata() {
            ensure!(metadata.len() <= 128, "PARQUET_ALLOCATION_LIMIT");
            for pair in metadata {
                ensure!(
                    pair.key().is_some_and(|v| v.len() <= 4096)
                        && pair.value().is_none_or(|v| v.len() <= 4096),
                    "PARQUET_ALLOCATION_LIMIT"
                );
            }
        }
        let children = field.children();
        match field.type_type() {
            T::Int => ensure!(
                field
                    .type_as_int()
                    .is_some_and(|v| v.is_signed() && [32, 64].contains(&v.bitWidth())),
                "PARQUET_ALLOCATION_LIMIT"
            ),
            T::FloatingPoint => ensure!(
                field.type_as_floating_point().is_some_and(|v| [
                    arrow_ipc::Precision::SINGLE,
                    arrow_ipc::Precision::DOUBLE
                ]
                .contains(&v.precision())),
                "PARQUET_ALLOCATION_LIMIT"
            ),
            T::Date => ensure!(
                field
                    .type_as_date()
                    .is_some_and(|v| v.unit() == arrow_ipc::DateUnit::DAY),
                "PARQUET_ALLOCATION_LIMIT"
            ),
            T::Timestamp => ensure!(
                field
                    .type_as_timestamp()
                    .is_some_and(|v| v.unit() == arrow_ipc::TimeUnit::MICROSECOND),
                "PARQUET_ALLOCATION_LIMIT"
            ),
            T::Decimal => ensure!(
                field.type_as_decimal().is_some_and(|v| v.bitWidth() == 128
                    && (1..=38).contains(&v.precision())
                    && (0..=v.precision()).contains(&v.scale())),
                "PARQUET_ALLOCATION_LIMIT"
            ),
            _ => {}
        }
        match field.type_type() {
            T::List | T::LargeList => {
                ensure!(
                    depth == 1 && children.is_some_and(|c| c.len() == 1),
                    "PARQUET_ALLOCATION_LIMIT"
                );
                for child in children.unwrap() {
                    validate_field(child, depth + 1, nodes)?;
                }
            }
            T::Bool
            | T::Int
            | T::FloatingPoint
            | T::Utf8
            | T::LargeUtf8
            | T::Utf8View
            | T::Date
            | T::Timestamp
            | T::Decimal => ensure!(
                children.is_none_or(|c| c.is_empty()),
                "PARQUET_ALLOCATION_LIMIT"
            ),
            T::FixedSizeBinary => ensure!(
                children.is_none_or(|c| c.is_empty())
                    && field
                        .type_as_fixed_size_binary()
                        .is_some_and(|v| v.byteWidth() == 16),
                "PARQUET_ALLOCATION_LIMIT"
            ),
            _ => anyhow::bail!("PARQUET_ALLOCATION_LIMIT"),
        }
        Ok(())
    }
    if let Some(pairs) = metadata.file_metadata().key_value_metadata() {
        ensure!(pairs.len() <= 128, "PARQUET_ALLOCATION_LIMIT");
        for pair in pairs {
            if pair.key != "ARROW:schema" {
                ensure!(
                    pair.key.len() <= 4096 && pair.value.as_ref().is_none_or(|v| v.len() <= 4096),
                    "PARQUET_ALLOCATION_LIMIT"
                );
                continue;
            }
            let encoded = pair
                .value
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
            ensure!(encoded.len() <= 8 * 1024 * 1024, "PARQUET_ALLOCATION_LIMIT");
            let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
            let raw = if bytes.len() > 8 && bytes[..4] == [255; 4] {
                &bytes[8..]
            } else {
                bytes.as_slice()
            };
            let message = arrow_ipc::root_as_message(raw)
                .map_err(|_| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
            ensure!(message.bodyLength() == 0, "PARQUET_ALLOCATION_LIMIT");
            let schema = message
                .header_as_schema()
                .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
            if let Some(pairs) = schema.custom_metadata() {
                ensure!(pairs.len() <= 128, "PARQUET_ALLOCATION_LIMIT");
                for pair in pairs {
                    ensure!(
                        pair.key().is_some_and(|v| v.len() <= 4096)
                            && pair.value().is_none_or(|v| v.len() <= 4096),
                        "PARQUET_ALLOCATION_LIMIT"
                    );
                }
            }
            let fields = schema
                .fields()
                .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
            ensure!(
                (1..=256).contains(&fields.len()),
                "PARQUET_ALLOCATION_LIMIT"
            );
            let mut nodes = 0;
            for value in fields {
                validate_field(value, 1, &mut nodes)?;
            }
        }
    }
    Ok(())
}

pub(super) struct Chunk {
    pub offset: u64,
    pub size: u64,
    values: i64,
    rows: i64,
    uncompressed: i64,
    variable: bool,
    dictionary: bool,
}
/// An upper bound on decoded leaf buffers includes dictionary expansion and offsets.
pub(super) fn metadata(metadata: &ParquetMetaData, file_size: u64) -> Result<Vec<Chunk>> {
    ensure!(
        metadata.num_row_groups() <= 4096,
        "PARQUET_ALLOCATION_LIMIT"
    );
    let mut output = Vec::new();
    for group in metadata.row_groups() {
        ensure!(
            (0..=VALUES).contains(&group.num_rows())
                && group.num_columns() <= 256
                && (0..=GROUP_BYTES as i64).contains(&group.total_byte_size()),
            "PARQUET_ALLOCATION_LIMIT"
        );
        let mut expanded = 0u64;
        let mut compressed = 0u64;
        for column in group.columns() {
            let values = column.num_values();
            let uncompressed = column.uncompressed_size();
            let (offset, size) = column.byte_range();
            ensure!(
                (0..=VALUES).contains(&values)
                    && (0..=GROUP_BYTES as i64).contains(&uncompressed)
                    && size <= PAGE_BYTES as u64
                    && offset.checked_add(size).is_some_and(|end| end <= file_size),
                "PARQUET_ALLOCATION_LIMIT"
            );
            let variable = column.column_type() == Type::BYTE_ARRAY;
            let dictionary = column
                .encodings()
                .any(|e| matches!(e, Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY));
            if variable {
                ensure!(
                    column.encodings().all(|e| matches!(
                        e,
                        Encoding::PLAIN
                            | Encoding::RLE
                            | Encoding::PLAIN_DICTIONARY
                            | Encoding::RLE_DICTIONARY
                    )),
                    "PARQUET_ALLOCATION_LIMIT"
                );
            }
            let decoded = if variable && dictionary {
                u64::try_from(uncompressed)?
                    .checked_mul(u64::try_from(values)?)
                    .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?
            } else {
                u64::try_from(uncompressed)?
            };
            expanded = expanded
                .checked_add(decoded)
                .and_then(|n| n.checked_add(values as u64 * 16))
                .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
            compressed = compressed
                .checked_add(size)
                .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
            ensure!(
                expanded <= GROUP_BYTES && compressed <= GROUP_BYTES,
                "PARQUET_ALLOCATION_LIMIT"
            );
            output.push(Chunk {
                offset,
                size,
                values,
                rows: group.num_rows(),
                uncompressed,
                variable,
                dictionary,
            });
        }
    }
    Ok(output)
}

fn data_header(input: &mut Compact<'_>, v2: bool) -> Result<(i64, i64)> {
    let mut last = 0;
    let mut seen = BTreeSet::new();
    let mut count = None;
    let mut encoding = None;
    while let Some((id, kind)) = input.field(&mut last, &mut seen)? {
        if id == 1 {
            count = Some(input.integer(kind)?);
        } else if id == if v2 { 4 } else { 2 } {
            encoding = Some(input.integer(kind)?);
        } else if v2 && (id == 2 || id == 3 || id == 5 || id == 6) {
            ensure!(
                (0..=PAGE_BYTES as i64).contains(&input.integer(kind)?),
                "PARQUET_ALLOCATION_LIMIT"
            );
        } else {
            input.skip(kind, 0)?;
        }
    }
    Ok((
        count.ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?,
        encoding.ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?,
    ))
}
/// Walk each compressed column without decoding values or allocating claimed page buffers.
pub(super) struct PageWalk {
    position: usize,
    values: i64,
    uncompressed: i64,
    dictionaries: usize,
}
impl PageWalk {
    pub fn new() -> Self {
        Self {
            position: 0,
            values: 0,
            uncompressed: 0,
            dictionaries: 0,
        }
    }
    pub fn position(&self) -> u64 {
        self.position as u64
    }
    /// Parse only a bounded header. Encoded page bodies are skipped without fetching or decoding.
    pub fn header(&mut self, bytes: &[u8], chunk: &Chunk) -> Result<()> {
        let mut input = Compact::new(&bytes[..bytes.len().min(HEADER_BYTES)], 4096);
        let mut last = 0;
        let mut seen = BTreeSet::new();
        let mut kind = None;
        let mut encoded = None;
        let mut decoded = None;
        let mut v1 = None;
        let mut v2 = None;
        let mut dictionary = None;
        while let Some((id, field)) = input.field(&mut last, &mut seen)? {
            match id {
                1 => kind = Some(input.integer(field)?),
                2 => decoded = Some(input.integer(field)?),
                3 => encoded = Some(input.integer(field)?),
                5 | 7 | 8 => {
                    ensure!(field == 12, "PARQUET_ALLOCATION_LIMIT");
                    let header = data_header(&mut input, id == 8)?;
                    match id {
                        5 => v1 = Some(header),
                        7 => dictionary = Some(header),
                        _ => v2 = Some(header),
                    }
                }
                _ => input.skip(field, 0)?,
            }
        }
        let encoded =
            usize::try_from(encoded.ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?)?;
        let decoded = decoded.ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
        ensure!(
            encoded <= PAGE_BYTES && (0..=PAGE_BYTES as i64).contains(&decoded),
            "PARQUET_ALLOCATION_LIMIT"
        );
        match kind {
            Some(0) | Some(3) => {
                let (count, encoding) = if kind == Some(0) {
                    ensure!(v2.is_none(), "PARQUET_ALLOCATION_LIMIT");
                    v1
                } else {
                    ensure!(v1.is_none(), "PARQUET_ALLOCATION_LIMIT");
                    v2
                }
                .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
                ensure!(
                    dictionary.is_none() && (0..=chunk.values).contains(&count),
                    "PARQUET_ALLOCATION_LIMIT"
                );
                if encoding == 2 || encoding == 8 {
                    ensure!(
                        chunk.dictionary && self.dictionaries == 1,
                        "PARQUET_ALLOCATION_LIMIT"
                    );
                }
                if chunk.variable {
                    ensure!([0, 2, 8].contains(&encoding), "PARQUET_ALLOCATION_LIMIT");
                }
                self.values = self
                    .values
                    .checked_add(count)
                    .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
                ensure!(self.values <= chunk.values, "PARQUET_ALLOCATION_LIMIT");
            }
            Some(2) => {
                let (count, encoding) =
                    dictionary.ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
                self.dictionaries += 1;
                ensure!(
                    chunk.dictionary
                        && self.dictionaries == 1
                        && self.values == 0
                        && v1.is_none()
                        && v2.is_none()
                        && (0..=VALUES).contains(&count)
                        && encoding == 0,
                    "PARQUET_ALLOCATION_LIMIT"
                );
            }
            _ => anyhow::bail!("PARQUET_ALLOCATION_LIMIT"),
        }
        self.uncompressed = self
            .uncompressed
            .checked_add(decoded + input.position as i64)
            .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
        self.position = self
            .position
            .checked_add(input.position)
            .and_then(|n| n.checked_add(encoded))
            .ok_or_else(|| anyhow::anyhow!("PARQUET_ALLOCATION_LIMIT"))?;
        ensure!(
            self.position <= chunk.size as usize && self.uncompressed <= chunk.uncompressed,
            "PARQUET_ALLOCATION_LIMIT"
        );
        Ok(())
    }
    pub fn finish(&self, chunk: &Chunk) -> Result<()> {
        ensure!(
            self.position as u64 == chunk.size
                && self.values == chunk.values
                && self.uncompressed == chunk.uncompressed
                && chunk.rows <= VALUES,
            "PARQUET_ALLOCATION_LIMIT"
        );
        Ok(())
    }
}
#[cfg(test)]
pub(super) fn pages(bytes: &[u8], chunk: &Chunk) -> Result<()> {
    ensure!(bytes.len() as u64 == chunk.size, "PARQUET_ALLOCATION_LIMIT");
    let mut walk = PageWalk::new();
    while walk.position() < chunk.size {
        walk.header(&bytes[walk.position as usize..], chunk)?;
    }
    walk.finish(chunk)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn integer(value: i64) -> Vec<u8> {
        let mut value = ((value << 1) ^ (value >> 63)) as u64;
        let mut bytes = Vec::new();
        loop {
            let byte = (value & 127) as u8;
            value >>= 7;
            bytes.push(byte | if value == 0 { 0 } else { 128 });
            if value == 0 {
                return bytes;
            }
        }
    }
    fn page(decoded: i64, values: i64) -> Vec<u8> {
        let mut p = vec![0x15, 0];
        p.push(0x15);
        p.extend(integer(decoded));
        p.extend([0x15, 0, 0x2c, 0x15]);
        p.extend(integer(values));
        p.extend([0x15, 0, 0x15, 6, 0x15, 6, 0, 0]);
        p
    }
    fn encoded(values: usize, width: usize, repeated: bool, dictionary: bool) -> Vec<u8> {
        use parquet::{
            column::writer::ColumnWriter,
            data_type::ByteArray,
            file::{properties::WriterProperties, writer::SerializedFileWriter},
            schema::parser::parse_message_type,
        };
        use std::sync::Arc;
        let schema=Arc::new(parse_message_type(if repeated {
            "message schema { optional group tags (LIST) { repeated group list { required binary element (UTF8); } } }"
        } else {"message schema { required binary value (UTF8); }"}).unwrap());
        let properties = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(dictionary)
                .set_dictionary_page_size_limit(8 * 1024 * 1024)
                .build(),
        );
        let mut bytes = Vec::new();
        let mut file = SerializedFileWriter::new(&mut bytes, schema, properties).unwrap();
        let mut group = file.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        let data = vec![ByteArray::from(vec![b'x'; width]); values];
        let definition = vec![2i16; values];
        let mut repetition = vec![1i16; values];
        repetition[0] = 0;
        if let ColumnWriter::ByteArrayColumnWriter(writer) = column.untyped() {
            writer
                .write_batch(
                    &data,
                    repeated.then_some(definition.as_slice()),
                    repeated.then_some(repetition.as_slice()),
                )
                .unwrap();
        } else {
            panic!("expected byte array");
        }
        column.close().unwrap();
        group.close().unwrap();
        file.close().unwrap();
        bytes
    }
    #[test]
    fn real_dictionary_array_expansion_is_rejected_before_decode() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let bytes = encoded(128, 1024 * 1024, true, true);
        assert!(bytes.len() < 2 * 1024 * 1024);
        let size = bytes.len() as u64;
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().row_group(0).num_rows(), 1);
        assert_eq!(reader.metadata().row_group(0).column(0).num_values(), 128);
        assert!(metadata(reader.metadata(), size).is_err());
    }
    #[test]
    fn real_plain_and_small_dictionary_columns_have_exact_bounded_walks() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        for (count, width, repeated, dictionary) in
            [(1, 1024 * 1024, false, false), (128, 8, true, true)]
        {
            let bytes = encoded(count, width, repeated, dictionary);
            let reader = SerializedFileReader::new(bytes::Bytes::from(bytes.clone())).unwrap();
            let chunks = metadata(reader.metadata(), bytes.len() as u64).unwrap();
            for chunk in chunks {
                pages(
                    &bytes[chunk.offset as usize..(chunk.offset + chunk.size) as usize],
                    &chunk,
                )
                .unwrap();
            }
            footer(
                &bytes[bytes.len()
                    - 8
                    - u32::from_le_bytes(
                        bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap(),
                    ) as usize..bytes.len() - 8],
            )
            .unwrap();
        }
    }
    #[test]
    fn real_page_header_size_claim_is_rejected_before_decode() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let file = encoded(1, 1024 * 1024, false, false);
        let reader = SerializedFileReader::new(bytes::Bytes::from(file.clone())).unwrap();
        let mut chunks = metadata(reader.metadata(), file.len() as u64).unwrap();
        let chunk = chunks.remove(0);
        let mut bytes = file[chunk.offset as usize..(chunk.offset + chunk.size) as usize].to_vec();
        let mut input = Compact::new(&bytes, 4096);
        let mut last = 0;
        let mut seen = BTreeSet::new();
        let mut replace = None;
        while let Some((id, kind)) = input.field(&mut last, &mut seen).unwrap() {
            if id == 2 {
                let start = input.position;
                input.integer(kind).unwrap();
                replace = Some(start..input.position);
                break;
            }
            input.skip(kind, 0).unwrap();
        }
        let range = replace.unwrap();
        let oversized = integer(64 * 1024 * 1024);
        assert_eq!(range.len(), oversized.len());
        bytes[range].copy_from_slice(&oversized);
        assert!(pages(&bytes, &chunk).is_err());
    }
    #[test]
    fn oversized_page_rejected_before_any_decode() {
        let p = page(i32::MAX as i64, 1);
        let c = Chunk {
            offset: 0,
            size: p.len() as u64,
            values: 1,
            rows: 1,
            uncompressed: i32::MAX as i64,
            variable: true,
            dictionary: false,
        };
        assert!(pages(&p, &c).is_err());
    }
    #[test]
    fn oversized_value_count_rejected_before_any_decode() {
        let p = page(0, i32::MAX as i64);
        let c = Chunk {
            offset: 0,
            size: p.len() as u64,
            values: 1,
            rows: 1,
            uncompressed: p.len() as i64,
            variable: true,
            dictionary: false,
        };
        assert!(pages(&p, &c).is_err());
    }
    #[test]
    fn encoded_column_walk_is_exact() {
        let p = page(0, 1);
        let c = Chunk {
            offset: 0,
            size: p.len() as u64,
            values: 1,
            rows: 1,
            uncompressed: p.len() as i64,
            variable: true,
            dictionary: false,
        };
        pages(&p, &c).unwrap();
        let mut extra = p.clone();
        extra.push(0);
        assert!(pages(&extra, &c).is_err());
    }
    #[test]
    fn raw_collection_claim_is_rejected_without_allocating() {
        assert!(footer(&[0x29, 0xfc, 0xff, 0xff, 0xff, 0xff, 7]).is_err());
    }
    #[test]
    fn compact_nesting_is_bounded() {
        let bytes = [vec![0x1c; 18], vec![0; 19]].concat();
        let mut input = Compact::new(&bytes, 1000);
        assert!(input.skip(12, 0).is_err());
    }
}
