use anyhow::{Result, ensure};
use serde::Serialize;

pub const CHUNKER_REVISION: &str = "unicode1000-overlap100-paragraph-v1";

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TextChunk {
    pub ordinal: usize,
    pub start: usize,
    pub end: usize,
    pub text: String,
}

/// The caller's document bound limits both this offset index and returned text.
pub fn chunks(text: &str) -> Result<Vec<TextChunk>> {
    ensure!(text.len() <= 8 * 1024 * 1024, "SEARCH_DOCUMENT_LIMIT");
    let mut offsets: Vec<usize> = text.char_indices().map(|(offset, _)| offset).collect();
    offsets.push(text.len());
    let count = offsets.len() - 1;
    let mut start = 0;
    let mut chunks = Vec::new();
    while start < count {
        let hard_end = (start + 1000).min(count);
        let end = if hard_end < count {
            // Include the separator; never let paragraph preference prevent progress.
            (start + 101..hard_end)
                .rev()
                .find(|&i| {
                    &text[offsets[i - 1]..offsets[i]] == "\n"
                        && &text[offsets[i]..offsets[i + 1]] == "\n"
                })
                .map_or(hard_end, |i| i + 1)
        } else {
            hard_end
        };
        chunks.push(TextChunk {
            ordinal: chunks.len(),
            start,
            end,
            text: text[offsets[start]..offsets[end]].into(),
        });
        if end == count {
            break;
        }
        start = end - 100;
    }
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_offsets_preserve_complete_text_with_exact_overlap() {
        for text in [
            String::new(),
            "λ🦀中".repeat(1100),
            "x".repeat(1001),
            "\n".repeat(2200),
        ] {
            let result = chunks(&text).unwrap();
            let original: Vec<char> = text.chars().collect();
            assert_eq!(result.is_empty(), original.is_empty());
            let mut restored = String::new();
            for (index, chunk) in result.iter().enumerate() {
                assert_eq!(chunk.ordinal, index);
                assert_eq!(
                    chunk.text,
                    original[chunk.start..chunk.end].iter().collect::<String>()
                );
                assert!(chunk.end - chunk.start <= 1000);
                if index == 0 {
                    assert_eq!(chunk.start, 0);
                    restored.push_str(&chunk.text);
                } else {
                    assert_eq!(chunk.start, result[index - 1].end - 100);
                    restored.extend(chunk.text.chars().skip(100));
                }
            }
            assert_eq!(restored, text);
        }
    }

    #[test]
    fn paragraph_preference_is_deterministic_and_preserves_delimiters() {
        let text = format!("{}\n\n{}", "a".repeat(700), "b".repeat(600));
        let result = chunks(&text).unwrap();
        assert_eq!(result[0].end, 702);
        assert_eq!(result[1].start, 602);
        assert_eq!(result[1].end, 1302);
        assert!(result[0].text.ends_with("\n\n"));
        assert!(chunks(&"x".repeat(8 * 1024 * 1024 + 1)).is_err());
    }
}
