use anyhow::{Result, ensure};
use serde::Serialize;
use std::collections::VecDeque;

pub const CHUNKER_REVISION: &str = "unicode1000-overlap100-paragraph-v1";

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TextChunk {
    pub ordinal: usize,
    pub start: usize,
    pub end: usize,
    pub text: String,
}

/// Keep only the offsets in the current chunk window, including its end boundary.
pub fn chunks(text: &str) -> Result<Vec<TextChunk>> {
    ensure!(text.len() <= 8 * 1024 * 1024, "SEARCH_DOCUMENT_LIMIT");
    const WINDOW: usize = 1000;
    const OVERLAP: usize = 100;
    let mut boundaries = text
        .char_indices()
        .map(|(offset, _)| offset)
        .chain(std::iter::once(text.len()))
        .peekable();
    let mut offsets = VecDeque::with_capacity(WINDOW + 1);
    let mut start = 0;
    let mut chunks = Vec::new();
    loop {
        offsets.extend((&mut boundaries).take(WINDOW + 1 - offsets.len()));
        let hard_end = offsets.len() - 1;
        if hard_end == 0 {
            break;
        }
        let last = boundaries.peek().is_none();
        let end = if last {
            hard_end
        } else {
            // Include the separator; never let paragraph preference prevent progress.
            (OVERLAP + 1..hard_end)
                .rev()
                .find(|&i| {
                    &text[offsets[i - 1]..offsets[i]] == "\n"
                        && &text[offsets[i]..offsets[i + 1]] == "\n"
                })
                .map_or(hard_end, |i| i + 1)
        };
        chunks.push(TextChunk {
            ordinal: chunks.len(),
            start,
            end: start + end,
            text: text[offsets[0]..offsets[end]].into(),
        });
        if last {
            break;
        }
        let advance = end - OVERLAP;
        start += advance;
        offsets.drain(..advance);
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
    fn paragraph_offsets_survive_window_refills_and_exact_end() {
        let paragraph = format!("{}\n\n", "🦀".repeat(698));
        let text = paragraph.repeat(3) + &"λ".repeat(400);
        let result = chunks(&text).unwrap();
        assert_eq!(
            result
                .iter()
                .map(|chunk| (chunk.start, chunk.end))
                .collect::<Vec<_>>(),
            [(0, 700), (600, 1400), (1300, 2100), (2000, 2500)]
        );

        let exact = "λ".repeat(898) + "\n\n" + &"中".repeat(100);
        assert_eq!(chunks(&exact).unwrap()[0].end, 1000);
        assert_eq!(chunks(&(exact + "中")).unwrap()[0].end, 900);
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
