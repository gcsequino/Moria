//! Bounded streaming compaction. At most eight run heads and one bounded block per source stay live.
use super::{
    key_run::{RunEntry, RunKind, RunValue, compare_keys, key_route},
    key_run_io::RunReader,
};
use anyhow::{Result, ensure};
use futures::{StreamExt, stream::BoxStream};
struct Source<'a> {
    stream: BoxStream<'a, Result<RunEntry>>,
    head: Option<RunEntry>,
    previous: Option<Vec<u8>>,
}
struct Merge<'a> {
    sources: Vec<Source<'a>>,
    kind: RunKind,
    bucket: u8,
    remaining: u64,
    started: bool,
}
impl Merge<'_> {
    async fn advance(&mut self, index: usize) -> Result<()> {
        let source = &mut self.sources[index];
        source.head = source.stream.next().await.transpose()?;
        if let Some(entry) = &source.head {
            ensure!(self.remaining > 0, "RESOURCE_LIMIT");
            self.remaining -= 1;
            ensure!(
                !entry.key.is_empty()
                    && entry.key.len() <= 4 * 1024 * 1024
                    && key_route(&entry.key)[0] == self.bucket
                    && source
                        .previous
                        .as_ref()
                        .is_none_or(|previous| compare_keys(previous, &entry.key).is_lt())
                    && matches!(
                        (self.kind, &entry.value),
                        (_, RunValue::Deleted)
                            | (RunKind::Hashes, RunValue::Hash(_) | RunValue::DeferredHash)
                            | (RunKind::Locations, RunValue::Location(_))
                    ),
                "DATASET_KEY_INDEX_INVALID"
            );
            source.previous = Some(entry.key.clone());
        }
        Ok(())
    }
    async fn next(mut self) -> Result<Option<(RunEntry, Self)>> {
        if !self.started {
            for index in 0..self.sources.len() {
                self.advance(index).await?;
            }
            self.started = true;
        }
        let smallest = self
            .sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| source.head.as_ref().map(|head| (index, head)))
            .min_by(|(_, a), (_, b)| compare_keys(&a.key, &b.key))
            .map(|(index, _)| index);
        let Some(index) = smallest else {
            return Ok(None);
        };
        let key = self.sources[index].head.as_ref().unwrap().key.clone();
        // Sources are newest first; full key equality resolves routing-hash collisions.
        let chosen = self
            .sources
            .iter()
            .position(|source| source.head.as_ref().is_some_and(|head| head.key == key))
            .unwrap();
        let entry = self.sources[chosen].head.take().unwrap();
        for source_index in 0..self.sources.len() {
            if source_index == chosen
                || self.sources[source_index]
                    .head
                    .as_ref()
                    .is_some_and(|head| head.key == key)
            {
                self.advance(source_index).await?;
            }
        }
        // A partial merge cannot prove that untouched older levels lack this key.
        Ok(Some((entry, self)))
    }
}
fn ordered_merge<'a>(
    sources: Vec<(u64, BoxStream<'a, Result<RunEntry>>)>,
    kind: RunKind,
    bucket: u8,
    maximum_input_rows: u64,
) -> Result<BoxStream<'a, Result<RunEntry>>> {
    ensure!(
        !sources.is_empty() && sources.len() <= 8 && maximum_input_rows <= 9_007_199_254_740_991,
        "RESOURCE_LIMIT"
    );
    ensure!(
        sources.windows(2).all(|pair| pair[0].0 > pair[1].0),
        "DATASET_KEY_INDEX_INVALID"
    );
    let state = Merge {
        sources: sources
            .into_iter()
            .map(|(_, stream)| Source {
                stream,
                head: None,
                previous: None,
            })
            .collect(),
        kind,
        bucket,
        remaining: maximum_input_rows,
        started: false,
    };
    Ok(Box::pin(futures::stream::try_unfold(state, Merge::next)))
}
pub(crate) fn merge_partition_stream<'a>(
    readers: &'a [RunReader],
    bucket: u8,
    maximum_input_rows: u64,
) -> Result<BoxStream<'a, Result<RunEntry>>> {
    let first = readers
        .first()
        .ok_or_else(|| anyhow::anyhow!("DATASET_KEY_INDEX_INVALID"))?;
    ensure!(
        readers
            .iter()
            .all(|reader| reader.descriptor().kind == first.descriptor().kind
                && reader.descriptor().key_schema_hash == first.descriptor().key_schema_hash),
        "DATASET_KEY_INDEX_INVALID"
    );
    ordered_merge(
        readers
            .iter()
            .map(|reader| {
                (
                    reader.descriptor().sequence,
                    reader.stream_partition(bucket),
                )
            })
            .collect(),
        first.descriptor().kind,
        bucket,
        maximum_input_rows,
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    fn input(sequence: u64, rows: Vec<RunEntry>) -> (u64, BoxStream<'static, Result<RunEntry>>) {
        (
            sequence,
            Box::pin(futures::stream::iter(rows.into_iter().map(Ok))),
        )
    }
    #[tokio::test]
    async fn partial_compaction_retains_deletion_above_untouched_older_runs() {
        let key = b"retired".to_vec();
        let bucket = key_route(&key)[0];
        let tombstone = RunEntry {
            key: key.clone(),
            value: RunValue::Deleted,
        };
        let partial = ordered_merge(
            vec![input(3, vec![tombstone.clone()]), input(2, vec![])],
            RunKind::Hashes,
            bucket,
            10,
        )
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        let final_merge = ordered_merge(
            vec![
                input(4, partial),
                input(
                    1,
                    vec![RunEntry {
                        key,
                        value: RunValue::Hash([1; 32]),
                    }],
                ),
            ],
            RunKind::Hashes,
            bucket,
            10,
        )
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(final_merge, vec![tombstone]);
    }
    #[tokio::test]
    async fn streaming_compaction_keeps_newest_exact_keys_and_fails_ambiguity_or_budget() {
        let mut keys = (0..1000u64)
            .map(|i| i.to_be_bytes().to_vec())
            .filter(|key| key_route(key)[0] == 7)
            .collect::<Vec<_>>();
        keys.sort_by(|a, b| compare_keys(a, b));
        assert!(keys.len() >= 2);
        let old = keys
            .iter()
            .map(|key| RunEntry {
                key: key.clone(),
                value: RunValue::Hash([1; 32]),
            })
            .collect::<Vec<_>>();
        let delta = vec![
            RunEntry {
                key: keys[0].clone(),
                value: RunValue::Deleted,
            },
            RunEntry {
                key: keys[1].clone(),
                value: RunValue::Hash([2; 32]),
            },
        ];
        let rows = ordered_merge(
            vec![input(3, delta), input(2, old.clone())],
            RunKind::Hashes,
            7,
            100,
        )
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(rows.len(), keys.len());
        assert_eq!(rows[0].key, keys[0]);
        assert_eq!(rows[0].value, RunValue::Deleted);
        assert_eq!(rows[1].key, keys[1]);
        assert_eq!(rows[1].value, RunValue::Hash([2; 32]));
        assert!(
            ordered_merge(
                vec![input(2, old.clone()), input(2, vec![])],
                RunKind::Hashes,
                7,
                100
            )
            .is_err()
        );
        assert!(
            ordered_merge(vec![input(2, old)], RunKind::Hashes, 7, 1)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
    }
}
