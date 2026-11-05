# Bounded capability-probe streaming

Capability probes expose only their completed protected result. Their bounded
stream decoder was nevertheless calling the full current-authority guard for
every upstream fragment, in addition to the independent500ms live guard and
protected completion. Actual local GPU inference showed succeeded stream and
structured-stream calls spending67.65s and42.77s in that path; the overall real
backend trial failed with MODEL_INCOMPLETE while probing streamTools.

The probe loop now decodes its existing bounded stream under the independent
live guard, then uses the unchanged current-authority completion fence. It retains
the4MiB decoder bound and original120s call deadline. Ordinary user streaming
release checks are unchanged. A new actual HTTP test holds a probe stream after
its first event, revokes the caller, verifies the socket closes within the original
ten-second cancellation bound and proves no later probe dispatch.

Full native Models passes19/19 and Secret custody11/11, zero skips/cancellations.
Full static quality, all nine artifact fingerprints and99 native units pass.
The next real local Qwen3/Nomic instance run passes all six chat/stream/tool/
structured combinations plus embeddings, reaching logic configuration. That
composed run still fails later with an unavailable API; neither retained actual
inference trial is a successful whole-instance qualification. They remain in this
evidence directory, alongside exact commands/source hashes and verified cleanup.

The actual inference driver uses an explicit loopback local provider on its own
host network; canonical offline CI remains isolated. No paid provider or AWS
resource was used. This proves protocol behavior, not inference quality,
production capacity, complete platform integration or final independent CI.
