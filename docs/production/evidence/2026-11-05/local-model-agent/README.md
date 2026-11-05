# Local model selection for the composed Agent fixture

The local thinking checkpoint consumed all 64 requested completion tokens and
returned finish reason length with no answer. The actual composed Model call
failed closed with MODEL_INCOMPLETE. The explicit non-thinking Qwen3 4B
checkpoint returns a complete answer using two tokens for the same prompt, with
the 64-token ceiling unchanged. The existing model digests were preserved.

The actual composed backend retry with the pinned non-thinking checkpoint
passed chat/embedding probes and the normal, Function-tool and real-model Agent
runs. It then reached a fixture-only Dataset namespace join comparing a text
resource reference with a UUID. Both fixture joins now compare the canonical
UUID text; the actual Dataset binding and namespace checks are retained.

This is a local Model/Agent milestone. The full corrected backend run, browser
interaction, restore and production workload remain separate gates.
