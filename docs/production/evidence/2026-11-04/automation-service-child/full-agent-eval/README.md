# Full original Agent and Evaluation integration files

Both original files executed sequentially with all assertions and deadlines unchanged against the frozen c89523e alias/inspector cohort (native source digest d1f7f631d8aa5cec91d97eb5815c69b0bda44affc5798c308b8088f95449d196, 79 native units, all-nine artifacts). Exact image: sha256:b9d1727df6293ab4acf24fb6649dd227327bf4b947ea075eb39ba7bfa1932b2b. Each run used fresh independent owned scratch pools.

- Agent: **24/24 pass**, zero skipped/cancelled, 369.568 seconds including cleanup. All three original groups ran. Source automation factory/lost acknowledgement recovery passed in 83.327 seconds.
- Evaluation: **7/9 reported tests pass** (7 of 8 subcases, root wrapper failed), zero skipped/cancelled, 481.555 seconds including cleanup. Repeats/version comparison passed in 135.045 seconds and actual root COMMIT acknowledgement loss/SIGKILL recovery passed in 76.468 seconds.
- The remaining Evaluation failure is the Source automation service evaluation lifecycle barrier. Its private child Functions retained pending _system/log/<uuid> observations and failed with FUNCTION_FAILED; parent evaluations recorded FUNCTION_OBSERVATION_INCOMPLETE. Earlier ordinary Function observations sealed. Exact callback failure diagnosis and a full green rerun are still required.

All controllers, labeled children, owned scratch pools and temporary paths were verified absent after both terminal runs. These results do not qualify the later Connector/ACL combined cohort.
