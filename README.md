# Beat Claude — Engineer 004 (brief 2026-07) — Marcus Still

Real-time analytics pipeline design for Single Grain's Engineer 004 challenge, grounded in the fixture and backed by a runnable prototype of the riskiest component.

- **Written answer:** [SUBMISSION.md](SUBMISSION.md) (PDF: `Marcus_Still_BeatClaude_Engineer004.pdf`)
- **Fixture analysis:** `python3 detect_anomalies.py event_sample.jsonl` — 14 findings with event_ids
- **Prototype + benchmark:** `python3 prototype/bench.py` — ack-after-durable collector, checkpointed replayable processor, crash mid-stream, reconciliation must balance
- **Recorded run:** [BENCH_OUTPUT.txt](BENCH_OUTPUT.txt)

Fixture sha256: `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`. Python 3 stdlib only, no dependencies.
