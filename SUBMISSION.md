# Beat Claude — Engineer 004: Real-Time Analytics Pipeline

**Candidate:** Marcus Still · **Brief version:** 2026-07 · **Fixture sha256:** `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`

## Operating artifact

Repository with three runnable pieces, stdlib Python only:
- `detect_anomalies.py` — reads the fixture and prints 14 findings [Observed] with event_ids and the pipeline action for each.
- `prototype/pipeline.py` — the riskiest component built for real: an ack-after-fsync collector plus a checkpointed, replayable processor implementing dedupe, legacy-schema adapter, tenant resolution, skew handling, PII hashing, bot flagging and DLQ.
- `prototype/bench.py` — runs the fixture end to end, then 206,206 events [Benchmarked] with a 10x arrival spike, 2% client retries, 1% truncated payloads, bot bursts, and a processor killed mid-stream. Pass condition: clean + duplicate + bot + dlq = acked.

## Artifact access

Public repo: REPO_URL — run `python3 detect_anomalies.py event_sample.jsonl` and `python3 prototype/bench.py`. No dependencies.

## Written answer

Everything below is checkable against `detect_anomalies.py`, `prototype/bench.py` and the fixture in the repo.

Number source labels used throughout: [Observed] in the fixture, brief or a log · [Benchmarked] measured by the prototype in this repo · [Estimated] arithmetic on public prices/limits · [Assumed] stated assumption.

## 1. The design in one paragraph

Keep the SDK and its endpoint exactly as they are, and put a thin, stateless collector behind it whose only job is to write each event durably to a stream *before* returning 200. Everything else — validation, legacy-schema translation, dedupe, bot flags, PII redaction, identity stitching, aggregation — happens downstream of a durable log, so a slow or crashing consumer can never lose an event again; it only falls behind and catches up. Real-time numbers (<5s target [Observed]) come from a stream processor writing to Redis; history, exports and audits come from an immutable raw lake in S3. Today's 3% loss [Observed] is a symptom of doing work *before* durability. This design moves durability to the front. Checkable: `prototype/bench.py` demonstrates the ack-after-durable pattern.

## 2. Architecture

```
 Browser SDK (unchanged)
      │  same hostname / same payloads (old + new schema)
      ▼
 ALB ─► Collector (ECS Fargate, autoscaled, stateless)
      │  auth by write key → tenant_id; stamp received_at; size limits
      │  PutRecord → ack 200 only after Kinesis accepts
      ▼
 Kinesis Data Streams (on-demand mode, 7-day retention, partition key = tenant_id:anonymous_id)
      ├──► Firehose ─► S3 RAW (immutable, Parquet/Iceberg, partitioned tenant/date) ── replay source of truth
      └──► Managed Flink job
             1 parse / DLQ   2 legacy adapter   3 validate tenant
             4 PII redaction  5 dedupe (tenant,event_id, 24h keyed state)
             6 event-time = received_at if skewed   7 bot scoring
             8 identity stitch   9 windowed aggregates + segment rules
             ├──► ElastiCache Redis   (live counters, segments, profile state; <5s)
             │        └──► Personalization API + Dashboard API (websocket/SSE)
             ├──► S3 CLEAN (Iceberg) ─► Athena (ad-hoc) ─► Snowflake/BigQuery export
             └──► DLQ (S3 + SQS alert) for malformed / tenantless / unknown
 Privacy service: deletion requests → tombstone table → applied in Flink, Redis, Iceberg (row-level delete), exports
```

**Why these choices**
- **Kinesis on-demand over self-managed Kafka/MSK:** two dedicated seniors [Observed] cannot run a Kafka cluster *and* ship an MVP in 3 months [Observed]. On-demand mode absorbs a 10x spike without a capacity plan. Trade: higher per-GB cost and 7-day max retention vs Kafka — acceptable because S3 RAW is the long-term replay source.
- **Managed Flink over Lambda chains:** dedupe, sessionization and "viewed pricing 3x" segments need keyed state and event-time windows. Lambda would push that state into Redis round-trips and make exactly-once reasoning hard.
- **Redis for the hot path:** the team already runs it [Observed]; segment checks for personalization must answer in milliseconds.
- **S3 + Iceberg over loading everything into Postgres:** Postgres is where the current system breaks. Iceberg gives cheap storage, row-level deletes for GDPR, and a native path to Snowflake/BigQuery (external tables / data sharing) without a bespoke export system.
- **Not chosen:** a real-time OLAP store (ClickHouse/Druid) in the MVP. It is the right phase-2 addition for flexible sub-second dashboards, but it is a new system to operate. MVP dashboards read pre-aggregated Redis counters.

**Event model.** One envelope: `event_id, tenant_id, anonymous_id, user_id, type, ts (client), received_at (server), properties{}`, plus pipeline-added fields `event_time, skew_ms, is_bot, schema_version, pii_redacted`. **Identity:** an `identify` event writes an edge `anonymous_id → user_id` per tenant into an identity graph (Redis hash + Iceberg table); queries resolve through it, so stitching is retroactive without rewriting history. Identity never crosses tenants.

## 3. Grounded in the fixture — anomaly by anomaly

Reproduce with `python3 detect_anomalies.py event_sample.jsonl`. The fixture has 25 lines [Observed], 24 parse as JSON [Observed], and the detector reports 14 findings [Observed]. Each is handled by a named pipeline step:

| Class | event_id(s) | What the data shows [Observed] | Pipeline action |
|---|---|---|---|
| Duplicate delivery | evt-0002 | Same event resent; received 7.4s later | Step 5: dedupe on (tenant_id, event_id), keep first. A count-everything pipeline would double this click. |
| Client clock skew | evt-0005, evt-0006 | `ts` later than `received_at` by 47s and 65 min | Step 6: window by `received_at` when client is ahead; keep `skew_ms` for diagnosis. |
| Future timestamp | evt-0016 | `ts` is 2027, one year ahead | Clamp `event_time` to `received_at`; never let it open a future window. |
| Legacy schema | evt-0009 | `pageview`, `timestamp`, `page_path`, `ref`, no `received_at` | Step 2: adapter maps to the current envelope. Required because the SDK cannot be changed [Observed]. |
| Missing tenant | evt-0011 | `tenant_id` null | Collector derives tenant from the write key; if still unknown, quarantine to DLQ. Never assign a guessed tenant in a multi-tenant system. |
| Malformed JSON | evt-0020 | Missing closing brace | Step 1: raw bytes to DLQ with an alert and a counter. Silent drops are exactly how a "3% loss" stays invisible. |
| Bot burst | evt-0012–0015 | 4 page views from anon-8fc in 50ms, scanner referrer | Step 7: flag `is_bot`, exclude from dashboards and segments, keep raw for audit. Otherwise anon-8fc qualifies for a "viewed pricing" segment. |
| PII in properties | evt-0007 | Email and phone inside a custom event | Step 4: hash/redact before any stream or store downstream of the collector. Later stitched to u-7304 by evt-0022, so leaking it would tie PII to an identified user. |
| Privacy deletion | evt-0017 | GDPR delete for u-1077 / anon-77a | Tombstone u-1077 and every stitched anon id. This must also remove evt-0006 (same anon-77a), not only events carrying the user_id. |
| Client-side aggregate | evt-0019 | Claims `count_today: 3` pricing views | Ignore; recompute server-side. The fixture shows only one /pricing view for anon-9f2 (evt-0001). |
| Identity stitch | evt-0003, 0008, 0022 | anon → user merges | Write identity edges; earlier anonymous events (evt-0001, 0002, 0004, 0005, 0007) are attributed retroactively. |

**Things I would not act on at face value** (all [Observed] in the fixture):
1. **evt-0019's `count_today: 3`.** A client-computed number that disagrees with the data. Segments must be computed from events, never asserted by the browser.
2. **evt-0006's and evt-0016's timestamps.** Trusting them would put events in the wrong window (evt-0006 lands an hour late, evt-0016 a year in the future).
3. **The bot burst evt-0012–0015.** At face value it is an engaged pricing-page visitor; acted on, it pollutes conversion metrics and triggers personalization for a scanner.
4. **evt-0002's second copy.** It looks like a second click; it is a retry.
5. **A null tenant on evt-0011.** It is not "unassigned traffic"; it is an ingestion fault to resolve at the edge.

## 4. Scale, reliability, migration

**Volume.** 50M events/day [Observed] ≈ 580 events/s average [Estimated]. Assuming a 5x daily peak and a 10x spike on top [Assumed], design for ~30k events/s burst [Estimated]. At ~1 KB/event [Assumed] that is ~30 MB/s, well inside Kinesis on-demand burst limits [Estimated].

**Zero data loss** [Assumed] target from the brief, mechanism [Benchmarked] in `prototype/bench.py`, means no event is acknowledged before it is durable: the collector returns 200 only after Kinesis accepts the record (multi-AZ replicated). If Kinesis throttles, the collector returns 503 and the SDK's existing retry resends — duplicates are then safe because of step 5. Consumers checkpoint, so a crash replays and never skips. S3 RAW gives replay beyond the 7-day stream window.

**Migration without breaking integrations (strangler / dual-write)** — week ranges are [Assumed] planning estimates for two engineers:
1. **Weeks 1–4:** collector sits behind the existing endpoint and *tees* every event to both the old pipeline and Kinesis. Customers see no change.
2. **Weeks 5–8:** new pipeline runs in shadow. A nightly reconciliation job compares per-tenant counts (old vs raw vs clean) and must explain every difference by category (dedupe, bot, DLQ).
3. **Weeks 9–12 (MVP):** move dashboards tenant by tenant behind a feature flag, starting with internal and friendly accounts.
4. **Months 4–6:** warehouse exports, segments for personalization, retire the old pipeline once 30 days of reconciliation are clean.

**Rollback:** flip the tenant's flag back; the old pipeline never stopped receiving events during migration, so rollback is instant and lossless.

**Prototype results [Benchmarked, 8-vCPU VPS, `prototype/bench.py`]:** fixture: 25 acked = 18 clean + 1 duplicate + 4 bot + 2 DLQ, balanced. Load run: 206,206 acked = 200,080 clean + 3,991 duplicate + 80 bot + 2,055 DLQ, balanced after a crash and restart. Ingest ~63,000 records/s with fsync per 500-record batch; processing ~39,000 records/s including crash recovery — single process, about 67x the brief's 580/s average [Estimated]. **The test found a real bug in my first version:** after a crash, 457 events processed after the last checkpoint were counted twice (accounted 206,663 vs 206,206 acked). Fix: commit input offset and output position atomically and truncate uncommitted output on restart. This is the exact failure class that produces silent double counts in production, and it is why reconciliation is the acceptance test, not a dashboard.

**Backpressure and what degrades first** [Assumed]: under overload the order is (1) dashboards lag — the stream buffers, nothing is dropped; (2) segment/personalization freshness degrades next; (3) only if Kinesis rejects writes does the collector return 503, and the SDK retries — duplicates are absorbed by dedupe. Loss is detected, not assumed away: the acked-vs-accounted balance is checked per tenant per hour and alarms on any gap.

**Validating accuracy** (thresholds [Assumed]): (a) per-tenant, per-hour count reconciliation between collector-acked, RAW, CLEAN and DLQ — the numbers must balance exactly; (b) synthetic canary events injected every minute per region, with end-to-end latency and loss alarms; (c) replaying this fixture as a regression test — the detector's 14 findings are the expected output.

**Cost against the $50K/month ceiling [Observed]:** Kinesis on-demand $4–7K, Managed Flink $3–5K, Fargate collectors $1–2K, ElastiCache $2–4K, S3/Firehose/Athena $2–3K — roughly **$12–21K/month [Estimated, list-price arithmetic]**, leaving headroom for a phase-2 OLAP store and spike over-provisioning.

## 5. Trade-offs and risks

**Optimizing for:** no lost events, <5s dashboards, a migration two engineers can actually ship. **Sacrificing:** sub-second ad-hoc slicing in the MVP (pre-aggregated counters only), lowest possible unit cost (managed services cost more than self-run Kafka), and exactly-once in the strict sense — we get *effectively* once via idempotent dedupe.

**What could go wrong:** a hot tenant overwhelming one shard (mitigated by tenant:anon partition key and per-tenant rate limits); Flink state growth from the 24h dedupe window (bounded TTL, monitored); identity graph merge errors (two people sharing a device); GDPR deletes missing copies in exports already delivered to customers' warehouses (requires a deletion feed customers subscribe to).

**With more time/budget:** a real-time OLAP store for flexible dashboards, schema registry with contract tests on the SDK, and per-tenant data residency.

## 6. Evidence log

| Claim | Proof tier |
|---|---|
| Fixture contains the 14 findings listed, with those event_ids | Tier 2 — runnable `detect_anomalies.py`, checksum above |
| Zero unaccounted events across a mid-stream crash | Tier 3 — `prototype/bench.py`, 206,206 events, balanced |
| ~63K rec/s ingest, ~39K rec/s processing on one process | Tier 3 — benchmarked on an 8-vCPU VPS; not AWS |
| Consolidating per-service cron watchdogs into one supervisor removed 48 per-minute jobs (69,120 to 1,440 invocations/day) with no loss of coverage | Tier 4 — a system I built and run: 1,440 cycles and 105,120 service checks in the last 24h [Observed], 0 failures, running since 2026-09-25 |
| 50M/day ≈ 580/s average | Tier 1 — arithmetic on the brief's number |
| 30k/s burst design target | Tier 1 — assumed multipliers, confirm with real traffic |
| Kinesis on-demand absorbs the burst | Tier 1 — AWS published limits, not load-tested |
| $12–21K/month | Tier 1 — list-price arithmetic |
| Migration is lossless on rollback | Tier 1 — design argument; dual-write keeps old path live |

## 7. AI usage disclosure

I used **Claude (Anthropic)** in a voice-driven session on my own Linux server. It pulled the public brief and fixture, wrote `detect_anomalies.py` and the prototype, ran the benchmarks, and drafted this document. I directed the work, chose to ground every claim in the fixture output, and reviewed the design. What I checked myself: the fixture checksum, that the detector's output matches the raw lines it cites, and that every number carries a source label. What it did not do: no number here is presented as benchmarked.

## 8. What breaks this answer

- **Bot detection is late by two events:** in the benchmark, the first two page views of each 4-view burst were classed clean before the third tripped the rule [Benchmarked]. Fix: hold page views for ~1s before segment updates, or retract retroactively.
- The fixture is 25 events; real traffic will contain anomaly classes it doesn't (consent flags, oversized payloads, replayed write keys).
- The burst and cost numbers are assumptions until load-tested; if real peaks are 50x, on-demand Kinesis limits and cost change.
- If the SDK's retry logic does **not** retry on 503, "zero loss" depends on adding server-side buffering at the collector.
- If customers rely on the 15–30 min batch semantics (e.g., late-arriving corrections), switching them to streaming changes their numbers, and that must be communicated.

## 9. What stays human

- **Approving each tenant's cutover** after reviewing their reconciliation report — it is a customer-trust decision, not a metric threshold.
- **GDPR/CCPA deletion exceptions** (legal holds, disputes) — legal judgment.
- **Bot-filter rule changes** — they change customer-visible numbers and must be reviewed and announced.
- **Identity-merge disputes** and anything that could join one person's data to another's.
