# Partitioned document pipeline — rollout

This matches the plan in the parent monorepo (partitioner → unit workers → aggregator + Postgres leases + outbox; `FLASH_DOC_PARTITIONED_PCT` bucket flag on evidence-processor).

## Feature flag

- **`FLASH_DOC_PARTITIONED_PCT`** on **evidence-processor** (0–100): hash `evidence_id` to choose `document.partition.requested` vs legacy `document.processing.requested`.
- **Dev soak:** set `100` after new ECS tasks (partitioner, unit worker, aggregator) are healthy.
- **Prod:** ramp **`1 → 10 → 50 → 100`** with gates; watch manifest failure rate, outbox pending age, p95 completion.

## Services (ECS)

Deploy **three** Kafka-driven entrypoints alongside the legacy SQS worker until the flag reaches 100% for two weeks:

| `ENTRYPOINT_MODE` | Consumes | Purpose |
|-------------------|----------|---------|
| `partitioner` | `document.partition.requested` | pikepdf split, manifest + outbox unit requests |
| `unit_worker` | `document.unit.requested` | Docling per chunk, lease + outbox unit completed |
| `aggregator` | `document.unit.completed` | merge, terminal `document.processing.completed` |

Autoscale **unit_worker** on **`document.unit.requested`** consumer lag; **aggregator** on **`document.unit.completed`** lag (MSK CloudWatch `SumOffsetLag`).

## After prod @ 100%

- Remove legacy monolithic doc path and Kafka→SQS bridge when stable.
- Ensure `processing.heartbeat` is fully removed (leases + multi-signal reaper replace it).
