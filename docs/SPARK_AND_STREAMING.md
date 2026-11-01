# Spark batch job and streaming

## PySpark batch job: `spark_jobs/batch_daily_kpis.py`

Repeats in PySpark what dbt does (dedupe, status clean-up, data-quality flags, daily KPIs with window functions) and writes Parquet
(or Delta with `--format delta`). `tests/test_spark_batch.py` reconciles it with `marts.mart_daily_kpis` in DuckDB, **every metric on every
one of the 90 days, to the cent**.

That reconciliation found a real bug. With money stored as floating point, 96 of 76,321 orders differed by one cent between the two engines
(64.85 x 0.1 is `6.484999999999999` as a double, exactly `6.485` as a decimal; half-way cases then round down in one engine and up in the
other). Money is now an exact decimal in dbt (`numeric`) and in Spark (`decimal`). The commission total moved from 709,494.76 (floats) to
**709,496.31**, which an independent Python `Decimal` calculation reproduces for all 76,321 orders.

## Streaming: `streaming/`

```
order_events.csv --producer--> Kafka/Redpanda topic "order_events" (key = order_id)
                  --stream_job--> parse JSON -> drop duplicate event_ids -> 5-minute windows by event type -> Parquet/Delta
```

| Piece | What it does |
|---|---|
| `producer.py` | replays events in event-time order to Kafka, or to JSON-lines files (`--to-files`) when no broker is available |
| `events.py` | the logic: `parse_events`, `events_per_window` (watermark 10 min, `dropDuplicatesWithinWatermark`, tumbling window) |
| `stream_job.py` | reads Kafka or a folder of files, applies the logic, writes with a checkpoint |

Concepts in this design:
- **Event time vs processing time:** windows use the time inside the event, not the time it arrived.
- **Watermark:** how long to wait for late events (10 minutes). Windows older than the watermark are final, and late events for them are ignored.
- **At-least-once delivery:** the source can send an event twice (1,513 duplicates in the data), so duplicates are removed by `event_id` inside the watermark.
- **Append mode:** a window is written once, when it is final, so output rows never change. The newest windows appear later.
- **Checkpoint:** stores progress and state, so a restart continues instead of repeating or losing events.
- **Chained stateful steps:** dedupe and window counts are two stateful steps; the second learns about a new watermark one micro-batch later, so a late event is dropped once the watermark has passed its window plus one more batch.

## What was verified, and what was not

| Verified (Spark 4.3.0, Java 21, Linux) | Not verified |
|---|---|
| batch job equals the dbt mart on all 90 days | anything that needs a Kafka broker: `--to-kafka`, `--source kafka`, `docker-compose.redpanda.yml` |
| full replay of 304,156 events (11 micro-batches) equals a pandas calculation window by window | `--format delta` (Delta jars come from Maven, which the author's environment cannot reach) |
| late event dropped, in-watermark duplicate counted once, invalid JSON ignored | running Spark natively on Windows |
| Parquet output holds only final, correct windows | |
| producer: ordering, speed, chunking, Kafka key = order id (with a fake Kafka client) | |
| tests pass in two different time zones (an earlier version only passed in UTC) | |

## Run it (Linux, macOS or WSL2)

```
pip install -r requirements-spark.txt
python spark_jobs/batch_daily_kpis.py --batch-dir data/raw/batch_001 --out output/daily_kpis
python streaming/producer.py --events data/raw/batch_001/order_events.csv --to-files stream_in --chunk-size 25000
python streaming/stream_job.py --source files --input stream_in --out output/event_windows --checkpoint output/_ckpt --trigger once
pytest -q tests/test_spark_batch.py tests/test_streaming.py
```
For the Kafka version see the header of `docker-compose.redpanda.yml`. The Kafka source downloads its connector from Maven the first time.
