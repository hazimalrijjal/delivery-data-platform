"""Producer and streaming logic. The Spark tests run real Structured Streaming (local mode) over JSON files; the Kafka source itself
is not exercised (no broker here), only the part that is identical for Kafka: parse -> deduplicate -> window.
Skipped when pyspark/Java are missing or the pipeline has not been run."""
import json
import os
import shutil
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from streaming import producer  # noqa: E402

BATCH = ROOT / "data" / "raw" / "batch_001"
needs_spark = pytest.mark.skipif(shutil.which("java") is None, reason="needs Java")


def ev(i, typ, ts, order="O1"):
    return {"event_id": f"E{i}", "order_id": order, "event_type": typ, "event_ts": ts}


# ---------------------------------------------------------------- producer (no Spark needed)
def test_replay_sorts_by_event_time_and_respects_limit():
    sent = []
    events = [ev(2, "placed", "2026-07-01 10:05:00"), ev(1, "placed", "2026-07-01 10:00:00"), ev(3, "placed", "2026-07-01 10:10:00")]
    n = producer.replay(sorted(events, key=lambda e: e["event_ts"]), sent.append, limit=2)
    assert n == 2 and [e["event_id"] for e in sent] == ["E1", "E2"]


def test_replay_speed_sleeps_for_scaled_gaps():
    sleeps = []
    events = [ev(1, "placed", "2026-07-01 10:00:00"), ev(2, "placed", "2026-07-01 10:10:00")]
    producer.replay(events, lambda e: None, speed=60, sleep=sleeps.append)
    assert sleeps == [10.0]                              # 600 simulated seconds at 60x = 10 real seconds


def test_file_sender_writes_chunks_of_json_lines(tmp_path):
    send = producer.file_sender(tmp_path, chunk_size=2)
    for i in range(5):
        send(ev(i, "placed", "2026-07-01 10:00:00"))
    send.flush()
    files = sorted(tmp_path.glob("part-*.json"))
    assert [len(f.read_text().splitlines()) for f in files] == [2, 2, 1]
    assert json.loads(files[0].read_text().splitlines()[0])["event_id"] == "E0"


def test_kafka_sender_uses_order_id_as_key():
    pytest.importorskip("kafka")
    with patch("kafka.KafkaProducer") as KP:
        send = producer.kafka_sender("localhost:9092", "order_events")
        send(ev(1, "placed", "2026-07-01 10:00:00", order="O77"))
    call = KP.return_value.send.call_args
    assert call.args[0] == "order_events" and call.kwargs["key"] == "O77"
    assert call.kwargs["value"]["event_id"] == "E1"


# ---------------------------------------------------------------- Spark streaming
@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from spark_jobs.session import get_spark
    s = get_spark("test_stream")
    yield s
    s.stop()


def write_files(directory: Path, batches: list[list[dict]]) -> None:
    """One file per batch, with increasing modification times so Spark reads them in this order."""
    directory.mkdir(parents=True, exist_ok=True)
    t = time.time() - 100
    for i, events in enumerate(batches, start=1):
        f = directory / f"part-{i:05d}.json"
        f.write_text("".join(json.dumps(e) + "\n" for e in events))
        os.utime(f, (t + i, t + i))


def collect_updates(spark, input_dir: Path, tmp: Path) -> dict:
    """Run the streaming transform (update mode) to the end and return the latest count per (window start, event type)."""
    from pyspark.sql import functions as F
    from streaming.events import events_per_window, parse_events
    from streaming.stream_job import read_stream
    results: dict = {}

    def sink(df, batch_id):
        # Format the window start to text INSIDE Spark (session time zone = UTC). collect() would convert timestamps to the time zone
        # of this machine and shift every window.
        rows = df.select(F.date_format(F.col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("w"), "event_type", "count").collect()
        for r in rows:
            results[(r["w"], r["event_type"])] = r["count"]

    q = (events_per_window(parse_events(read_stream(spark, "files", input_dir))).writeStream
         .outputMode("update").foreachBatch(sink).option("checkpointLocation", str(tmp / "ckpt"))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return results


@needs_spark
def test_late_events_are_dropped_and_duplicates_counted_once(spark, tmp_path):
    """Four micro-batches (one file each).
    The job chains two stateful steps (deduplicate, then window counts). The second step sees a new watermark one micro-batch after the
    first, so an event is only dropped as late once the watermark has passed its window AND one more batch has been processed.
    That is why the late event arrives in batch 4 and not batch 2."""
    b1 = [ev(1, "placed", "2026-07-01 10:00:00"), ev(2, "placed", "2026-07-01 10:20:00", "O2")]       # watermark -> 10:10
    b2 = [ev(2, "placed", "2026-07-01 10:20:00", "O2"),      # DUPLICATE inside the watermark: counted once
          ev(4, "placed", "2026-07-01 10:21:00", "O4"),
          ev(5, "placed", "2026-07-01 10:40:00", "O5")]                                                 # watermark -> 10:30
    b3 = [ev(6, "placed", "2026-07-01 10:41:00", "O6")]
    b4 = [ev(3, "placed", "2026-07-01 10:01:00", "O3")]      # LATE: window 10:00-10:05 closed long ago, so it is ignored
    write_files(tmp_path / "in", [b1, b2, b3, b4])
    got = collect_updates(spark, tmp_path / "in", tmp_path)
    assert got == {("2026-07-01 10:00:00", "placed"): 1,
                   ("2026-07-01 10:20:00", "placed"): 2,
                   ("2026-07-01 10:40:00", "placed"): 2}


@needs_spark
def test_bad_json_is_ignored(spark, tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    (d / "part-00001.json").write_text('{"event_id":"E1","order_id":"O1","event_type":"placed","event_ts":"2026-07-01 10:00:00"}\n'
                                       'this is not json\n{"event_id":"E2","order_id":"O2","event_type":"placed","event_ts":"yesterday"}\n')
    got = collect_updates(spark, d, tmp_path)
    assert got == {("2026-07-01 10:00:00", "placed"): 1}


@needs_spark
@pytest.mark.skipif(not (BATCH / "order_events.csv").exists(), reason="run the pipeline first")
def test_full_replay_matches_pandas(spark, tmp_path):
    """Replay all 304,156 events (1,513 of them duplicates) in 11 micro-batches and compare every window with pandas."""
    send = producer.file_sender(tmp_path / "in", chunk_size=30000)
    n = producer.replay(producer.read_events(BATCH / "order_events.csv"), send)
    send.flush()
    got = collect_updates(spark, tmp_path / "in", tmp_path)

    df = pd.read_csv(BATCH / "order_events.csv", dtype=str).drop_duplicates("event_id")
    df["window_start"] = pd.to_datetime(df["event_ts"]).dt.floor("5min")
    truth = df.groupby(["window_start", "event_type"]).size()
    expected = {(k[0].strftime("%Y-%m-%d %H:%M:%S"), k[1]): int(v) for k, v in truth.items()}
    assert n == 304156 and sum(got.values()) == 302643 == len(df)
    assert got == expected


@needs_spark
def test_append_parquet_output_contains_only_final_correct_windows(spark, tmp_path):
    from streaming.stream_job import read_stream, start_query
    events, i = [], 0
    for minute in range(0, 120, 3):                            # two hours of events, one every 3 minutes, one duplicate each
        ts = f"2026-07-01 {10 + minute // 60}:{minute % 60:02d}:00"
        events += [ev(i, "placed", ts, f"O{i}")] * 2
        i += 1
    write_files(tmp_path / "in", [events[k:k + 20] for k in range(0, len(events), 20)])
    q = start_query(read_stream(spark, "files", tmp_path / "in"), tmp_path / "out", tmp_path / "ckpt", trigger="once")
    q.awaitTermination()
    from pyspark.sql import functions as F
    out = (spark.read.parquet(str(tmp_path / "out"))
           .select(F.date_format("window_end", "yyyy-MM-dd HH:mm:ss").alias("window_end"), "events").toPandas())
    assert len(out) > 0
    assert (out["events"] >= 1).all()
    # each emitted 5-minute window holds 1 or 2 distinct events (3-minute spacing), never the doubled duplicates
    assert out["events"].max() <= 2
    # the newest windows are not emitted yet: the watermark has not passed their end
    assert out["window_end"].max() < "2026-07-01 11:57:00"       # text in this format sorts like time
