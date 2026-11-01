"""Spark Structured Streaming job: events (Kafka or JSON files) -> parse -> deduplicate -> 5-minute windows -> Parquet/Delta.

    python streaming/stream_job.py --source files --input stream_in --out output/event_windows --checkpoint output/_ckpt --trigger once
    python streaming/stream_job.py --source kafka --bootstrap localhost:9092 --topic order_events --out output/event_windows \
           --checkpoint output/_ckpt [--format delta]

Output mode is "append": a window is written once, after the watermark has passed its end, so the numbers in the output never change.
The newest windows therefore appear only when later events arrive (or the stream ends with the watermark far enough ahead).
The checkpoint folder stores progress and state, so a restart continues where it stopped instead of repeating or losing events.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spark_jobs.session import get_spark  # noqa: E402
from streaming.events import events_per_window, flatten_window, parse_events  # noqa: E402


def read_stream(spark: SparkSession, source: str, input_dir: Path | None = None, bootstrap: str | None = None,
                topic: str = "order_events", max_files_per_trigger: int = 1) -> DataFrame:
    if source == "kafka":
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", bootstrap).option("subscribe", topic)
                .option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()
                .selectExpr("CAST(value AS STRING) AS value"))
    return spark.readStream.option("maxFilesPerTrigger", max_files_per_trigger).text(str(input_dir))   # one row per line, column `value`


def start_query(stream: DataFrame, out: Path, checkpoint: Path, fmt: str = "parquet", trigger: str = "continuous"):
    result = flatten_window(events_per_window(parse_events(stream)))
    writer = (result.writeStream.outputMode("append").format(fmt)
              .option("path", str(out)).option("checkpointLocation", str(checkpoint)))
    writer = writer.trigger(availableNow=True) if trigger == "once" else writer.trigger(processingTime="10 seconds")
    return writer.start()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["kafka", "files"], required=True)
    p.add_argument("--input", type=Path)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="order_events")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--checkpoint", type=Path, required=True)
    p.add_argument("--format", choices=["parquet", "delta"], default="parquet")
    p.add_argument("--trigger", choices=["once", "continuous"], default="continuous",
                   help="once = process everything available, then stop; continuous = keep running")
    a = p.parse_args()
    if a.source == "files" and not a.input:
        raise SystemExit("--input is required with --source files")
    spark = get_spark("stream_job", delta=a.format == "delta", kafka=a.source == "kafka")
    query = start_query(read_stream(spark, a.source, a.input, a.bootstrap, a.topic), a.out, a.checkpoint, a.format, a.trigger)
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
