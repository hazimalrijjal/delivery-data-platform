"""Replay order_events.csv as a stream of JSON events, to Kafka (Redpanda) or to a folder of JSON-lines files.

    python streaming/producer.py --events data/raw/batch_001/order_events.csv --to-kafka localhost:9092 --topic order_events --speed 600
    python streaming/producer.py --events data/raw/batch_001/order_events.csv --to-files stream_in --chunk-size 20000

Events are replayed in event-time order. --speed N means N simulated seconds per real second (0 = as fast as possible).
The key of every Kafka message is the order id, so all events of one order land in the same partition and keep their order.
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Callable, Iterator


def read_events(csv_path: Path) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return sorted(rows, key=lambda r: (r["event_ts"], r["event_id"]))


def replay(events: list[dict], send: Callable[[dict], None], speed: float = 0, limit: int | None = None,
           sleep: Callable[[float], None] = time.sleep) -> int:
    """Call send(event) for every event in order. With speed > 0, wait so that simulated time passes `speed` times faster."""
    from datetime import datetime
    sent, prev = 0, None
    for ev in events:
        if limit is not None and sent >= limit:
            break
        if speed > 0:
            now = datetime.strptime(ev["event_ts"], "%Y-%m-%d %H:%M:%S")
            if prev is not None and now > prev:
                sleep((now - prev).total_seconds() / speed)
            prev = max(prev, now) if prev else now
        send(ev)
        sent += 1
    return sent


def kafka_sender(bootstrap: str, topic: str):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=20,
                             key_serializer=lambda k: k.encode(), value_serializer=lambda v: json.dumps(v).encode())

    def send(ev: dict) -> None:
        producer.send(topic, key=ev["order_id"], value=ev)

    send.flush = producer.flush
    return send


def file_sender(out_dir: Path, chunk_size: int):
    """Write events into numbered JSON-lines files, `chunk_size` events per file (one file ~ one micro-batch)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    state = {"n": 0, "file": None}

    def send(ev: dict) -> None:
        if state["n"] % chunk_size == 0:
            if state["file"]:
                state["file"].close()
            state["file"] = open(out_dir / f"part-{state['n'] // chunk_size + 1:05d}.json", "w", encoding="utf-8")
        state["file"].write(json.dumps(ev) + "\n")
        state["n"] += 1

    def flush() -> None:
        if state["file"]:
            state["file"].close()

    send.flush = flush
    return send


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", type=Path, required=True)
    dest = p.add_mutually_exclusive_group(required=True)
    dest.add_argument("--to-kafka", metavar="HOST:PORT")
    dest.add_argument("--to-files", type=Path)
    p.add_argument("--topic", default="order_events")
    p.add_argument("--chunk-size", type=int, default=20000)
    p.add_argument("--speed", type=float, default=0)
    p.add_argument("--limit", type=int)
    a = p.parse_args()
    send = kafka_sender(a.to_kafka, a.topic) if a.to_kafka else file_sender(a.to_files, a.chunk_size)
    n = replay(read_events(a.events), send, a.speed, a.limit)
    send.flush()
    print(f"sent {n:,} events")


if __name__ == "__main__":
    main()
