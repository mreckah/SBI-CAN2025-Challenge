import base64
import json
import os
import time
from pathlib import Path

from hdfs import InsecureClient
from kafka import KafkaConsumer, KafkaProducer


def _env(name: str, default: str) -> str:
    val = os.getenv(name)
    return val if val is not None and val != "" else default


KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = _env("KAFKA_TOPIC", "can2025_data_files")
LOCAL_DATA_DIR = _env("LOCAL_DATA_DIR", "/data")
HDFS_URL = _env("HDFS_URL", "http://namenode:9870")
HDFS_DIR = _env("HDFS_DIR", "/data")
CLEANUP_HDFS = _env("CLEANUP_HDFS", "false").strip().lower() in {"1", "true", "yes", "y"}


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        linger_ms=50,
    )


def get_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="data-pipeline-to-hdfs",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )


def get_hdfs() -> InsecureClient:
    return InsecureClient(HDFS_URL, user="root")


def _local_rel_paths() -> set[str]:
    base = Path(LOCAL_DATA_DIR)
    files = [p for p in base.rglob("*") if p.is_file()]
    return {p.relative_to(base).as_posix() for p in files}


def cleanup_hdfs_extraneous_files() -> int:
    """Delete files in HDFS_DIR that are not present locally under LOCAL_DATA_DIR."""
    hdfs = get_hdfs()
    hdfs.makedirs(HDFS_DIR)

    local_paths = _local_rel_paths()
    deleted = 0

    try:
        hdfs_listing = hdfs.list(HDFS_DIR, status=True)
    except Exception as e:
        raise RuntimeError(f"Failed to list HDFS dir {HDFS_DIR}: {e}")

    # Only delete files at top-level and 1-level deep paths that we uploaded.
    # (Keeps behavior safe and predictable.)
    for name, status in hdfs_listing:
        hdfs_path = f"{HDFS_DIR.rstrip('/')}/{name}"
        if status.get("type") != "FILE":
            continue

        rel_path = name
        if rel_path not in local_paths:
            hdfs.delete(hdfs_path)
            deleted += 1
            print(f"Deleted from HDFS (not in local data): {hdfs_path}")

    return deleted


def produce_all_files() -> int:
    base = Path(LOCAL_DATA_DIR)
    if not base.exists() or not base.is_dir():
        raise RuntimeError(f"LOCAL_DATA_DIR not found or not a dir: {base}")

    files = [p for p in base.rglob("*") if p.is_file()]
    if not files:
        print(f"No files found under {base}")
        return 0

    producer = get_producer()
    count = 0
    for p in files:
        rel_path = p.relative_to(base).as_posix()
        content = p.read_bytes()
        msg = {
            "rel_path": rel_path,
            "filename": p.name,
            "content_b64": base64.b64encode(content).decode("ascii"),
            "content_type": "application/octet-stream",
            "ts_ms": int(time.time() * 1000),
        }
        producer.send(KAFKA_TOPIC, value=msg)
        count += 1
        print(f"Produced to Kafka topic={KAFKA_TOPIC} file={rel_path} bytes={len(content)}")

    producer.flush()
    producer.close()

    # Sentinel to let consumer know when to stop.
    sentinel = {
        "type": "_DONE_",
        "ts_ms": int(time.time() * 1000),
    }
    producer = get_producer()
    producer.send(KAFKA_TOPIC, value=sentinel)
    producer.flush()
    producer.close()

    return count


def consume_and_write_hdfs(timeout_s: int = 120) -> int:
    hdfs = get_hdfs()
    consumer = get_consumer()

    hdfs.makedirs(HDFS_DIR)

    written = 0
    deadline = time.time() + timeout_s

    for msg in consumer:
        val = msg.value
        if isinstance(val, dict) and val.get("type") == "_DONE_":
            print("Received DONE sentinel. Stopping consumer.")
            break

        rel_path = val.get("rel_path")
        b64 = val.get("content_b64")
        if not rel_path or not b64:
            print("Skipping message without rel_path/content_b64")
            continue

        data = base64.b64decode(b64.encode("ascii"))
        target_path = f"{HDFS_DIR.rstrip('/')}/{rel_path}"

        parent = os.path.dirname(target_path)
        if parent:
            hdfs.makedirs(parent)

        with hdfs.write(target_path, overwrite=True) as w:
            w.write(data)

        written += 1
        print(f"Wrote HDFS file={target_path} bytes={len(data)}")

        if time.time() > deadline:
            print(f"Timeout reached ({timeout_s}s). Stopping consumer.")
            break

    consumer.close()
    return written


def main() -> None:
    if CLEANUP_HDFS:
        deleted = cleanup_hdfs_extraneous_files()
        print(f"Cleanup done. Deleted_from_hdfs={deleted}")

    produced = produce_all_files()
    written = consume_and_write_hdfs(timeout_s=180)
    print(f"Done. Produced files={produced}, written_to_hdfs={written}")


if __name__ == "__main__":
    main()
