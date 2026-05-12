"""
Generate schema-valid shipment_products and shipment_status JSON files in the landing zone.

Uses the same config and validation path as ingest_raw (get_config → resolve_config →
get_schema → validate_schema → validate_file).

Examples:
    .venv/bin/python src/event-gen.py
    .venv/bin/python src/event-gen.py --partition-date 2026-05-12 -n 10
    .venv/bin/python src/event-gen.py --seed 42
"""

import argparse
import json
import logging
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TypedDict, cast

from jsonschema import Draft202012Validator

from app_logger import configure_logger
from config_util import ResolvedConfig, get_config, resolve_config, _REPO_ROOT_PATH
from ingest_raw import get_schema, validate_file, validate_schema

logger = logging.getLogger(__name__)

data_stream = Literal["shipment_status", "shipment_products"]
shipment_types = Literal["ecommerce", "wholesale", "return"]
shipment_statuses = Literal["start", "complete", "cancel"]

_STATUS_EVENT_NAMES: dict[shipment_statuses, str] = {
    "start": "shipmentStartEvent",
    "complete": "shipmentCompleteEvent",
    "cancel": "shipmentCancelEvent",
}


class BatchContext(TypedDict):
    shipment_business_id: str
    shipment_id: str
    shipment_type: shipment_types


def _parse_partition_date(s: str) -> date:
    return date.fromisoformat(s)


def _utc_day_window(d: date) -> tuple[datetime, datetime]:
    day_start = datetime(d.year, d.month, d.day, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    return day_start, day_end


def _to_iso_z(dt: datetime) -> str:
    dt_utc = dt.astimezone(UTC)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def _ordered_instants(
    rng, day_start: datetime, day_end: datetime, n: int
) -> list[datetime]:
    u = sorted(rng.random() for _ in range(n))
    span = (day_end - day_start).total_seconds()
    return [day_start + timedelta(seconds=t * span) for t in u]


def _pick_shipment_type(rng) -> shipment_types:
    return rng.choice(("ecommerce", "wholesale", "return"))


def _new_batch_context(rng) -> BatchContext:
    return BatchContext(
        shipment_business_id=f"sh_{uuid.uuid4().hex[:12]}",
        shipment_id=f"s_{uuid.uuid4().hex[:12]}",
        shipment_type=_pick_shipment_type(rng),
    )


def _build_products_payload(
    ctx: BatchContext,
    event_timestamp: str,
    event_id: str,
    rng,
) -> dict[str, Any]:
    n_lines = rng.randint(1, 3)
    products = [
        {
            "product_id": f"p_{uuid.uuid4().hex[:8]}",
            "product_quantity": rng.randint(1, 99),
        }
        for _ in range(n_lines)
    ]
    return {
        "event_name": "shipmentProductsAllocated",
        "event_timestamp": event_timestamp,
        "event_id": event_id,
        "event_data": {
            "shipment_id": ctx["shipment_id"],
            "shipment_business_id": ctx["shipment_business_id"],
            "shipment_type": ctx["shipment_type"],
            "products": products,
        },
    }


def _build_status_payload(
    ctx: BatchContext,
    status: shipment_statuses,
    event_timestamp: str,
    status_timestamp: str,
    event_id: str,
) -> dict[str, Any]:
    return {
        "event_name": _STATUS_EVENT_NAMES[status],
        "event_timestamp": event_timestamp,
        "event_id": event_id,
        "event_data": {
            "shipment_id": ctx["shipment_id"],
            "shipment_business_id": ctx["shipment_business_id"],
            "shipment_type": ctx["shipment_type"],
            "status": status,
            "status_timestamp": status_timestamp,
        },
    }


def _status_sequence_for_k(k: int, rng) -> list[shipment_statuses]:
    if k == 0:
        return []
    if k == 1:
        return [cast(shipment_statuses, rng.choice(("start", "complete", "cancel")))]
    return ["start", "complete"]


def _load_stream_validators(
    envt: str,
) -> tuple[dict[data_stream, Draft202012Validator], dict[data_stream, ResolvedConfig]]:
    config_path = _REPO_ROOT_PATH / f"config/{envt}.yaml"
    validators: dict[data_stream, Draft202012Validator] = {}
    resolved: dict[data_stream, ResolvedConfig] = {}
    for data in ("shipment_status", "shipment_products"):
        loaded = get_config(data, config_path)
        res = resolve_config(loaded)
        validators[data] = validate_schema(get_schema(res.schema_path))
        resolved[data] = res
    return validators, resolved


def _write_validated_json(
    path: Path, payload: dict[str, Any], validator: Draft202012Validator
) -> None:
    validate_file(payload, validator)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)
        f.write("\n")


def generate(
    partition_date: date,
    batch_count: int,
    envt: str,
    rng,
) -> None:
    validators, resolved = _load_stream_validators(envt)
    dt_partition = partition_date.isoformat()
    day_start, day_end = _utc_day_window(partition_date)

    seq_products = 0
    seq_status = 0

    for _ in range(batch_count):
        ctx = _new_batch_context(rng)
        k = rng.randint(0, 2)
        times = _ordered_instants(rng, day_start, day_end, k + 1)
        status_seq = _status_sequence_for_k(k, rng)

        event_id_p = str(uuid.uuid4())
        t_p = times[0]
        ts_p = _to_iso_z(t_p)
        products_payload = _build_products_payload(ctx, ts_p, event_id_p, rng)
        seq_products += 1
        out_p = (
            resolved["shipment_products"].lz_pending_path
            / f"dt={dt_partition}"
            / f"{seq_products:06d}_{event_id_p[:8]}.json"
        )
        logger.debug("Writing %s", out_p)
        _write_validated_json(out_p, products_payload, validators["shipment_products"])

        for i, st in enumerate(status_seq):
            t_i = times[i + 1]
            ts_i = _to_iso_z(t_i)
            event_id_s = str(uuid.uuid4())
            status_payload = _build_status_payload(
                ctx, st, ts_i, ts_i, event_id_s
            )
            seq_status += 1
            out_s = (
                resolved["shipment_status"].lz_pending_path
                / f"dt={dt_partition}"
                / f"{seq_status:06d}_{event_id_s[:8]}.json"
            )
            logger.debug("Writing %s", out_s)
            _write_validated_json(
                out_s, status_payload, validators["shipment_status"]
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate validated shipment event JSON files under landing-zone/pending."
    )
    parser.add_argument(
        "--partition-date",
        type=_parse_partition_date,
        default=None,
        help="dt= partition (YYYY-MM-DD). Default: today in UTC.",
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=5,
        metavar="N",
        help="Number of batches (default: 5).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional RNG seed for reproducible output.",
    )
    parser.add_argument(
        "-e",
        "--environment",
        default="dev",
        help="Config environment name (default: dev).",
    )
    args = parser.parse_args()
    partition_date = args.partition_date or datetime.now(UTC).date()
    if args.count < 1:
        parser.error("--count must be at least 1")
    rng = random.Random(args.seed)
    generate(partition_date, args.count, args.environment, rng)


if __name__ == "__main__":
    logger = configure_logger()
    main()
