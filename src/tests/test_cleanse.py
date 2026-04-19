from copy import deepcopy

import pandas as pd
import pytest

from cleanse import (
    CleansedParameters,
    add_shipment_products_uuids,
    add_shipment_status_uuids,
    add_uuids,
    build_uuid,
    merge_df,
)

def test_build_uuid_is_deterministic() -> None:
    uuid_1 = build_uuid("biz-1", "status-a")
    uuid_2 = build_uuid("biz-1", "status-a")
    assert uuid_1 == uuid_2
    assert isinstance(uuid_2, str)
    assert build_uuid("1") != build_uuid("2")
    assert build_uuid("1", "2") != build_uuid("3", "4")

def test_merge_df_merges_df_and_gives_correct_to_id() -> None:
    df = pd.DataFrame(
        {
            "event_id": ["e1", "e1", "e2"],
            "offset_id": [1, 2, 3],
        }
    )
    merged, to_id = merge_df(df, partition_by="event_id", order_by="offset_id")
    assert to_id == 3
    assert len(merged) == 2
    assert int(merged.loc[df["event_id"] == "e1"]["offset_id"].iloc[0]) == 2

def test_add_shipment_status_uuids() -> None:
    payload = {
        "event_data": {
            "shipment_business_id": "sh_1",
            "status": "complete",
        }
    }
    out = add_shipment_status_uuids(payload)
    assert out["event_data"]["shipment_uuid"] == build_uuid("sh_1")
    assert out["event_data"]["shipment_status_uuid"] == build_uuid("sh_1", "complete")


def test_add_shipment_products_uuids() -> None:
    payload = {
        "event_data": {
            "shipment_business_id": "sh_1",
            "products": [
                {"product_id": "p1", "qty": 1},
                {"product_id": "p2", "qty": 2},
            ],
        }
    }
    out = add_shipment_products_uuids(payload)
    assert out["event_data"]["shipment_uuid"] == build_uuid("sh_1")
    products = out["event_data"]["products"]
    assert products[0]["shipment_product_uuid"] == build_uuid("sh_1", "p1")
    assert products[1]["shipment_product_uuid"] == build_uuid("sh_1", "p2")


def test_add_uuids_dispatches_and_leaves_input_untouched() -> None:
    status_payload = {
        "event_data": {
            "shipment_business_id": "sh_1",
            "status": "complete",
        }
    }
    before_changes = deepcopy(status_payload)
    payload_cln = add_uuids(status_payload, "shipment_status")
    assert status_payload == before_changes
    assert payload_cln != before_changes
    assert "shipment_uuid" not in status_payload["event_data"]
    assert "shipment_uuid" in payload_cln["event_data"]
