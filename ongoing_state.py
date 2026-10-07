from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

import os
import sys


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Missing required environment variable: {name}")
        sys.exit(1)
    return value


ONGOING_BASE_URL = require_env("ONGOING_BASE_URL")
ONGOING_USERNAME = require_env("ONGOING_USERNAME")
ONGOING_PASSWORD = require_env("ONGOING_PASSWORD")
ONGOING_GOODS_OWNER_ID = require_env("ONGOING_GOODS_OWNER_ID")

STATE_FILE = Path(os.getenv("ONGOING_STATE_FILE", "ongoing_state.json"))
MAX_ITEMS = 1000
OVERLAP_MINUTES = 5

TERMINAL_CUSTOMER_STATUSES = {500, 900, 1000}
ACTIVE_INCOMING_PO_STATUSES = {100, 400}


def endpoint(resource: str) -> str:
    base = ONGOING_BASE_URL.rstrip("/")
    if not base.lower().endswith("/api/v1"):
        base += "/api/v1"
    return f"{base}/{resource.lstrip('/')}"


def get_list(resource: str, params: dict, description: str) -> list[dict]:
    response = requests.get(
        endpoint(resource),
        params=params,
        auth=HTTPBasicAuth(ONGOING_USERNAME, ONGOING_PASSWORD),
        headers={"Accept": "application/json"},
        timeout=(15, 90),
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"Could not retrieve {description}. HTTP {response.status_code}: "
            f"{response.text[:3000]}"
        ) from exc
    data = response.json()
    if data is None:
        return []
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected Ongoing response for {description}: {type(data).__name__}")
    if len(data) >= MAX_ITEMS:
        raise RuntimeError(
            f"{description} hit the {MAX_ITEMS}-record safety ceiling. "
            "Checkpoint was NOT written."
        )
    return [x for x in data if isinstance(x, dict)]


def num(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def clean(v: float):
    return int(v) if float(v).is_integer() else v


def sku_from_article(article) -> str | None:
    if not isinstance(article, dict):
        return None
    value = article.get("articleNumber")
    if value is None:
        return None
    value = str(value).strip().upper()
    return value or None


def status_from_info(info: dict) -> int | None:
    for key in ("orderStatusNumber", "purchaseOrderStatusNumber", "statusNumber"):
        value = info.get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass
    status = info.get("orderStatus") or info.get("purchaseOrderStatus")
    if isinstance(status, dict):
        for key in ("number", "statusNumber"):
            try:
                if status.get(key) is not None:
                    return int(status[key])
            except (TypeError, ValueError):
                pass
    return None


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def with_overlap(ts: str) -> str:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return (dt - timedelta(minutes=OVERLAP_MINUTES)).isoformat().replace("+00:00", "Z")


def inventory_record(item: dict) -> tuple[str, dict] | None:
    article_info = item.get("articleInfo")
    sku = sku_from_article(article_info) or sku_from_article(item)
    inventory = item.get("inventoryInfo")
    if not sku or not isinstance(inventory, dict):
        return None
    article_id = None
    if isinstance(article_info, dict):
        article_id = article_info.get("articleSystemId") or article_info.get("articleId")
    article_id = article_id or item.get("articleSystemId") or item.get("articleId")
    # Key by stable article id when available; SKU fallback keeps this usable.
    key = str(article_id) if article_id is not None else f"SKU:{sku}"
    return key, {"sku": sku, "physical": clean(max(0.0, num(inventory.get("numberOfItems"))))}


def order_record(item: dict) -> tuple[str, dict] | None:
    info = item.get("orderInfo")
    if not isinstance(info, dict):
        return None
    order_id = info.get("orderId") or item.get("orderId") or item.get("id")
    order_number = str(info.get("orderNumber") or "").strip()
    if order_id is None and not order_number:
        return None
    key = str(order_id) if order_id is not None else f"NUMBER:{order_number}"
    lines = defaultdict(float)
    for line in item.get("orderLines") or []:
        if not isinstance(line, dict):
            continue
        sku = sku_from_article(line.get("article"))
        if not sku:
            continue
        remaining = max(0.0, num(line.get("orderedNumberOfItems")) - num(line.get("pickedNumberOfItems")))
        if remaining:
            lines[sku] += remaining
    return key, {
        "order_number": order_number,
        "status": status_from_info(info),
        "remaining_by_sku": {k: clean(v) for k, v in sorted(lines.items())},
    }


def po_record(item: dict) -> tuple[str, dict] | None:
    info = item.get("purchaseOrderInfo")
    if not isinstance(info, dict):
        return None
    po_id = info.get("purchaseOrderId") or item.get("purchaseOrderId") or item.get("id")
    po_number = str(info.get("purchaseOrderNumber") or "").strip()
    if po_id is None and not po_number:
        return None
    key = str(po_id) if po_id is not None else f"NUMBER:{po_number}"
    lines = defaultdict(float)
    for line in item.get("purchaseOrderLines") or []:
        if not isinstance(line, dict):
            continue
        sku = sku_from_article(line.get("article"))
        if not sku:
            continue
        remaining = max(0.0, num(line.get("advisedNumberOfItems")) - num(line.get("receivedNumberOfItems")))
        if remaining:
            lines[sku] += remaining
    return key, {
        "purchase_order_number": po_number,
        "status": status_from_info(info),
        "remaining_by_sku": {k: clean(v) for k, v in sorted(lines.items())},
    }


def exact_order(order_number: str):
    rows = get_list("orders", {
        "goodsOwnerId": ONGOING_GOODS_OWNER_ID,
        "orderNumber": order_number,
        "maxOrdersToGet": 10,
    }, f"customer order {order_number!r}")
    for item in rows:
        info = item.get("orderInfo")
        if isinstance(info, dict) and str(info.get("orderNumber")) == str(order_number):
            return item
    return None


def exact_po(po_number: str):
    rows = get_list("purchaseOrders", {
        "goodsOwnerId": ONGOING_GOODS_OWNER_ID,
        "purchaseOrderNumber": po_number,
        "maxPurchaseOrdersToGet": 10,
    }, f"purchase order {po_number!r}")
    for item in rows:
        info = item.get("purchaseOrderInfo")
        if isinstance(info, dict) and str(info.get("purchaseOrderNumber")) == str(po_number):
            return item
    return None


def derive(state: dict):
    physical = defaultdict(float)
    for rec in state["records"]["inventory"].values():
        if rec["physical"] > 0:
            physical[rec["sku"]] += num(rec["physical"])

    demand = defaultdict(float)
    for rec in state["records"]["customer_orders"].values():
        if rec.get("status") in TERMINAL_CUSTOMER_STATUSES:
            continue
        for sku, qty in rec.get("remaining_by_sku", {}).items():
            demand[sku] += num(qty)

    incoming = defaultdict(float)
    for rec in state["records"]["purchase_orders"].values():
        if rec.get("status") not in ACTIVE_INCOMING_PO_STATUSES:
            continue
        for sku, qty in rec.get("remaining_by_sku", {}).items():
            incoming[sku] += num(qty)

    all_skus = sorted(set(physical) | set(demand) | set(incoming))
    positions, surplus = {}, {}
    for sku in all_skus:
        p, d, inc = physical[sku], demand[sku], incoming[sku]
        s = max(0.0, p + inc - d)
        positions[sku] = {
            "physical": clean(p), "demand": clean(d),
            "incoming": clean(inc), "local_surplus": clean(s),
        }
        if s > 0:
            surplus[sku] = clean(s)

    state["derived"] = {
        "physical_by_sku": {k: clean(v) for k, v in sorted(physical.items()) if v > 0},
        "customer_demand_by_sku": {k: clean(v) for k, v in sorted(demand.items()) if v > 0},
        "incoming_by_sku": {k: clean(v) for k, v in sorted(incoming.items()) if v > 0},
        "local_surplus_by_sku": dict(sorted(surplus.items())),
        "positions": positions,
    }


def bootstrap(query_started: str) -> dict:
    print("No checkpoint found — BOOTSTRAP")
    articles = get_list("articles", {
        "goodsOwnerId": ONGOING_GOODS_OWNER_ID,
        "onlyArticlesInStock": "true",
        "maxArticlesToGet": MAX_ITEMS,
    }, "positive-stock articles")
    orders = get_list("orders", {
        "goodsOwnerId": ONGOING_GOODS_OWNER_ID,
        "maxOrdersToGet": MAX_ITEMS,
    }, "customer orders")
    pos = get_list("purchaseOrders", {
        "goodsOwnerId": ONGOING_GOODS_OWNER_ID,
        "maxPurchaseOrdersToGet": MAX_ITEMS,
    }, "purchase orders")

    state = {
        "schema_version": 2,
        "safe_for_shopify": True,
        "updated_at_utc": query_started,
        "watermarks": {
            "inventory": query_started,
            "customer_orders": query_started,
            "purchase_orders": query_started,
            "returns": query_started,
        },
        "records": {"inventory": {}, "customer_orders": {}, "purchase_orders": {}},
        "derived": {},
    }
    for item in articles:
        rec = inventory_record(item)
        if rec:
            state["records"]["inventory"][rec[0]] = rec[1]
    for item in orders:
        rec = order_record(item)
        if rec and rec[1]["status"] not in TERMINAL_CUSTOMER_STATUSES:
            state["records"]["customer_orders"][rec[0]] = rec[1]
    for item in pos:
        rec = po_record(item)
        if rec and rec[1]["status"] in ACTIVE_INCOMING_PO_STATUSES:
            state["records"]["purchase_orders"][rec[0]] = rec[1]

    derive(state)
    print(f"Bootstrap fetched: inventory {len(articles)}, orders {len(orders)}, POs {len(pos)}")
    return state


def incremental(state: dict, query_started: str) -> dict:
    if state.get("schema_version") != 2:
        raise RuntimeError("Checkpoint schema is not version 2. Delete ongoing_state.json and bootstrap once.")

    w = state["watermarks"]
    inv_from = with_overlap(w["inventory"])
    ord_from = with_overlap(w["customer_orders"])
    po_from = with_overlap(w["purchase_orders"])
    ret_from = with_overlap(w.get("returns", w["customer_orders"]))

    print("Checkpoint found — INCREMENTAL")
    print(f"Inventory changes from: {inv_from}")
    inv_changes = get_list("articles", {
        "goodsOwnerId": ONGOING_GOODS_OWNER_ID,
        "stockInfoChangedFrom": inv_from,
        "maxArticlesToGet": MAX_ITEMS,
    }, "articles with stock changes")

    print(f"Customer status changes from: {ord_from}")
    order_changes = get_list("orders", {
        "goodsOwnerId": ONGOING_GOODS_OWNER_ID,
        "orderStatusChangedTimeFrom": ord_from,
        "maxOrdersToGet": MAX_ITEMS,
    }, "customer orders with status changes")

    print(f"Returns from: {ret_from}")
    return_changes = get_list("orders", {
        "goodsOwnerId": ONGOING_GOODS_OWNER_ID,
        "lastReturnedFrom": ret_from,
        "maxOrdersToGet": MAX_ITEMS,
    }, "customer orders with returns")

    print(f"PO status changes from: {po_from}")
    po_changes = get_list("purchaseOrders", {
        "goodsOwnerId": ONGOING_GOODS_OWNER_ID,
        "purchaseOrderStatusChangedTimeFrom": po_from,
        "maxPurchaseOrdersToGet": MAX_ITEMS,
    }, "purchase orders with status changes")

    # Inventory change feed includes zero transitions: replace the record.
    for item in inv_changes:
        rec = inventory_record(item)
        if rec:
            state["records"]["inventory"][rec[0]] = rec[1]

    # Changed/returned orders replace their previous contribution.
    changed_orders = {}
    for item in order_changes + return_changes:
        rec = order_record(item)
        if rec:
            changed_orders[rec[0]] = rec
    for key, (_, rec) in changed_orders.items():
        if rec["status"] in TERMINAL_CUSTOMER_STATUSES:
            state["records"]["customer_orders"].pop(key, None)
        else:
            state["records"]["customer_orders"][key] = rec

    # IMPORTANT: status-change polling alone cannot prove line quantities stayed
    # unchanged. Refresh every still-active stored order exactly each run.
    active_orders = list(state["records"]["customer_orders"].items())
    exact_order_refreshes = 0
    for old_key, old in active_orders:
        n = old.get("order_number")
        if not n:
            continue
        item = exact_order(n)
        exact_order_refreshes += 1
        if item is None:
            continue
        rec = order_record(item)
        if not rec:
            continue
        new_key, value = rec
        if new_key != old_key:
            state["records"]["customer_orders"].pop(old_key, None)
        if value["status"] in TERMINAL_CUSTOMER_STATUSES:
            state["records"]["customer_orders"].pop(new_key, None)
        else:
            state["records"]["customer_orders"][new_key] = value

    # Discover status transitions/new POs, then exactly refresh every active PO.
    for item in po_changes:
        rec = po_record(item)
        if not rec:
            continue
        key, value = rec
        if value["status"] in ACTIVE_INCOMING_PO_STATUSES:
            state["records"]["purchase_orders"][key] = value
        else:
            state["records"]["purchase_orders"].pop(key, None)

    active_pos = list(state["records"]["purchase_orders"].items())
    exact_po_refreshes = 0
    for old_key, old in active_pos:
        n = old.get("purchase_order_number")
        if not n:
            continue
        item = exact_po(n)
        exact_po_refreshes += 1
        if item is None:
            continue
        rec = po_record(item)
        if not rec:
            continue
        new_key, value = rec
        if new_key != old_key:
            state["records"]["purchase_orders"].pop(old_key, None)
        if value["status"] in ACTIVE_INCOMING_PO_STATUSES:
            state["records"]["purchase_orders"][new_key] = value
        else:
            state["records"]["purchase_orders"].pop(new_key, None)

    # Advance only after every GET and all processing succeeded.
    state["updated_at_utc"] = query_started
    state["watermarks"] = {
        "inventory": query_started,
        "customer_orders": query_started,
        "purchase_orders": query_started,
        "returns": query_started,
    }
    derive(state)

    print(
        "Incremental fetched: "
        f"inventory changes {len(inv_changes)}, "
        f"order-status changes {len(order_changes)}, "
        f"return changes {len(return_changes)}, "
        f"PO-status changes {len(po_changes)}"
    )
    print(
        "Exact refreshes: "
        f"active customer orders {exact_order_refreshes}, "
        f"active POs {exact_po_refreshes}"
    )
    return state


def save_atomic(state: dict):
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(STATE_FILE)


def main():
    query_started = iso_now()
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        state = incremental(state, query_started)
    else:
        state = bootstrap(query_started)

    save_atomic(state)
    d = state["derived"]
    print()
    print("=" * 72)
    print("ONGOING STATE CHECKPOINT COMPLETE — READ ONLY AGAINST ONGOING")
    print("=" * 72)
    print(f"Stored inventory records: {len(state['records']['inventory'])}")
    print(f"Stored active orders:     {len(state['records']['customer_orders'])}")
    print(f"Stored active POs:        {len(state['records']['purchase_orders'])}")
    print(f"SKUs with physical stock: {len(d['physical_by_sku'])}")
    print(f"SKUs with demand:         {len(d['customer_demand_by_sku'])}")
    print(f"SKUs with incoming:       {len(d['incoming_by_sku'])}")
    print(f"SKUs with local surplus:  {len(d['local_surplus_by_sku'])}")
    print(f"Wrote: {STATE_FILE.resolve()}")
    print("No Shopify calls were made. No Ongoing data was modified.")


if __name__ == "__main__":
    main()
