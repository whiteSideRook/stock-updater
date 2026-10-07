from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests

STATE_FILE = Path("ongoing_state.json")


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Missing required environment variable: {name}")
        sys.exit(1)
    return value


SHOPIFY_STORE = require_env("SHOPIFY_STORE")
ACCESS_TOKEN = require_env("ACCESS_TOKEN")

API_GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2025-07/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}


def graphql(query: str, variables: dict | None = None) -> dict:
    response = requests.post(
        API_GRAPHQL,
        headers=HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=(15, 90),
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(f"Shopify GraphQL errors: {payload['errors']}")
    return payload["data"]


def load_surplus_skus() -> dict[str, float]:
    if not STATE_FILE.exists():
        raise RuntimeError(f"{STATE_FILE} does not exist.")

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))

    if state.get("schema_version") != 2:
        raise RuntimeError(
            f"Unsupported ongoing_state schema: {state.get('schema_version')!r}"
        )
    if state.get("safe_for_shopify") is not True:
        raise RuntimeError("ongoing_state.json is not marked safe_for_shopify.")

    raw = state.get("derived", {}).get("local_surplus_by_sku")
    if not isinstance(raw, dict):
        raise RuntimeError("local_surplus_by_sku is missing from ongoing_state.json.")

    result = {}
    for sku, qty in raw.items():
        try:
            qty_num = float(qty)
        except (TypeError, ValueError):
            continue
        sku_norm = str(sku).strip().upper()
        if sku_norm and qty_num > 0:
            result[sku_norm] = qty_num
    return result


def fetch_archived_products() -> dict[str, dict]:
    """
    Uses the same Shopify product/status/variant fields already used by the
    existing stock scripts. Only ARCHIVED products are retained locally.
    """
    products = {}
    cursor = None

    query = """
    query($cursor: String) {
      products(first:50, after:$cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            title
            status
            variants(first:100) {
              edges {
                node { sku }
              }
            }
          }
        }
      }
    }
    """

    while True:
        data = graphql(query, {"cursor": cursor})
        connection = data["products"]

        for edge in connection["edges"]:
            product = edge["node"]
            if str(product.get("status", "")).upper() != "ARCHIVED":
                continue

            skus = {
                str(v["node"].get("sku") or "").strip().upper()
                for v in product["variants"]["edges"]
                if str(v["node"].get("sku") or "").strip()
            }
            products[product["id"]] = {
                "title": product["title"],
                "skus": skus,
            }

        page = connection["pageInfo"]
        if not page["hasNextPage"]:
            break
        cursor = page["endCursor"]
        time.sleep(0.25)

    return products


def activate_product(product_id: str):
    mutation = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product { id status }
        userErrors { field message }
      }
    }
    """
    data = graphql(
        mutation,
        {"input": {"id": product_id, "status": "ACTIVE"}},
    )
    result = data["productUpdate"]
    errors = result.get("userErrors") or []
    if errors:
        raise RuntimeError(f"Could not activate {product_id}: {errors}")

    product = result.get("product") or {}
    if str(product.get("status", "")).upper() != "ACTIVE":
        raise RuntimeError(
            f"Shopify did not confirm ACTIVE status for {product_id}: {product}"
        )


def main():
    surplus = load_surplus_skus()
    print(f"Local-surplus SKUs (>0): {len(surplus)}")

    if not surplus:
        print("No local surplus. Nothing to reactivate.")
        return

    archived = fetch_archived_products()
    print(f"Archived Shopify products inspected: {len(archived)}")

    candidates = []
    matched_surplus_skus = set()

    for product_id, product in archived.items():
        matches = sorted(product["skus"] & surplus.keys())
        if not matches:
            continue

        matched_surplus_skus.update(matches)
        candidates.append({
            "product_id": product_id,
            "title": product["title"],
            "matches": [(sku, surplus[sku]) for sku in matches],
        })

    print(f"Archived products with local surplus: {len(candidates)}")

    if not candidates:
        print("Nothing to reactivate.")
        return

    for item in candidates:
        match_text = ", ".join(
            f"{sku} (surplus {qty:g})" for sku, qty in item["matches"]
        )
        print(f"Activating: {item['title']} — {match_text}")
        activate_product(item["product_id"])
        time.sleep(0.3)

    print(f"Reactivated {len(candidates)} archived products.")
    print("Inventory quantities were NOT modified.")


if __name__ == "__main__":
    main()
