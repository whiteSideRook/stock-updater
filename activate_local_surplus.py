from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests

STATE_FILE = Path(os.getenv("ONGOING_STATE_FILE", "ongoing_state.json"))


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
        raise RuntimeError("Shopify GraphQL returned an error.")
    return payload["data"]


def load_surplus_skus() -> set[str]:
    if not STATE_FILE.exists():
        raise RuntimeError("Ongoing state file is missing.")

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))

    if state.get("schema_version") != 2:
        raise RuntimeError("Unsupported Ongoing state schema.")
    if state.get("safe_for_shopify") is not True:
        raise RuntimeError("Ongoing state is not marked safe_for_shopify.")

    raw = state.get("derived", {}).get("local_surplus_by_sku")
    if not isinstance(raw, dict):
        raise RuntimeError("local_surplus_by_sku is missing from Ongoing state.")

    return {
        str(sku).strip().upper()
        for sku, qty in raw.items()
        if str(sku).strip() and _positive(qty)
    }


def _positive(value) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def fetch_archived_products() -> dict[str, set[str]]:
    products = {}
    cursor = None

    query = """
    query($cursor: String) {
      products(first:50, after:$cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            status
            variants(first:100) {
              edges { node { sku } }
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
            products[product["id"]] = skus

        page = connection["pageInfo"]
        if not page["hasNextPage"]:
            break
        cursor = page["endCursor"]
        time.sleep(0.25)

    return products


def activate_product(product_id: str) -> None:
    mutation = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product { id status }
        userErrors { field message }
      }
    }
    """
    data = graphql(mutation, {"input": {"id": product_id, "status": "ACTIVE"}})
    result = data["productUpdate"]
    if result.get("userErrors"):
        raise RuntimeError("Shopify rejected a product activation.")
    if str((result.get("product") or {}).get("status", "")).upper() != "ACTIVE":
        raise RuntimeError("Shopify did not confirm ACTIVE status.")


def main() -> None:
    surplus_skus = load_surplus_skus()
    print(f"Local-surplus SKUs available for protection: {len(surplus_skus)}")

    if not surplus_skus:
        print("No local surplus. Nothing to reactivate.")
        return

    archived = fetch_archived_products()
    candidates = [
        product_id
        for product_id, product_skus in archived.items()
        if product_skus & surplus_skus
    ]

    print(f"Archived products inspected: {len(archived)}")
    print(f"Archived products eligible for reactivation: {len(candidates)}")

    for product_id in candidates:
        activate_product(product_id)
        time.sleep(0.3)

    print(f"Reactivated {len(candidates)} archived products.")
    print("No inventory quantities were modified.")


if __name__ == "__main__":
    main()
