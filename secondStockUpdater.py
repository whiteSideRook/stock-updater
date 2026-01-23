import requests
import pandas as pd
import time
import chardet
from io import StringIO
from ftplib import FTP
import os
import sys


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Missing required environment variable: {name}")
        sys.exit(1)
    return value


# --- Shopify credentials ---
SHOPIFY_STORE = require_env("SHOPIFY_STORE")
ACCESS_TOKEN = require_env("ACCESS_TOKEN")

# --- FTP credentials ---
FTP_HOST = require_env("FTP_HOST")
FTP_USER = require_env("FTP_USER")
FTP_PASS = require_env("FTP_PASS")
FILENAME = require_env("FILENAME")

# --- API endpoint ---
API_GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2025-07/graphql.json"
HEADERS_GRAPHQL = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}


# --- Download CSV from FTP (FTPS) ---
def fetch_csv_from_ftp():
    ftp = FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASS)

    with open(FILENAME, "wb") as f:
        ftp.retrbinary(f"RETR {FILENAME}", f.write)

    ftp.quit()
    print(f"Downloaded latest {FILENAME} from FTP")
    return FILENAME


# --- 1. Fetch location ID ---
def get_location_id():
    query = "{ locations(first:1) { edges { node { id name } } } }"
    r = requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL, json={"query": query})
    r.raise_for_status()
    edges = r.json()["data"]["locations"]["edges"]
    if not edges:
        raise Exception("No locations found.")
    loc = edges[0]["node"]
    print(f"Using location: {loc['name']} (ID: {loc['id'].split('/')[-1]})")
    return loc["id"]


# --- 2. Fetch all products & inventoryItemIds ---
def fetch_inventory_items():
    inventory_map = {}
    cursor = None
    while True:
        query = """
        query($cursor: String) {
          products(first:50, after:$cursor) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                title
                variants(first:100) {
                  edges {
                    node { sku inventoryItem { id } }
                  }
                }
              }
            }
          }
        }
        """
        variables = {"cursor": cursor}
        r = requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL, json={"query": query, "variables": variables})
        r.raise_for_status()
        data = r.json()

        if "errors" in data:
            print("GraphQL error occurred while fetching products.")
            break

        edges = data.get("data", {}).get("products", {}).get("edges", [])
        for edge in edges:
            title = edge["node"]["title"]
            for variant_edge in edge["node"]["variants"]["edges"]:
                node = variant_edge["node"]
                sku = node["sku"]
                inv_id = node["inventoryItem"]["id"]
                if sku:
                    inventory_map[sku.strip()] = {"inventoryItemId": inv_id, "title": title}

        page_info = data.get("data", {}).get("products", {}).get("pageInfo", {})
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
        else:
            break

        time.sleep(0.5)

    print(f"Fetched {len(inventory_map)} SKUs from Shopify.")
    return inventory_map


# --- 3. Read CSV ---
def read_csv(input_file, valid_skus):
    updates = []
    skipped = 0

    with open(input_file, 'rb') as f:
        raw = f.read()
        detected = chardet.detect(raw)
        encoding = detected['encoding'] or 'utf-8'
        print(f"Detected CSV encoding: {encoding}")

    sample = raw[:2048].decode(encoding, errors='replace')
    if sample.count(';') > sample.count(','):
        sep = ';'
    elif sample.count('\t') > sample.count(','):
        sep = '\t'
    else:
        sep = ','
    print(f"Detected CSV separator: '{sep}'")

    decoded_content = raw.decode(encoding, errors='replace')
    df = pd.read_csv(StringIO(decoded_content), sep=sep, header=None, index_col=False)

    for _, row in df.iterrows():
        sku = str(row[0]).strip()

        try:
            qty = int(row[2])
        except Exception:
            qty = None

        if qty is not None and (qty < 0 or qty > 1_000_000):
            qty = None

        if sku in valid_skus and qty is not None:
            updates.append({
                "sku": sku,
                "quantity": qty,
                "title": valid_skus[sku]["title"],
                "inventoryItemId": valid_skus[sku]["inventoryItemId"]
            })
        else:
            skipped += 1

    print(f"Prepared {len(updates)} inventory updates from CSV. Skipped {skipped} invalid/missing SKUs.")
    return updates


# --- 4. Inventory mutation ---
MUTATION = """
mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
  inventorySetQuantities(input: $input) {
    inventoryAdjustmentGroup {
      createdAt
      changes { name delta }
    }
    userErrors { field message }
  }
}
"""


# --- 5. Throttle ---
def throttle_sleep(resp):
    limit = resp.headers.get("X-Shopify-Shop-Api-Call-Limit", "0/0")
    try:
        used, max_calls = map(int, limit.split("/"))
        if max_calls == 0 or used / max_calls <= 0.8:
            time.sleep(0.5)
        else:
            time.sleep(2)
    except Exception:
        time.sleep(0.5)


# --- 6. Batch update ---
def update_inventory(updates, location_gid, batch_size=250):
    total_updated = 0
    total_errors = 0

    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        variables = {
            "input": {
                "name": "available",
                "reason": "correction",
                "ignoreCompareQuantity": True,
                "referenceDocumentUri": "logistics://script/update",
                "quantities": [
                    {"inventoryItemId": item["inventoryItemId"], "locationId": location_gid, "quantity": item["quantity"]}
                    for item in batch
                ]
            }
        }

        r = requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL, json={"query": MUTATION, "variables": variables})
        r.raise_for_status()
        resp_json = r.json()

        user_errors = resp_json.get("data", {}).get("inventorySetQuantities", {}).get("userErrors", [])
        if user_errors:
            print("Inventory update returned user errors.")
            total_errors += len(user_errors)
        else:
            for _ in batch:
                total_updated += 1

        throttle_sleep(r)

    print(f"\nInventory update complete. {total_updated} updated, {total_errors} errors.")


# --- Main ---
def main():
    input_file = fetch_csv_from_ftp()
    location_gid = get_location_id()
    inventory_map = fetch_inventory_items()
    updates = read_csv(input_file, inventory_map)

    if updates:
        update_inventory(updates, location_gid)
    else:
        print("No valid SKUs to update.")


if __name__ == "__main__":
    main()
