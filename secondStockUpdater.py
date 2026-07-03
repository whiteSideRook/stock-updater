import requests
import pandas as pd
import time
import chardet
from io import StringIO
from ftplib import FTP
import os
import sys

BRAND_TO_PRODUCER = {
    "CATAGO": "Eldorado",
    "ELDORADO": "Eldorado",
    "EQUIPAGE": "Eldorado",
    "HORSEGUARD": "Eldorado"
}


def is_eldorado_product(vendor: str) -> bool:
    if not vendor:
        return False
    return vendor.strip().upper() in BRAND_TO_PRODUCER


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
                id
                title
                vendor
                status
                variants(first:100) {
                  edges {
                    node {
                      sku
                      inventoryItem { id }
                    }
                  }
                }
              }
            }
          }
        }
        """

        r = requests.post(
            API_GRAPHQL,
            headers=HEADERS_GRAPHQL,
            json={"query": query, "variables": {"cursor": cursor}}
        )

        r.raise_for_status()
        data = r.json()

        for edge in data["data"]["products"]["edges"]:
            product = edge["node"]
            title = product["title"]
            vendor = product.get("vendor", "")
            status = product.get("status", "")
            product_id = product.get("id")

            for v in product["variants"]["edges"]:
                node = v["node"]
                sku = node["sku"]
                inv_id = node["inventoryItem"]["id"]

                if sku:
                    inventory_map[sku.strip().upper()] = {
                        "inventoryItemId": inv_id,
                        "title": title,
                        "vendor": vendor,
                        "product_id": product_id,
                        "status": status
                    }

        page = data["data"]["products"]["pageInfo"]
        if page["hasNextPage"]:
            cursor = page["endCursor"]
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

def find_missing_eldorado_skus(inventory_map, csv_stock):
    missing = []

    for sku, data in inventory_map.items():
        if not is_eldorado_product(data.get("vendor", "")):
            continue

        if sku not in csv_stock or csv_stock.get(sku, 0) <= 0:
            missing.append({
                "sku": sku,
                "inventoryItemId": data["inventoryItemId"],
                "title": data["title"],
                "vendor": data["vendor"]
            })

    print(f"Eldorado missing SKUs: {len(missing)}")
    return missing

def build_product_groups(inventory_map):
    products = {}

    for sku, data in inventory_map.items():
        if not is_eldorado_product(data.get("vendor", "")):
            continue

        product_id = data.get("product_id")
        if not product_id:
            continue

        if data.get("status", "").upper() == "ARCHIVED":
            continue

        if product_id not in products:
            products[product_id] = {
                "title": data["title"],
                "skus": []
            }

        products[product_id]["skus"].append(sku)

    return products

def evaluate_products(products, csv_stock, min_variants_threshold=5):
    to_archive = []

    for product_id, data in products.items():
        skus = data["skus"]

        total_variants = len(skus)
        active_variants = sum(
            1 for s in skus
            if s in csv_stock and csv_stock[s] > 0
        )

        # CASE 1: all missing
        if active_variants == 0:
            to_archive.append({
                "product_id": product_id,
                "title": data["title"],
                "reason": "all_missing",
                "total": total_variants,
                "active": active_variants
            })
            continue

        # CASE 2: large product, only 1 variant left
        if total_variants >= min_variants_threshold and active_variants == 1:
            to_archive.append({
                "product_id": product_id,
                "title": data["title"],
                "reason": "single_variant_left",
                "total": total_variants,
                "active": active_variants
            })

    return to_archive

def remove_missing_skus(missing_skus, location_gid):
    updates = [
        {
            "sku": item["sku"],
            "quantity": 0,
            "inventoryItemId": item["inventoryItemId"]
        }
        for item in missing_skus
    ]

    update_inventory(updates, location_gid)


def archive_products(to_archive):
    MUTATION = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product { id status }
        userErrors { field message }
      }
    }
    """

    archived = 0

    for p in to_archive:
        variables = {
            "input": {
                "id": p["product_id"],
                "status": "ARCHIVED"
            }
        }

        r = requests.post(
            API_GRAPHQL,
            headers=HEADERS_GRAPHQL,
            json={"query": MUTATION, "variables": variables}
        )

        r.raise_for_status()
        archived += 1
        time.sleep(0.3)

    print(f"Archived products: {archived}")


def build_archived_product_groups(inventory_map):
    products = {}

    for sku, data in inventory_map.items():
        if not is_eldorado_product(data.get("vendor", "")):
            continue

        product_id = data.get("product_id")
        if not product_id:
            continue

        # ONLY archived products
        if data.get("status", "").upper() != "ARCHIVED":
            continue

        if product_id not in products:
            products[product_id] = {
                "title": data["title"],
                "skus": []
            }

        products[product_id]["skus"].append(sku)

    return products


def evaluate_archived_products_for_reactivation(products, csv_stock):
    to_unarchive = []

    for product_id, data in products.items():
        skus = data["skus"]
        total_variants = len(skus)

        active_variants = sum(
            1 for s in skus
            if s in csv_stock and csv_stock[s] > 0
        )

        # SMALL PRODUCT
        if total_variants < 5:
            if active_variants >= 1:
                to_unarchive.append({
                    "product_id": product_id,
                    "title": data["title"],
                    "total": total_variants,
                    "active": active_variants,
                    "reason": "small_product_recovery"
                })

        # LARGE PRODUCT
        else:
            if active_variants >= 2:
                to_unarchive.append({
                    "product_id": product_id,
                    "title": data["title"],
                    "total": total_variants,
                    "active": active_variants,
                    "reason": "large_product_recovery"
                })

    return to_unarchive

def unarchive_products(to_unarchive):
    MUTATION = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product { id status }
        userErrors { field message }
      }
    }
    """

    restored = 0

    for p in to_unarchive:
        print(f"Restoring: {p['title']} ({p['active']}/{p['total']})")

        variables = {
            "input": {
                "id": p["product_id"],
                "status": "ACTIVE"
            }
        }

        r = requests.post(
            API_GRAPHQL,
            headers=HEADERS_GRAPHQL,
            json={"query": MUTATION, "variables": variables}
        )

        r.raise_for_status()
        restored += 1
        time.sleep(0.3)

    print(f"Reactivated products: {restored}")



# --- Main ---
def main():
    input_file = fetch_csv_from_ftp()
    location_gid = get_location_id()

    inventory_map = fetch_inventory_items()

    # -------------------------
    # CSV updates (UNCHANGED)
    # -------------------------
    updates = read_csv(input_file, inventory_map)

    if updates:
        update_inventory(updates, location_gid)
    else:
        print("No valid SKUs to update.")

    # -------------------------
    # CSV SKUs
    # -------------------------
    df = pd.read_csv(input_file, header=None)

    csv_stock = {}

    for _, row in df.iterrows():
        sku = str(row[0]).strip().upper()

        try:
            qty = int(row[2])
        except Exception:
            continue

        if qty is None or qty < 0:
            continue

        csv_stock[sku] = qty

    # =========================================================
    # SKU CLEANUP DRY RUN (ELDORADO)
    # =========================================================
    missing_skus = find_missing_eldorado_skus(inventory_map, csv_stock)

    total_missing_skus = len(missing_skus)

    print("\n=== SKU CLEANUP DRY RUN (ELDORADO) ===")
    print(f"Eldorado SKUs to be set to 0: {total_missing_skus}")
    print("======================================\n")

    RUN_ZERO_STOCK = True

    if RUN_ZERO_STOCK:
        remove_missing_skus(missing_skus, location_gid)

    # =========================================================
    # PRODUCT GROUPING + ARCHIVE DRY RUN
    # =========================================================
    products = build_product_groups(inventory_map)

    to_archive = evaluate_products(products, csv_stock)

    total_products = len(to_archive)

    all_missing = sum(1 for p in to_archive if p["reason"] == "all_missing")
    single_left = sum(1 for p in to_archive if p["reason"] == "single_variant_left")

    print("\n=== PRODUCT ARCHIVE DRY RUN (ELDORADO) ===")
    print(f"Products flagged for archive: {total_products}")
    print(f"All variants missing: {all_missing}")
    print(f"Single variant remaining: {single_left}")
    print("==========================================\n")

    SAFETY_ARCHIVE_LIMIT = 500  # <-- adjust this number
    
    RUN_ARCHIVE = True


    if total_products > SAFETY_ARCHIVE_LIMIT:
        print("\n🚨 SAFETY STOP — ARCHIVING SKIPPED")
        print(f"Attempted to archive: {total_products} products")
        print(f"Limit: {SAFETY_ARCHIVE_LIMIT}")
        print("No products were archived.\n")
    else:
        if RUN_ARCHIVE:
            archive_products(to_archive)

    # =========================
    # REACTIVATION FLOW
    # =========================

    archived_products = build_archived_product_groups(inventory_map)

    to_unarchive = evaluate_archived_products_for_reactivation(
        archived_products,
        csv_stock
    )

    print("\n=== PRODUCT REACTIVATION DRY RUN (ELDORADO) ===")
    print(f"Products eligible for reactivation: {len(to_unarchive)}")
    print("==============================================\n")

    RUN_UNARCHIVE = True
    if RUN_UNARCHIVE and len(to_unarchive) <= 200:
        unarchive_products(to_unarchive)

if __name__ == "__main__":
    main()
