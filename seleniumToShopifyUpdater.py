import os
import time
import sys
import requests
import pandas as pd
import chardet
from io import StringIO

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Missing required environment variable: {name}")
        sys.exit(1)
    return value

BRAND_TO_PRODUCER = {
    "ABSORBINE": "Ekkia",
    "BORSTIQ": "Ekkia",
    "CARR & DAY MARTIN": "Ekkia",
    "CHOPLIN": "Ekkia",
    "EDEN BY PENELOPE": "Ekkia",
    "EFFAX": "Ekkia",
    "EQUI-KIDS": "Ekkia",
    "EQUITHEME": "Ekkia",
    "ERIC THOMAS": "Ekkia",
    "FEELING": "Ekkia",
    "HEINIGER": "Ekkia",
    "LEOVET": "Ekkia",
    "NACA": "Ekkia",
    "NAF": "Ekkia",
    "NORTON": "Ekkia",
    "PADDOCK": "Ekkia",
    "PENELOPE": "Ekkia",
    "PENELOPE COLLECTIONS": "Ekkia",
    "RIDING WORLD": "Ekkia",
    "FLECK": "Ekkia",
    "LISTER": "Ekkia",
    "PADDOCK SPORTS": "Ekkia"
}


# === ENV ===
APP_URL = require_env("APP_URL")
APP_PASSWORD = require_env("APP_PASSWORD")
SHOPIFY_STORE = require_env("SHOPIFY_STORE")
ACCESS_TOKEN = require_env("ACCESS_TOKEN")

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# === SHOPIFY ===
API_GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2025-07/graphql.json"
HEADERS_GRAPHQL = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}


# === SELENIUM SETUP ===
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )


# === DOWNLOAD ===
def download_latest_file():
    from selenium.common.exceptions import StaleElementReferenceException

    driver = setup_driver()

    try:
        driver.get(APP_URL)

        wait = WebDriverWait(
            driver,
            20,
            ignored_exceptions=(StaleElementReferenceException,)
        )

        # --- LOGIN ---
        password_input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
        )
        password_input.send_keys(APP_PASSWORD)

        submit_btn = driver.find_element(By.CSS_SELECTOR, "button, input[type='submit']")
        submit_btn.click()

        # --- WAIT FOR TABLE ---
        wait.until(EC.presence_of_element_located((By.ID, "files-datatable_data")))

        # wait for ajax overlay to disappear
        wait.until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, ".ajax-status-block-ui"))
        )

        # --- SORT BUTTON ---
        mod_header = wait.until(
            EC.element_to_be_clickable((By.ID, "files-datatable:j_idt156"))
        )

        # --- FIRST SORT CLICK ---
        first_name_before = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#files-datatable_data tr .filename-column a")
            )
        ).text

        driver.execute_script("arguments[0].click();", mod_header)

        wait.until(lambda d: (
            d.find_element(
                By.CSS_SELECTOR,
                "#files-datatable_data tr .filename-column a"
            ).text != first_name_before
        ))

        # --- SECOND SORT CLICK ---
        first_name_before = driver.find_element(
            By.CSS_SELECTOR,
            "#files-datatable_data tr .filename-column a"
        ).text

        driver.execute_script("arguments[0].click();", mod_header)

        wait.until(lambda d: (
            d.find_element(
                By.CSS_SELECTOR,
                "#files-datatable_data tr .filename-column a"
            ).text != first_name_before
        ))

        # ------------------------------------------------------------
        # FIXED PART: scan top 10 rows instead of trusting row 1
        # ------------------------------------------------------------
        rows = driver.find_elements(By.CSS_SELECTOR, "#files-datatable_data tr")

        filename = None
        target_row = None

        for row in rows[:10]:
            try:
                link = row.find_element(By.CSS_SELECTOR, ".filename-column a")

                name = (
                    link.get_attribute("aria-label")
                    or link.get_attribute("title")
                    or link.text
                    or ""
                ).strip()

                if name.startswith("PDT_DISPO_"):
                    filename = name
                    target_row = row
                    break

            except Exception:
                continue

        if not filename or not target_row:
            raise Exception("No PDT_DISPO file found in top 10 rows")

        print(f"Selected file: {filename}")

        # --- DOWNLOAD ---
        download_button = target_row.find_element(
            By.CSS_SELECTOR,
            "button[title*='Télé']"
        )

        driver.execute_script("arguments[0].click();", download_button)

        print(f"Downloading: {filename}")

        target_path = os.path.join(DOWNLOAD_DIR, filename)

        for _ in range(60):
            if os.path.exists(target_path):
                return target_path
            time.sleep(1)

        raise Exception("Download timeout")

    finally:
        driver.quit()


# === SHOPIFY HELPERS ===
def get_location_id():
    query = "{ locations(first:1) { edges { node { id name } } } }"
    r = requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL, json={"query": query})

    r.raise_for_status()
    resp_json = r.json()

    if "data" not in resp_json:
        print("Shopify response missing 'data':", resp_json)
        raise Exception("Failed to fetch Shopify locations. Check token and query.")

    edges = resp_json["data"]["locations"]["edges"]
    if not edges:
        raise Exception("No Shopify locations found")

    return edges[0]["node"]["id"]



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
                    node { sku inventoryItem { id } }
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

            for v in product["variants"]["edges"]:
                sku = v["node"]["sku"]
                inv_id = v["node"]["inventoryItem"]["id"]

                if sku:
                    inventory_map[sku.strip().upper()] = {
                        "inventoryItemId": inv_id,
                        "title": product["title"],
                        "vendor": product["vendor"],
                        "product_id": product["id"],
                        "status": product["status"]
                    }

        page = data["data"]["products"]["pageInfo"]
        if page["hasNextPage"]:
            cursor = page["endCursor"]
        else:
            break

        time.sleep(0.4)

    print(f"Fetched {len(inventory_map)} SKUs from Shopify.")
    return inventory_map


# === CSV ===
def read_csv(input_file, valid_skus):
    updates = []
    skipped = 0

    with open(input_file, "rb") as f:
        raw = f.read()

    encoding = chardet.detect(raw)["encoding"] or "utf-8"
    sample = raw[:2048].decode(encoding, errors="replace")

    if sample.count(";") > sample.count(","):
        sep = ";"
    elif sample.count("\t") > sample.count(","):
        sep = "\t"
    else:
        sep = ","

    df = pd.read_csv(StringIO(raw.decode(encoding, errors="replace")),
                     sep=sep, header=None)

    for _, row in df.iterrows():
        sku = str(row[0]).strip()

        try:
            qty = int(row[1])
        except Exception:
            qty = None

        if sku in valid_skus and qty is not None:
            updates.append({
                "sku": sku,
                "quantity": qty,
                "inventoryItemId": valid_skus[sku]["inventoryItemId"]
            })
        else:
            skipped += 1

    print(f"Prepared {len(updates)} updates, skipped {skipped}.")
    return updates


# === UPDATE ===
MUTATION = """
mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
  inventorySetQuantities(input: $input) {
    userErrors { field message }
  }
}
"""


def update_inventory(updates, location_gid, batch_size=250):
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]

        variables = {
            "input": {
                "name": "available",
                "reason": "correction",
                "ignoreCompareQuantity": True,
                "quantities": [
                    {
                        "inventoryItemId": item["inventoryItemId"],
                        "locationId": location_gid,
                        "quantity": item["quantity"]
                    }
                    for item in batch
                ]
            }
        }

        r = requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL,
                          json={"query": MUTATION, "variables": variables})
        r.raise_for_status()

        time.sleep(0.5)

    print(f"Inventory update complete for {len(updates)} items.")


def is_ekkia_product(vendor: str) -> bool:
    if not vendor:
        return False
    return vendor.strip().upper() in BRAND_TO_PRODUCER

def find_missing_ekkia_skus(inventory_map, csv_skus):
    missing = []

    for sku, data in inventory_map.items():
        vendor = data.get("vendor", "")

        # Only consider Ekkia brands
        if not is_ekkia_product(vendor):
            continue

        if sku not in csv_skus:
            missing.append({
                "sku": sku,
                "inventoryItemId": data["inventoryItemId"],
                "title": data["title"],
                "vendor": vendor
            })

    print(f"Identified {len(missing)} missing Ekkia SKUs.")
    return missing

def extract_csv_skus(input_file):
    skus = set()

    with open(input_file, "rb") as f:
        raw = f.read()

    encoding = chardet.detect(raw)["encoding"] or "utf-8"
    sample = raw[:2048].decode(encoding, errors="replace")

    if sample.count(";") > sample.count(","):
        sep = ";"
    elif sample.count("\t") > sample.count(","):
        sep = "\t"
    else:
        sep = ","

    df = pd.read_csv(StringIO(raw.decode(encoding, errors="replace")),
                     sep=sep, header=None)

    for _, row in df.iterrows():
        sku = str(row[0]).strip()
        if sku:
            skus.add(sku)

    print(f"Extracted {len(skus)} SKUs from CSV.")
    return skus



def dry_run_removals(missing_skus):
    count = len(missing_skus)
    print(f"[DRY RUN] {count} SKUs would be removed.")

    for item in missing_skus[:15]:
        print(f" - {item['sku']} | {item['vendor']} | {item['title']}")

    return count


def remove_missing_skus(missing_skus, location_gid):
    if len(missing_skus) > 1300:
        print(f"ABORTED: {len(missing_skus)} SKUs exceed safety limit (300).")
        return

    print(f"Removing {len(missing_skus)} SKUs (setting inventory to 0)...")

    removal_updates = [
        {
            "sku": item["sku"],
            "quantity": 0,
            "inventoryItemId": item["inventoryItemId"]
        }
        for item in missing_skus
    ]

    update_inventory(removal_updates, location_gid)

from collections import defaultdict

def build_product_groups(inventory_map):
    products = {}

    for sku, data in inventory_map.items():
        vendor = data.get("vendor", "")
        product_id = data.get("product_id")

        if not is_ekkia_product(vendor):
            continue

        if not product_id:
            continue

        # skip archived products (Shopify truth)
        if str(data.get("status", "")).upper() == "ARCHIVED":
            continue

        if product_id not in products:
            products[product_id] = {
                "title": data["title"],
                "vendor": vendor,
                "skus": []
            }

        products[product_id]["skus"].append(sku)

    return products

def evaluate_products(products, csv_skus, min_variants_threshold=5):
    to_archive = []

    for product_id, data in products.items():
        skus = data["skus"]

        total_variants = len(skus)
        active_variants = sum(1 for s in skus if s in csv_skus)

        # RULE A: all variants missing
        if active_variants == 0:
            to_archive.append({
                "product_id": product_id,
                "product": data["title"],
                "reason": "all_variants_missing",
                "total": total_variants,
                "active": active_variants
            })
            continue

        # RULE B: only 1 variant left but product is large enough
        if (
            active_variants == 1
            and total_variants >= min_variants_threshold
        ):
            to_archive.append({
                "product_id": product_id,
                "product": data["title"],
                "reason": "single_variant_remaining",
                "total": total_variants,
                "active": active_variants
            })

    return to_archive

def archive_products(to_archive, dry_run=True):
    """
    Archives Shopify products by setting status to ARCHIVED.
    """

    if not to_archive:
        print("No products to archive.")
        return

    MUTATION = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          status
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    archived_count = 0

    for item in to_archive:
        product_id = item["product_id"]

        # Shopify expects gid format already (you already store full id ✔)
        variables = {
            "input": {
                "id": product_id,
                "status": "ARCHIVED"
            }
        }

        if dry_run:
            continue

        r = requests.post(
            API_GRAPHQL,
            headers=HEADERS_GRAPHQL,
            json={"query": MUTATION, "variables": variables}
        )

        r.raise_for_status()
        resp = r.json()

        errors = resp.get("data", {}).get("productUpdate", {}).get("userErrors", [])
        if errors:
            print(f"Failed to archive {product_id}: {errors}")
            continue

        archived_count += 1
        time.sleep(0.3)

    print(f"Archived {archived_count} products.")


# === MAIN ===
def main():
    downloaded_file = download_latest_file()
    print(f"Using file: {downloaded_file}")

    location_gid = get_location_id()
    inventory_map = fetch_inventory_items()

    # === SKU UPDATE FLOW (UNCHANGED) ===
    updates = read_csv(downloaded_file, inventory_map)

    if updates:
        update_inventory(updates, location_gid)
    else:
        print("No SKUs to update.")

    # =========================================================
    # === NEW LOGIC: SKU CLEANUP + PRODUCT ARCHIVING ==========
    # =========================================================

    csv_skus = extract_csv_skus(downloaded_file)

    # -----------------------------
    # SKU-LEVEL CLEANUP (EKKIA)
    # -----------------------------
    ekkia_missing_skus = [
        {
            "sku": sku,
            "inventoryItemId": data["inventoryItemId"]
        }
        for sku, data in inventory_map.items()
        if is_ekkia_product(data.get("vendor", ""))
        and sku not in csv_skus
    ]

    print("\n=== SKU INVENTORY CLEANUP (EKKIA) ===")
    print(f"Ekkia SKUs to be set to 0: {len(ekkia_missing_skus)}")
    print("====================================\n")

    remove_missing_skus(ekkia_missing_skus, location_gid)
    
    # -----------------------------
    # PRODUCT GROUPING
    # -----------------------------
    products = build_product_groups(inventory_map)

    to_archive = evaluate_products(
        products,
        csv_skus,
        min_variants_threshold=5
    )

    # -----------------------------
    # PRODUCT ARCHIVE DRY RUN
    # -----------------------------
    total_products = len(to_archive)

    total_missing_variants = sum(
        p["total"] - p["active"] for p in to_archive
    )

    all_missing_products = sum(
        1 for p in to_archive if p["reason"] == "all_variants_missing"
    )

    single_variant_products = sum(
        1 for p in to_archive if p["reason"] == "single_variant_remaining"
    )

    print("\n=== PRODUCT ARCHIVE DRY RUN ===")
    print(f"Products flagged for archive: {total_products}")
    print(f"Products with ALL variants missing: {all_missing_products}")
    print(f"Products with SINGLE variant remaining: {single_variant_products}")
    print(f"Total missing variants (across flagged products): {total_missing_variants}")
    print("================================\n")

    # -----------------------------
    # OPTIONAL EXECUTION SWITCH
    # -----------------------------
    RUN_ARCHIVE = False  # change to True when ready

    archive_products(to_archive, dry_run=not RUN_ARCHIVE)


if __name__ == "__main__":
    main()
