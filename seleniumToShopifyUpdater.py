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


# =========================
# CONFIG
# =========================

DRY_RUN = True

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


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Missing required environment variable: {name}")
        sys.exit(1)
    return value


APP_URL = require_env("APP_URL")
APP_PASSWORD = require_env("APP_PASSWORD")
SHOPIFY_STORE = require_env("SHOPIFY_STORE")
ACCESS_TOKEN = require_env("ACCESS_TOKEN")

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

API_GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2025-07/graphql.json"
HEADERS_GRAPHQL = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}


# =========================
# SELENIUM
# =========================

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


def download_latest_file():
    driver = setup_driver()

    try:
        driver.get(APP_URL)
        wait = WebDriverWait(driver, 20)

        password_input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
        )
        password_input.send_keys(APP_PASSWORD)

        driver.find_element(By.CSS_SELECTOR, "button, input[type='submit']").click()

        wait.until(EC.presence_of_element_located((By.ID, "files-datatable_data")))
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".ajax-status-block-ui")))

        rows = driver.find_elements(By.CSS_SELECTOR, "#files-datatable_data tr")

        filename = None
        target_row = None

        for row in rows[:10]:
            try:
                link = row.find_element(By.CSS_SELECTOR, ".filename-column a")
                name = (link.text or "").strip()

                if name.startswith("PDT_DISPO_"):
                    filename = name
                    target_row = row
                    break
            except:
                continue

        if not filename:
            raise Exception("No PDT_DISPO file found")

        print(f"Selected file: {filename}")

        download_button = target_row.find_element(By.CSS_SELECTOR, "button[title*='Télé']")
        driver.execute_script("arguments[0].click();", download_button)

        target_path = os.path.join(DOWNLOAD_DIR, filename)

        for _ in range(60):
            if os.path.exists(target_path):
                return target_path
            time.sleep(1)

        raise Exception("Download timeout")

    finally:
        driver.quit()


# =========================
# SHOPIFY
# =========================

def get_location_id():
    query = "{ locations(first:1) { edges { node { id } } } }"
    r = requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL, json={"query": query})
    r.raise_for_status()
    return r.json()["data"]["locations"]["edges"][0]["node"]["id"]


def fetch_inventory_items():
    inventory_map = {}
    product_sku_map = {}
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

        r = requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL,
                          json={"query": query, "variables": {"cursor": cursor}})
        r.raise_for_status()
        data = r.json()

        for edge in data["data"]["products"]["edges"]:
            p = edge["node"]

            if p.get("status") == "ARCHIVED":
                continue

            vendor = (p.get("vendor") or "").upper().strip()

            for v in p["variants"]["edges"]:
                sku = v["node"]["sku"]
                if not sku:
                    continue

                sku = sku.strip()

                inventory_map[sku] = {
                    "inventoryItemId": v["node"]["inventoryItem"]["id"],
                    "productId": p["id"],
                    "vendor": vendor,
                    "title": p["title"]
                }

                product_sku_map.setdefault(p["id"], set()).add(sku)

        page = data["data"]["products"]["pageInfo"]
        if page["hasNextPage"]:
            cursor = page["endCursor"]
        else:
            break

        time.sleep(0.3)

    return inventory_map, product_sku_map


# =========================
# CSV
# =========================

def read_csv(input_file, valid_skus):
    updates = []
    csv_skus = set()

    with open(input_file, "rb") as f:
        raw = f.read()

    encoding = chardet.detect(raw)["encoding"] or "utf-8"
    text = raw.decode(encoding, errors="replace")

    sep = ";" if text.count(";") > text.count(",") else ","

    df = pd.read_csv(StringIO(text), sep=sep, header=None)

    for _, row in df.iterrows():
        sku = str(row[0]).strip()
        csv_skus.add(sku)

        try:
            qty = int(row[1])
        except:
            continue

        if sku in valid_skus:
            updates.append({
                "sku": sku,
                "quantity": qty,
                "inventoryItemId": valid_skus[sku]["inventoryItemId"]
            })

    return updates, csv_skus


# =========================
# LOGIC
# =========================

def compute_missing_and_archives(inventory_map, product_sku_map, csv_skus):
    missing_updates = []
    product_missing = {}

    for sku, data in inventory_map.items():
        if data["vendor"] not in BRAND_TO_PRODUCER:
            continue

        if sku not in csv_skus:
            missing_updates.append({
                "sku": sku,
                "quantity": 0,
                "inventoryItemId": data["inventoryItemId"],
                "productId": data["productId"]
            })

            product_missing.setdefault(data["productId"], set()).add(sku)

    archive_products = []

    for product_id, skus in product_sku_map.items():

        ekkia_skus = {
            s for s in skus
            if s in inventory_map
        }

        missing = product_missing.get(product_id, set())

        if ekkia_skus and ekkia_skus.issubset(missing):
            archive_products.append(product_id)

    return missing_updates, archive_products


# =========================
# SHOPIFY ACTIONS
# =========================

def archive_products(product_ids):
    mutation = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product { id status }
      }
    }
    """

    for pid in product_ids:
        requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL, json={
            "query": mutation,
            "variables": {"input": {"id": pid, "status": "ARCHIVED"}}
        })
        time.sleep(0.3)


def update_inventory(updates, location_gid):
    mutation = """
    mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
      inventorySetQuantities(input: $input) {
        userErrors { field message }
      }
    }
    """

    for i in range(0, len(updates), 250):
        batch = updates[i:i+250]

        variables = {
            "input": {
                "name": "available",
                "reason": "correction",
                "ignoreCompareQuantity": True,
                "quantities": [
                    {
                        "inventoryItemId": u["inventoryItemId"],
                        "locationId": location_gid,
                        "quantity": u["quantity"]
                    }
                    for u in batch
                ]
            }
        }

        requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL,
                      json={"query": mutation, "variables": variables})

        time.sleep(0.4)


# =========================
# MAIN
# =========================

def main():
    file = download_latest_file()
    print("File:", file)

    location_gid = get_location_id()

    inventory_map, product_sku_map = fetch_inventory_items()
    updates, csv_skus = read_csv(file, inventory_map)

    missing, to_archive = compute_missing_and_archives(
        inventory_map,
        product_sku_map,
        csv_skus
    )

    if DRY_RUN:
        print("\n=== DRY RUN ===")
        print("CSV updates:", len(updates))
        print("Missing SKUs (set to 0):", len(missing))
        print("Products to archive:", len(to_archive))
        return

    update_inventory(updates + missing, location_gid)

    if to_archive:
        archive_products(to_archive)


if __name__ == "__main__":
    main()
