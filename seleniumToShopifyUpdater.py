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

# === PRODUCER CONFIG ===
PRODUCER = "Ekkia"

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


def get_brands_for_producer(producer_name):
    return {
        brand.upper()
        for brand, producer in BRAND_TO_PRODUCER.items()
        if producer == producer_name
    }

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Missing required environment variable: {name}")
        sys.exit(1)
    return value


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
                publishedOnCurrentPublication
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

        r = requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL,
                          json={"query": query, "variables": {"cursor": cursor}})
        r.raise_for_status()
        data = r.json()

        if not data.get("data"):
            print("GRAPHQL FAILED RESPONSE:")
            print(data)
            raise Exception("Shopify returned null data (auth/scope issue)")

        for edge in data["data"]["products"]["edges"]:
            for v in edge["node"]["variants"]["edges"]:
                sku = v["node"]["sku"]
                inv_id = v["node"]["inventoryItem"]["id"]
                if sku:
                    inventory_map[sku.strip()] = {
                        "inventoryItemId": inv_id,
                        "productId": edge["node"]["id"],
                        "title": edge["node"]["title"],
                        "vendor": edge["node"]["vendor"],
                        "published": edge["node"]["publishedOnCurrentPublication"]
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

def find_skus_to_unpublish(inventory_map, updates, producer_name):
    supplier_skus = {u["sku"] for u in updates}
    producer_brands = get_brands_for_producer(producer_name)

    to_unpublish = []

    for sku, data in inventory_map.items():
        vendor = (data.get("vendor") or "").upper()

        if vendor not in producer_brands:
            continue

        if sku not in supplier_skus:
            to_unpublish.append({
                "sku": sku,
                "productId": data["productId"]
            })

    print(f"Products to unpublish: {len({x['productId'] for x in to_unpublish})}")
    return to_unpublish


def find_skus_to_republish(inventory_map, updates, producer_name):
    supplier_skus = {u["sku"] for u in updates}
    producer_brands = get_brands_for_producer(producer_name)

    to_republish = []

    for sku in supplier_skus:
        if sku not in inventory_map:
            continue

        data = inventory_map[sku]
        vendor = (data.get("vendor") or "").upper()

        if vendor not in producer_brands:
            continue

        # 👇 ONLY republish if currently unpublished
        if data.get("published"):
            continue

        to_republish.append({
            "sku": sku,
            "productId": data["productId"]
        })

    print(f"Products to republish: {len({x['productId'] for x in to_republish})}")
    return to_republish

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

def get_online_store_publication_id():
    query = """
    {
      publications(first: 10) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    """

    r = requests.post(API_GRAPHQL, headers=HEADERS_GRAPHQL, json={"query": query})
    r.raise_for_status()
    data = r.json()

    for edge in data["data"]["publications"]["edges"]:
        if "Online Store" in edge["node"]["name"]:
            return edge["node"]["id"]

    raise Exception("Online Store publication not found")

UNPUBLISH_MUTATION = """
mutation publishableUnpublish($id: ID!, $input: [PublicationInput!]!) {
  publishableUnpublish(id: $id, input: $input) {
    userErrors { field message }
  }
}
"""

PUBLISH_MUTATION = """
mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    userErrors { field message }
  }
}
"""


def unpublish_products(items, publication_id):
    seen = set()

    for item in items:
        pid = item["productId"]
        if pid in seen:
            continue
        seen.add(pid)

        variables = {
            "id": pid,
            "input": [{"publicationId": publication_id}]
        }

        r = requests.post(
            API_GRAPHQL,
            headers=HEADERS_GRAPHQL,
            json={"query": UNPUBLISH_MUTATION, "variables": variables}
        )
        r.raise_for_status()
        time.sleep(0.3)

    print(f"Unpublished {len(seen)} products.")


def publish_products(items, publication_id):
    seen = set()

    for item in items:
        pid = item["productId"]
        if pid in seen:
            continue
        seen.add(pid)

        variables = {
            "id": pid,
            "input": [{"publicationId": publication_id}]
        }

        r = requests.post(
            API_GRAPHQL,
            headers=HEADERS_GRAPHQL,
            json={"query": PUBLISH_MUTATION, "variables": variables}
        )
        r.raise_for_status()
        time.sleep(0.3)

    print(f"Republished {len(seen)} products.")

# === MAIN ===
def main():
    downloaded_file = download_latest_file()
    print(f"Using file: {downloaded_file}")

    location_gid = get_location_id()
    publication_id = get_online_store_publication_id()

    inventory_map = fetch_inventory_items()
    updates = read_csv(downloaded_file, inventory_map)

    # 🚨 SAFETY CHECK
    if len(updates) < 2000:
        print("ABORT: too few updates, possible supplier error")
        return

    if updates:
        update_inventory(updates, location_gid)
    else:
        print("No SKUs to update.")

    # === NEW LOGIC ===
    to_unpublish = find_skus_to_unpublish(inventory_map, updates, PRODUCER)
    to_republish = find_skus_to_republish(inventory_map, updates, PRODUCER)

    # SAFE TEST MODE (no API calls)
    print("TEST MODE: no publish changes applied")

    # === ENABLE BELOW AFTER VERIFYING COUNTS ===
    # if to_unpublish:
    #     unpublish_products(to_unpublish, publication_id)
    #
    # if to_republish:
    #     publish_products(to_republish, publication_id)


if __name__ == "__main__":
    main()
