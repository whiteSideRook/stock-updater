"""Shared Shopify read client and write gateway. No secrets are logged."""
import os, time, requests

class Shopify:
    def __init__(self, dry_run=True):
        self.dry_run = dry_run
        self.url = f"https://{os.environ['SHOPIFY_STORE']}/admin/api/2025-07/graphql.json"
        self.headers = {'X-Shopify-Access-Token':os.environ['ACCESS_TOKEN'], 'Content-Type':'application/json'}
        self.calls = 0
        self.writes = []
        self.products = {}
        self.inventory = {}

    def graphql(self, query, variables=None, write=False):
        if write and self.dry_run:
            raise RuntimeError('Blocked live Shopify mutation in dry-run mode')
        for attempt in range(5):
            self.calls += 1
            response = requests.post(self.url, headers=self.headers, json={'query':query,'variables':variables or {}}, timeout=(15,90))
            if response.status_code in (429,500,502,503,504) and attempt < 4:
                time.sleep(min(2**attempt,12)); continue
            response.raise_for_status()
            data = response.json()
            if data.get('errors'):
                raise RuntimeError(f"Shopify GraphQL errors: {data['errors']}")
            if 'data' not in data:
                raise RuntimeError('Shopify GraphQL response missing data')
            return data['data']
        raise RuntimeError('Shopify retry limit reached')

    def load(self):
        locations = self.graphql('{ locations(first:1) { edges { node { id name } } } }')['locations']['edges']
        if not locations: raise RuntimeError('No Shopify location found')
        self.location = locations[0]['node']['id']
        cursor = None
        while True:
            result = self.graphql("""query($cursor:String){products(first:50,after:$cursor){pageInfo{hasNextPage endCursor} edges{node{id title vendor status variants(first:100){pageInfo{hasNextPage} edges{node{sku inventoryItem{id}}}}}}}}""",{'cursor':cursor})['products']
            for edge in result['edges']:
                p=edge['node']; pid=p['id']
                # Never silently ignore variants beyond Shopify's first page.
                if p['variants']['pageInfo']['hasNextPage']:
                    raise RuntimeError(f'Product {pid} has >100 variants; aborting to avoid incomplete stock/archiving decisions')
                skus=set()
                for ve in p['variants']['edges']:
                    v=ve['node']; sku=str(v.get('sku') or '').strip().upper()
                    if not sku: continue
                    if not v.get('inventoryItem'): raise RuntimeError(f'Missing inventory item for {sku}')
                    if sku in self.inventory: raise RuntimeError(f'Duplicate Shopify SKU: {sku}')
                    skus.add(sku)
                    self.inventory[sku]={'inventoryItemId':v['inventoryItem']['id'],'title':p['title'],'vendor':p.get('vendor',''),'product_id':pid,'status':p['status']}
                self.products[pid]={'status':p['status'],'skus':skus}
            page=result['pageInfo']
            if not page['hasNextPage']: break
            cursor=page['endCursor']
            if not cursor: raise RuntimeError('Missing pagination cursor')
        print(f'Shared Shopify snapshot: {len(self.products)} products, {len(self.inventory)} SKUs, {self.calls} requests')
        return self.inventory

    def update(self, updates, location, source):
        if location != self.location: raise RuntimeError('Unexpected location')
        if not updates: return
        # Preserve ordering and supplier stages, deduplicate within a stage only.
        dedup={}
        for item in updates: dedup[item['inventoryItemId']]=item
        updates=list(dedup.values())
        self.writes.extend({'kind':'inventory','source':source,'sku':u.get('sku'),'quantity':u['quantity']} for u in updates)
        if self.dry_run: return
        mutation="""mutation($input:InventorySetQuantitiesInput!){inventorySetQuantities(input:$input){userErrors{field message}}}"""
        for i in range(0,len(updates),250):
            batch=updates[i:i+250]
            variables={'input':{'name':'available','reason':'correction','ignoreCompareQuantity':True,'quantities':[{'inventoryItemId':u['inventoryItemId'],'locationId':location,'quantity':u['quantity']} for u in batch]}}
            result=self.graphql(mutation,variables,write=True)['inventorySetQuantities']
            if result.get('userErrors'): raise RuntimeError(f"Shopify inventory userErrors: {result['userErrors']}")

    def status(self, product_id, new_status, source):
        if product_id not in self.products: raise RuntimeError(f'Unknown product: {product_id}')
        if self.products[product_id]['status'] == new_status: return
        self.writes.append({'kind':'status','source':source,'product_id':product_id,'status':new_status})
        if not self.dry_run:
            mutation="""mutation($input:ProductInput!){productUpdate(input:$input){product{id status} userErrors{field message}}}"""
            result=self.graphql(mutation,{'input':{'id':product_id,'status':new_status}},write=True)['productUpdate']
            if result.get('userErrors') or (result.get('product') or {}).get('status') != new_status:
                raise RuntimeError(f'Shopify did not confirm {new_status} for {product_id}: {result}')
        self.products[product_id]['status']=new_status
        for sku in self.products[product_id]['skus']:
            self.inventory[sku]['status']=new_status

    def archived(self):
        return {pid:p['skus'] for pid,p in self.products.items() if p['status']=='ARCHIVED'}
