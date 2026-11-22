"""Daily inventory coordinator. Default: no Shopify mutations.

Dry-run fetches real source data unless --fixture-dir is used; Ongoing checkpoint
is NOT refreshed in dry-run. Use --live only after reviewing a dry-run report.
"""
import argparse, json, os, sys
from pathlib import Path
from collections import Counter
from shopify_shared import Shopify


def run(dry_run=True, fixture_dir=None, skip_ongoing=False, report='inventory_report.json'):
    if fixture_dir:
        for k in ('ONGOING_BASE_URL','ONGOING_USERNAME','ONGOING_PASSWORD','ONGOING_GOODS_OWNER_ID'):
            os.environ.setdefault(k,'fixture')
        os.environ.setdefault('APP_URL','fixture://local')
        os.environ.setdefault('APP_PASSWORD','fixture')
        os.environ.setdefault('FTP_HOST','fixture')
        os.environ.setdefault('FTP_USER','fixture')
        os.environ.setdefault('FTP_PASS','fixture')
        os.environ.setdefault('FILENAME','eldorado.csv')
    import ongoing_state as ongoing
    import seleniumToShopifyUpdater as ekkia
    import secondStockUpdater as eldorado
    import activate_local_surplus as surplus

    if not dry_run and not skip_ongoing:
        ongoing.main()
    elif dry_run:
        print('DRY RUN: Ongoing refresh skipped; existing checkpoint used for surplus decisions')

    client=Shopify(dry_run=dry_run)
    if fixture_dir:
        data=json.loads((Path(fixture_dir)/'shopify.json').read_text(encoding='utf-8'))
        client.location=data['location']; client.inventory=data['inventory']; client.products={}
        for sku,item in client.inventory.items():
            p=client.products.setdefault(item['product_id'],{'status':item['status'],'skus':set()}); p['skus'].add(sku)
    else: client.load()

    # Adapt existing supplier logic without changing its stock/archiving decisions.
    ekkia.get_location_id=eldorado.get_location_id=lambda:client.location
    ekkia.fetch_inventory_items=eldorado.fetch_inventory_items=lambda:client.inventory
    ekkia.update_inventory=lambda updates, location, batch_size=250:client.update(updates,location,'Ekkia')
    eldorado.update_inventory=lambda updates, location, batch_size=250:client.update(updates,location,'Eldorado')
    ekkia.archive_products=lambda items,dry_run=True:[client.status(x['product_id'],'ARCHIVED','Ekkia') for x in items]
    ekkia.unarchive_products=lambda items,dry_run=True:[client.status(x['product_id'],'ACTIVE','Ekkia') for x in items]
    eldorado.archive_products=lambda items:[client.status(x['product_id'],'ARCHIVED','Eldorado') for x in items]
    eldorado.unarchive_products=lambda items:[client.status(x['product_id'],'ACTIVE','Eldorado') for x in items]
    surplus.fetch_archived_products=client.archived
    surplus.activate_product=lambda pid:client.status(pid,'ACTIVE','Ongoing surplus')

    if fixture_dir:
        ekkia.download_latest_file=lambda:str(Path(fixture_dir)/'ekkia.csv')
        eldorado.fetch_csv_from_ftp=lambda:str(Path(fixture_dir)/'eldorado.csv')
    # Preserve Ekkia -> Eldorado -> Ongoing reactivation order.
    try:
        ekkia.main()
        eldorado.main()
        surplus.main()
    finally:
        counts=Counter((w['source'],w['kind']) for w in client.writes)
        result={'mode':'dry-run' if dry_run else 'live','shopify_requests':client.calls,'operations':client.writes,'summary':{f'{k[0]}:{k[1]}':v for k,v in counts.items()}}
        Path(report).parent.mkdir(parents=True,exist_ok=True)
        Path(report).write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
        print(f'Report written: {report}; operations: {len(client.writes)}; Shopify requests: {client.calls}')
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--live',action='store_true',help='Allow Shopify writes (never the default)')
    parser.add_argument('--fixture-dir',help='Use local fixture supplier files and Shopify snapshot')
    parser.add_argument('--skip-ongoing',action='store_true',help='Reuse existing checkpoint')
    parser.add_argument('--report',default='inventory_report.json')
    args=parser.parse_args()
    if args.live and args.fixture_dir: parser.error('Live mode cannot use fixture data')
    if args.live and os.environ.get('ALLOW_LIVE_INVENTORY')!='YES':parser.error('Live mode requires ALLOW_LIVE_INVENTORY=YES')
    run(not args.live,args.fixture_dir,args.skip_ongoing,args.report)
