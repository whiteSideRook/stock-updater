# Daily inventory refactor — test-first release

Keep the original scripts as supplier-specific rule modules. `daily_inventory.py` coordinates them; `shopify_shared.py` fetches Shopify once, routes mutations, and tracks status changes. Ongoing is refreshed only in live mode. Supplier execution order remains Ekkia → Eldorado → local-surplus reactivation.

## Offline smoke test (no external calls)

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

## Fixture dry run

```bash
# PowerShell
$env:SHOPIFY_STORE='example.myshopify.com'
$env:ACCESS_TOKEN='fixture'
$env:ONGOING_STATE_FILE='tests/fixtures/ongoing_state.json'
python daily_inventory.py --fixture-dir tests/fixtures --report fixture_report.json
```

## Live-data dry run (READ-ONLY against Shopify, supplier sources)

Use your existing GitHub Actions secrets and a restored Ongoing checkpoint. Run the workflow manually, select `dry-run`. No Shopify mutations are sent. **Dry-run does not refresh Ongoing**, so surplus evaluation uses the last saved checkpoint. The report is written on the runner but not uploaded as an artifact, to avoid storage costs and exposing inventory data. Review the run log.

## Live mode

After confirming dry-run results, set repository **variable** `INVENTORY_LIVE_ENABLED=true` in GitHub Settings → Secrets and variables → Actions → Variables. Schedule and manually selected live runs require this gate. The CLI also requires `ALLOW_LIVE_INVENTORY=YES` and `--live`.

**Important review items**: supplier CSV parsing and existing business logic are retained; the Eldorado script still parses its file a second time for the cleanup decisions. This release removes repeated Shopify reads, centralizes mutations, and avoids a second archived-product scan, but does not yet unify all CSV parsing. It changes behavior for duplicate Shopify SKUs and >100 variants: it aborts safely rather than silently accepting incomplete/overwritten data. Mutations now check Shopify `userErrors` and fail instead of continuing silently. The shared client deduplicates repeated inventory items *within a single update stage* but not between supplier stages. This is intentional to retain the original precedence.

The workflow retains one rolling Ongoing cache by deleting older matching caches only after a successful live run. Other cache namespaces or artifacts are not touched. The new cache is not deleted. A dry run doesn't save a checkpoint. Scheduled runs are gated off until explicitly enabled.

**Rollback**: preserve the previous workflow and scripts in Git history. If comparison reveals differences, leave `INVENTORY_LIVE_ENABLED` unset and restore the prior commit. Never run both workflows concurrently.
