import json, os, subprocess, sys, tempfile, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
FIX=ROOT/'tests'/'fixtures'
class SmokeTest(unittest.TestCase):
    def test_fixture_dry_run(self):
        with tempfile.TemporaryDirectory() as td:
            env=os.environ.copy()
            env.update(SHOPIFY_STORE='example.myshopify.com',ACCESS_TOKEN='fixture',ONGOING_STATE_FILE=str(FIX/'ongoing_state.json'))
            report=Path(td)/'report.json'
            proc=subprocess.run([sys.executable,str(ROOT/'daily_inventory.py'),'--fixture-dir',str(FIX),'--report',str(report)],cwd=ROOT,env=env,text=True,capture_output=True)
            self.assertEqual(proc.returncode,0,proc.stdout+'\n'+proc.stderr)
            data=json.loads(report.read_text())
            self.assertEqual(data['mode'],'dry-run')
            self.assertEqual(data['shopify_requests'],0)
            self.assertTrue(any(x['source']=='Ekkia' and x['kind']=='inventory' for x in data['operations']))
            self.assertTrue(any(x['source']=='Eldorado' and x['kind']=='inventory' for x in data['operations']))
            self.assertTrue(any(x['source']=='Ongoing surplus' and x['product_id']=='gid://shopify/Product/4' for x in data['operations']))
    def test_live_requires_opt_in(self):
        env=os.environ.copy();env.pop('ALLOW_LIVE_INVENTORY',None)
        p=subprocess.run([sys.executable,str(ROOT/'daily_inventory.py'),'--live'],cwd=ROOT,env=env,text=True,capture_output=True)
        self.assertNotEqual(p.returncode,0)
        self.assertIn('ALLOW_LIVE_INVENTORY',p.stderr)
if __name__=='__main__':unittest.main()
