"""Prevent Selenium imports from being scoped only to setup_driver."""
import ast
import unittest
from pathlib import Path

class SeleniumImportsTest(unittest.TestCase):
    def test_download_dependencies_available_in_download_function(self):
        source = (Path(__file__).resolve().parents[1] / "seleniumToShopifyUpdater.py").read_text()
        tree = ast.parse(source)
        download = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "download_latest_file")
        names = set()
        for node in download.body:
            if isinstance(node, ast.ImportFrom):
                names.update(alias.asname or alias.name for alias in node.names)
        self.assertTrue({"By", "WebDriverWait", "EC"} <= names)
