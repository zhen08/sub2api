import hashlib
import unittest

from v0215_frontend_contract import frontend_digest, require_frontend_references, verify_frontend


class FrontendTests(unittest.TestCase):
    def setUp(self):
        self.html = b'<html><script nonce="abcdefghijklmnopqrstuv==">window.__APP_CONFIG__={"title":"test"};</script><script src="/assets/index-a.js"></script></html>'
        self.asset = b'const asset = "original";'
        self.reference = {'frontend_contract_sha256': frontend_digest(self.html), 'asset_sha256': hashlib.sha256(self.asset).hexdigest()}

    def test_only_nonce_rotation_is_accepted(self):
        rotated = self.html.replace(b'abcdefghijklmnopqrstuv==', b'ABCDEFGHIJKLMNOPQRSTUV==')
        self.assertNotEqual(hashlib.sha256(self.html).hexdigest(), hashlib.sha256(rotated).hexdigest())
        verify_frontend(self.reference, rotated, self.asset)

    def test_real_page_or_asset_changes_are_rejected(self):
        for modified in (self.html.replace(b'test', b'changed'), self.html.replace(b'index-a', b'index-b'), self.html.replace(b'</html>', b'<script>evil()</script></html>')):
            with self.assertRaises(RuntimeError):
                verify_frontend(self.reference, modified, self.asset)
        with self.assertRaises(RuntimeError):
            verify_frontend(self.reference, self.html, self.asset + b'changed')

    def test_missing_or_duplicated_config_nonce_is_rejected(self):
        for html in (self.html.replace(b'nonce=', b'other='), self.html + self.html):
            with self.assertRaises(RuntimeError):
                frontend_digest(html)

    def test_historical_raw_hash_receipts_are_rejected_before_cutover(self):
        with self.assertRaisesRegex(RuntimeError, 'fresh_frontend_contract_required'):
            require_frontend_references({'runs': [{'frontend_sha256': 'a' * 64}]})
        require_frontend_references({'runs': [self.reference]})


if __name__ == '__main__':
    unittest.main()
