"""Exact legacy/current channel guards, backed by a read-only live projection."""
import copy
import json
from pathlib import Path
import unittest

import controller as c
from test_controller import FakeAPI


class ChannelAPI(FakeAPI):
    def __init__(self):
        super().__init__()
        self.channels = json.loads(Path(__file__).with_name('fixtures').joinpath(
            'live_channels_20260923.json').read_text())['channels']

    def get(self, path, params=None):
        if path == '/channels':
            return {'items': copy.deepcopy(self.channels), 'total': len(self.channels),
                    'page': 1, 'page_size': 100}
        return super().get(path, params)


class ChannelInventoryTests(unittest.TestCase):
    def test_exact_live_configuration_accepted_without_writes(self):
        api = ChannelAPI()
        before = copy.deepcopy(api.channels)
        self.assertEqual(c.inventory(api), c.inventory(FakeAPI()))
        self.assertEqual(api.channels, before)
        self.assertEqual(api.writes, [])

    def test_exact_legacy_configuration_remains_accepted(self):
        api = FakeAPI()
        self.assertEqual(c.inventory(api)['openai_key_count'], 2)
        self.assertEqual(api.writes, [])

    def assert_rejected(self, api):
        with self.assertRaisesRegex(c.PolicyError, 'source channel .* drift'):
            c.inventory(api)
        self.assertEqual(api.writes, [])

    def test_wrong_or_partial_mapping_rejected_for_each_channel(self):
        for index in range(2):
            for alias in ('codex-auto-review', 'gpt-5.5'):
                for wrong in ('random-model', 'gpt-6-astra', 'gpt-5.6-luna', None):
                    with self.subTest(index=index, alias=alias, wrong=wrong):
                        api = ChannelAPI()
                        mapping = api.channels[index]['model_mapping']['openai']
                        if wrong is None:
                            del mapping[alias]
                        else:
                            mapping[alias] = wrong
                        self.assert_rejected(api)

    def test_extra_alias_platform_and_missing_mapping_rejected(self):
        for index in range(2):
            for kind in ('extra_alias', 'extra_platform', 'empty', 'missing'):
                with self.subTest(index=index, kind=kind):
                    api = ChannelAPI()
                    ch = api.channels[index]
                    if kind == 'extra_alias':
                        ch['model_mapping']['openai']['random-alias'] = 'gpt-6-luna'
                    elif kind == 'extra_platform':
                        ch['model_mapping']['anthropic'] = {}
                    elif kind == 'empty':
                        ch['model_mapping'] = {}
                    else:
                        del ch['model_mapping']
                    self.assert_rejected(api)

    def test_group_specific_sol_mapping_remains_exact(self):
        for index in range(2):
            for target in ('gpt-6-luna', 'gpt-6-sol', None):
                with self.subTest(index=index, target=target):
                    api = ChannelAPI()
                    mapping = api.channels[index]['model_mapping']['openai']
                    if target is None:
                        # Remove the required group-8 rule; add an otherwise-valid
                        # group-8 rule to group 6, where it is still forbidden.
                        if api.channels[index]['group_ids'] == [8]:
                            del mapping['gpt-5.6-sol']
                        else:
                            mapping['gpt-5.6-sol'] = 'gpt-5.6-terra'
                    else:
                        mapping['gpt-5.6-sol'] = target
                    self.assert_rejected(api)

    def test_non_mapping_guards_remain_enforced(self):
        changes = {'id': 99, 'status': 'disabled', 'restrict_models': True,
                   'billing_model_source': 'original', 'features': 'unexpected',
                   'features_config': {'enabled': True}, 'model_pricing': [{}],
                   'apply_pricing_to_account_stats': True,
                   'account_stats_pricing_rules': [{}]}
        for index in range(2):
            for field, value in changes.items():
                with self.subTest(index=index, field=field):
                    api = ChannelAPI()
                    api.channels[index][field] = value
                    self.assert_rejected(api)

    def test_channel_membership_guards_remain_enforced(self):
        for kind in ('missing', 'extra', 'combined', 'duplicate_group'):
            with self.subTest(kind=kind):
                api = ChannelAPI()
                if kind == 'missing':
                    api.channels.pop()
                elif kind == 'extra':
                    extra = copy.deepcopy(api.channels[0])
                    extra['id'] = 99
                    api.channels.append(extra)
                elif kind == 'combined':
                    api.channels[0]['group_ids'] = [8, 6]
                else:
                    api.channels[0]['group_ids'] = [8, 8]
                self.assert_rejected(api)


if __name__ == '__main__':
    unittest.main()
