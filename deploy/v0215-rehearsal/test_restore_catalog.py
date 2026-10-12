import copy
import unittest

from v0215_restore_catalog import RestoreCatalogError, verify_restore_catalog


class RestoreCatalogTests(unittest.TestCase):
    def setUp(self):
        self.source = {'server_version_num': 180004, 'bonus_nonzero': 0,
                       'columns': [{'table_name': 't', 'column_name': 'id', 'ordinal_position': 1, 'data_type': 'bigint'},
                                   {'table_name': 't', 'column_name': 'state', 'ordinal_position': 4, 'data_type': 'character varying'}],
                       'constraints': [{'table': 't', 'name': 'state_check', 'validated': False,
                                        'definition': "CHECK (((state)::text = ANY ((ARRAY['on'::character varying, 'off'::character varying])::text[]))) NOT VALID"}],
                       'indexes': [{'name': 't_pkey', 'valid': True}],
                       'rows': [{'table': 'public.t', 'count': 2, 'hash': 'unchanged'}]}
        self.restored = copy.deepcopy(self.source)
        self.restored['server_version_num'] = 180006
        self.restored['columns'][1]['ordinal_position'] = 2
        self.restored['constraints'][0]['definition'] = "CHECK (((state)::text = ANY (ARRAY[('on'::character varying)::text, ('off'::character varying)::text]))) NOT VALID"

    def test_realistic_roundtrip_keeps_data_and_logical_schema(self):
        self.assertTrue(verify_restore_catalog(self.source, self.restored)['logical_catalog_equal'])

    def test_comparison_does_not_mutate_evidence(self):
        before = copy.deepcopy((self.source, self.restored))
        verify_restore_catalog(self.source, self.restored)
        self.assertEqual(before, (self.source, self.restored))

    def test_real_changes_are_rejected(self):
        mutations = [lambda d: d['columns'][1].update(data_type='text'),
                     lambda d: d['columns'][1].update(column_name='other'),
                     lambda d: d['columns'][0].update(ordinal_position=9),
                     lambda d: d['columns'].pop(),
                     lambda d: d['constraints'][0].update(validated=True),
                     lambda d: d['constraints'][0].update(definition=d['constraints'][0]['definition'].replace("'off'", "'other'")),
                     lambda d: d['constraints'][0].update(definition=d['constraints'][0]['definition'].replace(' = ANY ', ' <> ALL ')),
                     lambda d: d['constraints'][0].update(definition=d['constraints'][0]['definition'].replace(' NOT VALID', '')),
                     lambda d: d['indexes'][0].update(valid=False),
                     lambda d: d['rows'][0].update(hash='changed'),
                     lambda d: d['rows'][0].update(count=3),
                     lambda d: d.update(bonus_nonzero=1),
                     lambda d: d.update(server_version_num=190000),
                     lambda d: d.update(server_version_num=180003),
                     lambda d: d['columns'].append(copy.deepcopy(d['columns'][0])),
                     lambda d: d['constraints'].append(copy.deepcopy(d['constraints'][0]))]
        for mutation in mutations:
            with self.subTest(mutation=mutations.index(mutation)):
                changed = copy.deepcopy(self.restored)
                mutation(changed)
                with self.assertRaises(RestoreCatalogError):
                    verify_restore_catalog(self.source, changed)

    def test_sql_string_contents_are_never_normalized(self):
        a, b = copy.deepcopy(self.source), copy.deepcopy(self.source)
        a['constraints'][0]['definition'] = "CHECK (state <> 'x (ARRAY[''on''::character varying])::text[]')"
        b['constraints'][0]['definition'] = "CHECK (state <> 'x ARRAY[(''on''::character varying)::text]')"
        with self.assertRaises(RestoreCatalogError):
            verify_restore_catalog(a, b)

    def test_unknown_expression_transform_is_rejected(self):
        self.restored['constraints'][0]['definition'] = self.restored['constraints'][0]['definition'].replace('character varying', 'varchar(1)')
        with self.assertRaises(RestoreCatalogError):
            verify_restore_catalog(self.source, self.restored)


if __name__ == '__main__':
    unittest.main()
