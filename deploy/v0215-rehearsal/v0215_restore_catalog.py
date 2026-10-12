"""Compare a logical restore without confusing physical layout with schema.

Only PostgreSQL 18 -> the same or a newer PostgreSQL 18 minor is supported.
This comparator is for pg_dump/pg_restore, never for in-place migration checks.
"""
import copy
import re


class RestoreCatalogError(RuntimeError):
    pass


_TOKEN = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|::|[A-Za-z_][A-Za-z_0-9]*|[0-9]+|[^\s]")
_ENUM = re.compile(r"'[A-Za-z0-9_]+'\Z")


def _constraint_tokens(definition):
    tokens = _TOKEN.findall(definition)
    if not tokens or tokens[0] != 'CHECK':
        return tokens
    result = []
    i = 0
    while i < len(tokens):
        if tokens[i:i + 3] == ['(', 'ARRAY', '[']:
            j = i + 3
            values = []
            while j + 3 < len(tokens) and _ENUM.fullmatch(tokens[j]):
                if tokens[j + 1:j + 4] != ['::', 'character', 'varying']:
                    break
                values.append(tokens[j:j + 4])
                j += 4
                if tokens[j:j + 1] != [',']:
                    break
                j += 1
            if values and tokens[j:j + 6] == [']', ')', '::', 'text', '[', ']']:
                result += ['ARRAY', '[']
                for n, value in enumerate(values):
                    if n:
                        result.append(',')
                    result += ['('] + value + [')', '::', 'text']
                result.append(']')
                i = j + 6
                continue
        result.append(tokens[i])
        i += 1
    return result


def _canonical(catalog):
    result = copy.deepcopy(catalog)
    tables = {}
    identities = set()
    for column in result['columns']:
        identity = (column['table_name'], column['column_name'])
        position = column['ordinal_position']
        if identity in identities or type(position) is not int or position < 1:
            raise RestoreCatalogError('invalid_column_identity_or_position')
        identities.add(identity)
        tables.setdefault(identity[0], []).append(column)
    columns = []
    for name in sorted(tables):
        ordered = sorted(tables[name], key=lambda row: row['ordinal_position'])
        if len({row['ordinal_position'] for row in ordered}) != len(ordered):
            raise RestoreCatalogError('duplicate_column_position')
        for position, row in enumerate(ordered, 1):
            row['ordinal_position'] = position
            columns.append(row)
    result['columns'] = columns
    identities = set()
    for constraint in result['constraints']:
        identity = (constraint['table'], constraint['name'])
        if identity in identities:
            raise RestoreCatalogError('duplicate_constraint_identity')
        identities.add(identity)
        constraint['definition'] = _constraint_tokens(constraint['definition'])
    result['constraints'].sort(key=lambda row: (row['table'], row['name']))
    result['server_version_num'] = 180000
    return result


def verify_restore_catalog(source, restored):
    """Require equal logical schema and row hashes; return a bounded receipt."""
    a, b = source['server_version_num'], restored['server_version_num']
    if type(a) is not int or type(b) is not int or a // 10000 != 18 or b // 10000 != 18 or b < a:
        raise RestoreCatalogError('unsupported_restore_server_versions')
    left, right = _canonical(source), _canonical(restored)
    if left != right:
        changed = sorted(key for key in left.keys() | right.keys() if left.get(key) != right.get(key))
        raise RestoreCatalogError('restored_logical_catalog_drift:' + ','.join(changed))
    return {'source_server_version_num': a, 'restored_server_version_num': b,
            'logical_catalog_equal': True, 'columns': len(source['columns']),
            'constraints': len(source['constraints']), 'row_hashes_equal': source['rows'] == restored['rows']}
