"""Fingerprint rendered HTML, excluding only the generated CSP nonce."""
import hashlib
import re

_NONCE = re.compile(rb'(<script nonce=")[A-Za-z0-9+/]{22}==(">window\.__APP_CONFIG__=)')


def frontend_digest(html):
    canonical, count = _NONCE.subn(rb'\1__CSP_NONCE_VALUE__\2', html)
    if count != 1:
        raise RuntimeError('frontend_config_nonce_shape')
    return hashlib.sha256(canonical).hexdigest()


def verify_frontend(reference, html, asset):
    expected = reference.get('frontend_contract_sha256')
    if not isinstance(expected, str) or not re.fullmatch('[a-f0-9]{64}', expected):
        raise RuntimeError('fresh_frontend_contract_required')
    if frontend_digest(html) != expected or hashlib.sha256(asset).hexdigest() != reference['asset_sha256']:
        raise RuntimeError('runtime_frontend_not_rehearsed')


def require_frontend_references(report):
    for phase in report.get('runs', []):
        if not isinstance(phase.get('frontend_contract_sha256'), str) or not re.fullmatch('[a-f0-9]{64}', phase['frontend_contract_sha256']):
            raise RuntimeError('fresh_frontend_contract_required')
