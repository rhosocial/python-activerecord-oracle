# tests/rhosocial/activerecord_oracle_test/feature/backend/test_ssl_connection_params.py
"""Regression tests: Oracle SSL/TLS config must reach ``oracledb.connect``.

python-oracledb has no ``ssl_ca``/``ssl_cert``/``ssl_key`` connection keywords.
TLS material is supplied either through a wallet or through a pre-built
``ssl_context``, and the wire protocol is selected with ``protocol='tcps'``.
These tests pin that the config performs that translation instead of silently
dropping the generic SSL fields it inherits from ``SSLMixin``.
"""

import ssl
import subprocess
import tempfile
from pathlib import Path

import pytest

from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig


def make_config(**kwargs):
    defaults = dict(host="db.example.com", port=1521, database="ORCL")
    defaults.update(kwargs)
    return OracleConnectionConfig(**defaults)


@pytest.fixture(scope="module")
def pem_files():
    """Self-signed CA plus a client certificate/key, generated on the fly."""
    directory = tempfile.mkdtemp()
    paths = {name: str(Path(directory) / name) for name in ("ca.crt", "ca.key", "client.crt", "client.key")}
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", paths["ca.key"], "-out", paths["ca.crt"], "-days", "1", "-subj", "/CN=TestCA"],
        check=True, capture_output=True,
    )
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", paths["client.key"], "-out", paths["client.crt"], "-days", "1", "-subj", "/CN=client"],
        check=True, capture_output=True,
    )
    return paths


def test_no_tls_configured_yields_no_keywords():
    """Leave python-oracledb entirely alone rather than forcing a protocol."""
    assert make_config().get_ssl_connection_params() == {}


@pytest.mark.parametrize("mode", ["require", "verify-ca", "verify-full"])
def test_ssl_mode_selects_tcps(mode):
    assert make_config(ssl_mode=mode).get_ssl_connection_params()["protocol"] == "tcps"


@pytest.mark.parametrize("mode", ["disable", "off", "tcp"])
def test_disabling_ssl_mode_selects_plain_tcp(mode):
    assert make_config(ssl_mode=mode).get_ssl_connection_params()["protocol"] == "tcp"


def test_explicit_protocol_wins_over_ssl_mode():
    params = make_config(ssl_mode="disable", protocol="tcps").get_ssl_connection_params()
    assert params["protocol"] == "tcps"


def test_wallet_is_forwarded():
    params = make_config(wallet_location="/opt/oracle/wallet", wallet_password="pw").get_ssl_connection_params()
    assert params["wallet_location"] == "/opt/oracle/wallet"
    assert params["wallet_password"] == "pw"
    assert params["protocol"] == "tcps"


def test_server_dn_matching_is_forwarded_only_when_set():
    """``None`` must stay absent: python-oracledb defaults it to True, whereas
    the generic SSLMixin defaults ``ssl_verify_cert`` to False. Emitting False
    unconditionally would silently weaken verification."""
    assert "ssl_server_dn_match" not in make_config().get_ssl_connection_params()

    params = make_config(ssl_server_dn_match=False, ssl_server_cert_dn="CN=db").get_ssl_connection_params()
    assert params["ssl_server_dn_match"] is False
    assert params["ssl_server_cert_dn"] == "CN=db"


def test_pem_material_becomes_an_ssl_context(pem_files):
    """oracledb thin mode takes no ssl_ca/ssl_cert/ssl_key, so they must be
    translated into a single SSLContext."""
    params = make_config(
        ssl_ca=pem_files["ca.crt"],
        ssl_cert=pem_files["client.crt"],
        ssl_key=pem_files["client.key"],
        ssl_verify_cert=True,
    ).get_ssl_connection_params()

    assert isinstance(params["ssl_context"], ssl.SSLContext)
    assert params["ssl_context"].verify_mode == ssl.CERT_REQUIRED
    assert params["protocol"] == "tcps"


def test_pem_material_without_verification_still_builds_context(pem_files):
    params = make_config(ssl_ca=pem_files["ca.crt"]).get_ssl_connection_params()
    context = params["ssl_context"]
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False


def test_client_certificate_without_key_is_rejected(pem_files):
    """A cert/key mismatch must be reported by us, not surface as an opaque
    driver error later."""
    with pytest.raises(ValueError, match="ssl_cert requires ssl_key"):
        make_config(ssl_cert=pem_files["client.crt"]).get_ssl_connection_params()
