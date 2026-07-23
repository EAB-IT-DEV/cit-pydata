"""Constructor / input validation tests.

These exercise the argument validation that now raises explicit exceptions
(previously bare ``assert`` statements, which are stripped under ``python -O``).
All failing paths raise before any SSM lookup or engine creation, so no
network or AWS credentials are required.
"""

import pytest

from cit_pydata.sql import api as sql_api
from cit_pydata.salesforce import api as sf_api
from cit_pydata.marketo import api as mkto_api
from cit_pydata.sftp import api as sftp_api


# --- SQLClient -------------------------------------------------------------


def test_sqlclient_requires_hostname():
    with pytest.raises(ValueError):
        sql_api.SQLClient({})


def test_sqlclient_rejects_unsupported_dialect():
    conn = {
        "sql_hostname": "h",
        "sql_user": "u",
        "sql_database": "d",
        "dialect": "bogus",
    }
    with pytest.raises(ValueError):
        sql_api.SQLClient(conn)


# --- SalesforceClient ------------------------------------------------------


def test_salesforce_requires_instance():
    with pytest.raises(ValueError):
        sf_api.SalesforceClient({})


def test_salesforce_requires_user_and_app():
    with pytest.raises(ValueError):
        sf_api.SalesforceClient({"instance": "prod"})


def test_get_field_value_from_relationship_lookup_validates_types():
    with pytest.raises(TypeError):
        sf_api.get_field_value_from_relationship_lookup("not-a-dict", "Field")
    with pytest.raises(ValueError):
        sf_api.get_field_value_from_relationship_lookup({"Field": 1}, "")
    assert sf_api.get_field_value_from_relationship_lookup({"Field": 1}, "Field") == 1


# --- MarketoClient ---------------------------------------------------------


def test_marketo_requires_valid_instance():
    with pytest.raises(ValueError):
        mkto_api.MarketoClient({})
    with pytest.raises(ValueError):
        mkto_api.MarketoClient({"instance": "not-a-real-instance", "app": "a"})


def test_marketo_requires_app():
    with pytest.raises(ValueError):
        mkto_api.MarketoClient({"instance": "production"})


# --- SFTPClient ------------------------------------------------------------


def test_sftp_requires_instance():
    with pytest.raises(ValueError):
        sftp_api.SFTPClient({})


def test_sftp_requires_port():
    with pytest.raises(ValueError):
        sftp_api.SFTPClient({"instance": "getpaid"})
