"""Mocked integration tests for the live code paths.

These exercise the methods that actually talk to external services, with the
service layer faked (no network, no credentials, no optional test deps). They
are regression guards for the bugs fixed in the pre-1.0 audit:

- SQL stored-procedure commit (the original reported bug)
- S3 empty-prefix KeyError + pagination
- SSM describe_parameters pagination
- Salesforce prod OAuth using form data (not URL query params) + missing-cred guard
- Marketo batch aggregation (pd.concat) + no shared standard-field mutation
- Marketo submit_form HTTP status handling

Everything is faked via monkeypatch, so no dotenv / boto session / real HTTP is
needed. Live paths not covered here (Box/SFTP transfers, mailer chunked upload)
are noted in KNOWN_ISSUES.md.
"""

import pytest

from cit_pydata.sql import api as sql_api
from cit_pydata.aws import api as aws_api
from cit_pydata.salesforce import api as sf_api
from cit_pydata.marketo import api as mkto_api


# ---------------------------------------------------------------------------
# SQL: execute_stored_procedure must COMMIT (the original reported bug)
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, rec):
        self._rec = rec

    def callproc(self, name, params):
        self._rec["callproc"] = (name, params)

    def execute(self, statement, params=None):
        self._rec["execute"] = (statement, params)

    def commit(self):
        self._rec["cursor_commit"] = self._rec.get("cursor_commit", 0) + 1

    def close(self):
        self._rec["cursor_closed"] = True


class _FakeConn:
    def __init__(self, rec):
        self._rec = rec

    def cursor(self):
        return _FakeCursor(self._rec)

    def commit(self):
        self._rec["conn_commit"] = self._rec.get("conn_commit", 0) + 1

    def close(self):
        self._rec["conn_closed"] = True


class _FakeEngine:
    def __init__(self, rec):
        self._rec = rec

    def raw_connection(self):
        return _FakeConn(self._rec)


def _make_sql_client(monkeypatch, dialect, rec):
    monkeypatch.setattr(
        sql_api.SQLClient, "_get_sql_engine", lambda self: _FakeEngine(rec)
    )
    return sql_api.SQLClient(
        {
            "sql_hostname": "h",
            "sql_user": "u",
            "sql_database": "d",
            "dialect": dialect,
        }
    )


def test_execute_stored_procedure_pymssql_commits(monkeypatch):
    rec = {}
    client = _make_sql_client(monkeypatch, "pymssql", rec)
    assert client.execute_stored_procedure("usp_Test", ["a", 1]) is True
    assert rec.get("callproc") == ("usp_Test", ["a", 1])
    # regression guard: the connection MUST be committed, else the SP's writes
    # are silently rolled back on close (the original bug).
    assert rec.get("conn_commit") == 1
    assert rec.get("conn_closed") is True


def test_execute_stored_procedure_pyodbc_commits(monkeypatch):
    rec = {}
    client = _make_sql_client(monkeypatch, "pyodbc", rec)
    assert client.execute_stored_procedure("usp_Test", ["a"]) is True
    assert rec.get("execute") == ("usp_Test ?", ["a"])
    assert rec.get("cursor_commit", 0) >= 1


# ---------------------------------------------------------------------------
# AWS: fake boto session/client injected via _get_boto_session
# ---------------------------------------------------------------------------


class _FakeBotoClient:
    def __init__(self, s3_pages=None, ssm_params=None, describe_pages=None):
        self._s3_pages = s3_pages or []
        self._s3_call = 0
        self._ssm_params = ssm_params or {}
        self._describe_pages = describe_pages or []
        self._describe_call = 0

    # --- S3 ---
    def list_objects_v2(self, **kwargs):
        page = self._s3_pages[self._s3_call]
        self._s3_call += 1
        return dict(page)  # copy; code calls .pop on it

    # --- SSM ---
    def get_parameter(self, Name, WithDecryption=False):
        if Name not in self._ssm_params:
            raise Exception("ParameterNotFound")
        return {"Parameter": {"Value": self._ssm_params[Name]}}

    def describe_parameters(self, **kwargs):
        # return successive pages regardless of token, but honor the loop's
        # NextToken presence to advance
        idx = self._describe_call
        self._describe_call += 1
        return self._describe_pages[idx]


class _FakeSession:
    def __init__(self, client):
        self._client = client

    def client(self, name):
        return self._client


@pytest.fixture
def patch_boto(monkeypatch):
    def _install(fake_client):
        monkeypatch.setattr(
            aws_api, "_get_boto_session", lambda **kwargs: _FakeSession(fake_client)
        )

    return _install


def test_s3_get_object_metadata_empty_prefix(patch_boto):
    # empty listing => no "Contents" key; must not KeyError
    patch_boto(_FakeBotoClient(s3_pages=[{}]))
    client = aws_api.S3Client(environment="dev", iam_user="user")
    response, df = client.get_object_metadata(Bucket="b", Prefix="empty/")
    assert df.empty


def test_s3_get_object_metadata_paginates(patch_boto):
    pages = [
        {"Contents": [{"Key": "a"}], "NextContinuationToken": "tok"},
        {"Contents": [{"Key": "b"}]},
    ]
    patch_boto(_FakeBotoClient(s3_pages=pages))
    client = aws_api.S3Client(environment="dev", iam_user="user")
    _, df = client.get_object_metadata(Bucket="b")
    assert sorted(df["Key"].tolist()) == ["a", "b"]


def test_ssm_get_parameter_success_and_missing(patch_boto):
    patch_boto(_FakeBotoClient(ssm_params={"/x/y": "secret"}))
    client = aws_api.SSMClient(environment="dev", iam_user="user")
    assert client.get_parameter("/x/y", with_decryption=True) == "secret"
    # missing parameter -> boto raises -> method returns None (logged)
    assert client.get_parameter("/does/not/exist") is None


def test_ssm_describe_parameters_paginates(patch_boto):
    pages = [
        {"Parameters": [{"Name": "p1"}], "NextToken": "n"},
        {"Parameters": [{"Name": "p2"}]},  # no NextToken -> loop terminates
    ]
    patch_boto(_FakeBotoClient(describe_pages=pages))
    client = aws_api.SSMClient(environment="dev", iam_user="user")
    result = client.describe_parameters(filter_parameters=[{"Key": "Name"}])
    assert [p["Name"] for p in result] == ["p1", "p2"]


# ---------------------------------------------------------------------------
# Salesforce: OAuth flow with faked SSM + faked requests
# ---------------------------------------------------------------------------


class _FakeSSMClient:
    """Returns a credential value based on the parameter name suffix."""

    _suffix_map = {}

    def __init__(self, *args, **kwargs):
        pass

    def get_parameter(self, name, with_decryption=False):
        for suffix, value in self._suffix_map.items():
            if name.endswith(suffix):
                return value
        return None


class _FakeResponse:
    def __init__(self, status_code=200, json_data=None, raise_exc=None):
        self.status_code = status_code
        self._json = json_data or {}
        self._raise = raise_exc
        self.text = "fake response body"

    def raise_for_status(self):
        if self._raise is not None:
            raise self._raise

    def json(self):
        return self._json


@pytest.fixture
def patch_sf_env(monkeypatch):
    # avoid dotenv / real SSM during SalesforceClient construction + auth
    monkeypatch.setattr(
        sf_api.util_api, "get_environment_variable", lambda **kwargs: "dev"
    )
    monkeypatch.setattr(sf_api.aws_api, "SSMClient", _FakeSSMClient)


def _sf_conn():
    return {
        "instance": "prod",
        "user": "svc",
        "app": "myapp",
        "base_ssm_parameter_name": "/eab-pydata/salesforce/",
    }


def test_salesforce_prod_auth_uses_form_data_not_query_params(patch_sf_env, monkeypatch):
    _FakeSSMClient._suffix_map = {
        "/username": "u@example.com",
        "/password": "pw",
        "/security_token": "tok",
        "/client_id": "cid",
        "/client_secret": "csecret",
    }

    captured = {}

    import requests

    def fake_post(self, url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return _FakeResponse(
            json_data={"access_token": "AT", "instance_url": "https://x.my.salesforce.com"}
        )

    monkeypatch.setattr(requests.Session, "post", fake_post)

    client = sf_api.SalesforceClient(_sf_conn())
    client._authenticate()

    # security regression: prod must send credentials as form data, never in
    # the URL query string.
    assert "login.salesforce.com" in captured["url"]
    assert "data" in captured["kwargs"]
    assert "params" not in captured["kwargs"]
    assert client.access_token == "AT"


def test_salesforce_missing_credential_raises(patch_sf_env, monkeypatch):
    # password missing -> guard must raise ValueError, not None + str TypeError
    _FakeSSMClient._suffix_map = {
        "/username": "u@example.com",
        "/client_id": "cid",
        "/client_secret": "csecret",
        # no /password, no /security_token
    }

    import requests

    monkeypatch.setattr(
        requests.Session,
        "post",
        lambda self, url, **kwargs: pytest.fail("should not reach HTTP call"),
    )

    client = sf_api.SalesforceClient(_sf_conn())
    with pytest.raises(ValueError):
        client._authenticate()


# ---------------------------------------------------------------------------
# Marketo: batch aggregation + submit_form status handling
# ---------------------------------------------------------------------------


@pytest.fixture
def patch_mkto_env(monkeypatch):
    monkeypatch.setattr(
        mkto_api.util_api, "get_environment_variable", lambda **kwargs: "dev"
    )


def _make_marketo():
    return mkto_api.MarketoClient(
        {
            "instance": "production",
            "app": "myapp",
            "base_ssm_parameter_name": "/eab-pydata/marketo/",
        }
    )


def test_marketo_get_custom_object_records_aggregates(patch_mkto_env, monkeypatch):
    mc = _make_marketo()

    captured = {}

    def fake_execute(**kwargs):
        captured["fields"] = kwargs.get("fields")
        return [
            {"seq": 0, "uniqueId": "u1"},
            {"seq": 1, "uniqueId": "u2"},
        ]

    monkeypatch.setattr(mc, "execute", fake_execute)

    df = mc.get_custom_object_records(
        "myObject_c",
        filter_type="linkId",
        filter_list=[{"linkId": "x"}, {"linkId": "y"}],
        custom_field_list=["extra_field"],
    )
    # pd.concat aggregation worked (replaces removed DataFrame.append)
    assert len(df) == 2
    assert "uniqueId" in df.columns
    assert "seq" not in df.columns
    # custom field forwarded to the request...
    assert "extra_field" in captured["fields"]
    # ...but the shared standard-field list was NOT mutated
    assert "extra_field" not in mc.CUSTOM_OBJECT_STANDARD_FIELDS
    assert len(mc.CUSTOM_OBJECT_STANDARD_FIELDS) == 5


def test_marketo_submit_form_status_handling(patch_mkto_env, monkeypatch):
    mc = _make_marketo()
    monkeypatch.setattr(mc, "_authenticate", lambda: None)

    class _FakeMkto:
        token = "tok"
        host = "https://mkto.example.com"

        def authenticate(self):
            pass

    mc.marketo = _FakeMkto()

    import requests

    # success
    monkeypatch.setattr(
        requests, "post", lambda **kwargs: _FakeResponse(json_data={"ok": True})
    )
    assert mc.submit_form({"input": []}) == {"ok": True}

    # HTTP error -> returns None (not an unhandled exception)
    monkeypatch.setattr(
        requests,
        "post",
        lambda **kwargs: _FakeResponse(status_code=400, raise_exc=Exception("400")),
    )
    assert mc.submit_form({"input": []}) is None
