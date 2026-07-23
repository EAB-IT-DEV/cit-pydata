# Known Issues

Items intentionally deferred as of the pre-1.0 audit. None sit on the primary,
actively-used code paths, but they are incomplete or unverified and should not be
relied on as-is until addressed.

## Incomplete / unverified methods

- **`salesforce.SalesforceSOAPClient`** — legacy SOAP email client. Re-authenticates
  on every call (never caches `sf_token`) and its SOAP `login` omits the security
  token, so it fails from non-whitelisted IPs. Not covered by tests.
- **`marketo.MarketoClient.sync_custom_object_records`** — explicitly incomplete:
  contains hardcoded single-record test queries (`where linkid = 'N00137649'`) and a
  `upsert_co_query_dict` that must be populated per custom object before use.
- **`sfsync.SFSyncClient.get_object_group`** — unimplemented stub (`pass`, returns
  `None`).
- **`sql.SQLClient.execute_with_timeout`** — the timeout is not actually enforced:
  `while cursor.nextset()` iterates result sets rather than bounding query runtime.
  The `text()` crash was fixed, but the timeout semantics need a redesign. Marked
  "not tested" in-code.
- **`sql.SQLClient.postgres_quickexport_df`** — marked "HAS NOT BEEN TESTED YET". The
  obvious engine-reference bug was fixed, but the method is otherwise unverified.
- **`box.BoxClient.get_file`** — not implemented; raises `NotImplementedError`.

## Test coverage

The `tests/` suite has two layers, all fully offline (no credentials, no network,
no optional test deps beyond `pytest`/`requests`/`boto3`):

- **Smoke / unit** (`test_imports.py`, `test_util.py`, `test_client_validation.py`):
  imports, pure helpers, logging, log cleanup, and constructor validation.
- **Mocked integration** (`test_integration_mocked.py`): live method paths with the
  service layer faked — SQL stored-procedure commit, S3 empty-prefix + pagination,
  SSM `describe_parameters` pagination, Salesforce prod OAuth (form-data + missing-cred
  guard), and Marketo batch aggregation + `submit_form` status handling.

**Still not covered** (would need `moto`/`responses` or real credentials): Box and SFTP
file transfers, the mailer chunked-upload/retry path, and real database execution
(the SQL engine is mocked, so `execute_sql`/`execute2` against a live DB is unverified).
Green tests mean "the fixed paths behave correctly against a faked backend," not "every
integration works end to end."
