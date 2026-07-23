# Changelog

All notable changes to `cit-pydata` are documented here. The format loosely
follows [Keep a Changelog](https://keepachangelog.com/); versions use PEP 440.

## [Unreleased]

Pre-1.0 audit pass across every client module. See `KNOWN_ISSUES.md` for items
intentionally deferred.

### Fixed
- **salesforce:** `get_object_by_record_id` / `get_replicatable_objects` referenced a
  non-existent `self.sf` attribute and crashed on every call; now use `self.ssf`
  (and `get_replicatable_objects` builds the client via `_get_simple_sf()`).
- **salesforce:** production OAuth sent `client_secret`/`password` as URL query
  parameters; now sent as form data with the correct content-type header (no secrets
  in the URL).
- **salesforce:** missing SSM credentials raise a clear error instead of a
  `None + str` `TypeError`; a failed `SSMClient` init now propagates instead of
  causing a later `NameError`.
- **marketo:** replaced `DataFrame.append()` (removed in pandas 2.x) with `pd.concat`.
- **marketo:** `delete_custom_object_records` operated on the wrong DataFrame and could
  delete the wrong records; now uses the computed delete set, guarded against empty.
- **marketo:** `get_program` referenced a non-existent `self.client`; fixed the mutable
  default argument and the shared standard-field list mutation; `submit_form` no longer
  logs the bearer token and now checks the HTTP status and returns the response.
- **aws:** `S3Client.get_object_metadata` raised `KeyError` on empty prefixes;
  `describe_parameters` now paginates via `NextToken`.
- **box:** corrected the `BoxAPIException` reference (was a non-existent attribute) and
  re-raise on auth failure.
- **sftp:** fixed a double upload on overwrite; `move_file_on_sftp` no longer leaks its
  session (added `auto_close`/`finally`).
- **util:** `get_logger` no longer silently skips the file handler when a root/ancestor
  logger already has a handler; log-cleanup regex is escaped and anchored so it cannot
  delete an unrelated job's logs; log-level parsing honors WARNING/ERROR; `cleanup_logs`
  no longer freezes "now" at import time; `get_log_level` never returns `None`.
- **sql:** `execute2` / `execute_with_timeout` wrap statements in `text()` (SQLAlchemy
  1.4); the psycopg2 paths use `engine.begin()` instead of deprecated `engine.execute`;
  `upsert_df` / `upsert_df_2` no longer crash on default arguments;
  `postgres_quickexport_df` engine reference fixed.

### Changed
- Constructors and input validators raise `ValueError`/`TypeError` instead of using
  `assert` (which is stripped under `python -O`), across `SQLClient`,
  `SalesforceClient`, `MarketoClient`, `SFTPClient`, `JobLogClient`, and
  `get_field_value_from_relationship_lookup`.
- `aws.S3Client` / `aws.SSMClient` raise on session-init failure instead of deferring
  to a later `NoneType` error.
- `sfsync.SFSyncClient` raises on an unsupported (non-pyodbc) dialect instead of
  returning a half-constructed client.
- `joblog.JobLogClient.update_joblog` is now a `@staticmethod`.

### Added
- `sql`: `sql_port` connection option (parity with the CorpIT copy).
- First test suite under `tests/` plus a `test` optional-dependency extra and pytest
  config: import smoke tests, `util` unit tests, constructor validation, and mocked
  integration tests for the fixed live paths (SQL stored-procedure commit, S3/SSM
  pagination, Salesforce prod OAuth, Marketo batch aggregation / `submit_form`).
