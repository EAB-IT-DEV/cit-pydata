"""Unit tests for cit_pydata.util.api pure helpers, logging, and log cleanup."""

import base64
import logging
import os
import re
import time

import pytest

from cit_pydata.util import api as util_api


# --- encode_base64 ---------------------------------------------------------


def test_encode_base64_known_value():
    assert util_api.encode_base64("hello") == base64.b64encode(b"hello").decode(
        "ascii"
    )


def test_encode_base64_none_raises():
    with pytest.raises(ValueError):
        util_api.encode_base64(None)


# --- multireplace ----------------------------------------------------------


def test_multireplace_prefers_longer_match():
    assert util_api.multireplace("hey abc", {"ab": "AB", "abc": "ABC"}) == "hey ABC"


def test_multireplace_ignore_case():
    assert util_api.multireplace("HELLO", {"hello": "x"}, ignore_case=True) == "x"


def test_multireplace_empty_replacements_is_noop():
    assert util_api.multireplace("unchanged", {}) == "unchanged"


# --- get_datetime_string ---------------------------------------------------


def test_get_datetime_string_format():
    assert re.fullmatch(r"\d{8}_\d{6}", util_api.get_datetime_string())


# --- remove_file -----------------------------------------------------------


def test_remove_file_removes_and_tolerates_missing(tmp_path):
    target = tmp_path / "gone.txt"
    target.write_text("x")
    util_api.remove_file(str(target))
    assert not target.exists()
    # second call on a now-missing file must not raise
    util_api.remove_file(str(target))


# --- list_dict_to_csv ------------------------------------------------------


def test_list_dict_to_csv_roundtrip(tmp_path):
    ok = util_api.list_dict_to_csv(
        str(tmp_path), "out.csv", [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    )
    assert ok is True
    content = (tmp_path / "out.csv").read_text()
    assert "a,b" in content
    assert "1,2" in content and "3,4" in content


def test_list_dict_to_csv_rejects_non_list():
    assert util_api.list_dict_to_csv("anywhere", "x.csv", "not-a-list") is False


def test_list_dict_to_csv_empty_without_columns(tmp_path):
    assert util_api.list_dict_to_csv(str(tmp_path), "x.csv", []) is False


# --- get_logger ------------------------------------------------------------


def test_get_logger_attaches_single_handler_and_honors_level():
    name = "cit_pydata_test_logger_level"
    logger = util_api.get_logger(name, "warning")
    assert isinstance(logger, logging.Logger)
    assert len(logger.handlers) == 1
    assert logger.handlers[0].level == logging.WARNING


def test_get_logger_does_not_duplicate_handlers_on_reuse():
    name = "cit_pydata_test_logger_reuse"
    first = util_api.get_logger(name, "info")
    handler_count = len(first.handlers)
    second = util_api.get_logger(name, "info")
    assert first is second
    assert len(second.handlers) == handler_count  # no duplicate handler added


# --- cleanup_logs (directly) ----------------------------------------------


def _touch_old(path, days_old=30):
    path.write_text("log")
    old = time.time() - days_old * 86400
    os.utime(path, (old, old))


def test_cleanup_logs_removes_only_old_matching_files(tmp_path):
    old_match = tmp_path / "job_20200101.log"
    fresh_match = tmp_path / "job_today.log"
    _touch_old(old_match, days_old=30)
    fresh_match.write_text("log")  # current mtime, should be retained

    regex = re.compile(r"^job_.*\.log$")
    removed = util_api.cleanup_logs(
        log_path=str(tmp_path),
        log_file_regex=regex,
        retain_last_n={"days": 1},
    )
    assert removed == 1
    assert not old_match.exists()
    assert fresh_match.exists()


def test_cleanup_logs_regex_does_not_match_other_job_prefix(tmp_path):
    """The get_logger cleanup regex is anchored on the '{base}_' separator so
    base name 'job' must not sweep another job's 'job2_*.log' files."""
    ours = tmp_path / "job_20200101.log"
    other = tmp_path / "job2_20200101.log"
    _touch_old(ours, days_old=30)
    _touch_old(other, days_old=30)

    # regex constructed exactly as get_logger builds it
    base_file_name = "job"
    regex = re.compile(rf"^{re.escape(base_file_name)}_.*\.log$")
    removed = util_api.cleanup_logs(
        log_path=str(tmp_path),
        log_file_regex=regex,
        retain_last_n={"days": 1},
    )
    assert removed == 1
    assert not ours.exists()
    assert other.exists()  # must NOT be deleted


# --- get_log_level ---------------------------------------------------------


def test_get_log_level_defaults_to_info_when_env_missing(tmp_path):
    missing = tmp_path / "does_not_exist.env"
    assert util_api.get_log_level(path_to_env=str(missing)) == "INFO"
