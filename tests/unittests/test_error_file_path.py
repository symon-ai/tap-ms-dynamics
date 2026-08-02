import os

from tap_dynamics import sanitize_error_file_path


def test_legitimate_relative_path_is_allowed(tmp_path, monkeypatch):
    # A plain filename inside the working directory is a legitimate value and
    # must resolve to a writable path under that directory.
    monkeypatch.chdir(tmp_path)

    result = sanitize_error_file_path('error.json')

    assert result is not None
    assert result == os.path.join(os.path.realpath(str(tmp_path)), 'error.json')
    # The sanitized path must stay inside the working directory.
    assert result.startswith(os.path.realpath(str(tmp_path)) + os.sep)


def test_parent_traversal_is_rejected(tmp_path, monkeypatch):
    # WP-33452 (CWE-73): a '..' traversal escaping the working directory must
    # be neutralized so nothing is written outside the intended location.
    monkeypatch.chdir(tmp_path)

    assert sanitize_error_file_path('../evil.json') is None
    assert sanitize_error_file_path('../../etc/passwd') is None


def test_absolute_path_escape_is_rejected(tmp_path, monkeypatch):
    # An absolute path pointing outside the working directory is rejected.
    monkeypatch.chdir(tmp_path)

    assert sanitize_error_file_path('/etc/passwd') is None
    assert sanitize_error_file_path('/tmp/some_other_dir/error.json') is None


def test_empty_or_missing_value_returns_none(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert sanitize_error_file_path(None) is None
    assert sanitize_error_file_path('') is None
