import json
import os

from tap_dynamics import resolve_safe_error_file_path


def test_legitimate_in_bounds_path_is_resolved_and_writable(tmp_path):
    # WP-32488: a legitimate, platform-controlled in-bounds path must still be accepted.
    base_dir = str(tmp_path)
    safe_path = resolve_safe_error_file_path('error.json', base_dir)

    assert safe_path is not None
    assert os.path.realpath(safe_path) == os.path.join(os.path.realpath(base_dir), 'error.json')

    error_info = {'message': 'boom', 'code': 'ERR'}
    with open(safe_path, 'w', encoding='utf-8') as fp:
        json.dump(error_info, fp)

    with open(safe_path, encoding='utf-8') as fp:
        assert json.load(fp) == error_info


def test_absolute_path_inside_base_dir_is_accepted(tmp_path):
    base_dir = str(tmp_path)
    absolute_in_bounds = os.path.join(base_dir, 'sub', 'error.json')

    safe_path = resolve_safe_error_file_path(absolute_in_bounds, base_dir)

    assert safe_path == os.path.realpath(absolute_in_bounds)


def test_parent_traversal_is_rejected(tmp_path):
    # WP-32488: `..` traversal escaping the allowed root must be rejected (CWE-73).
    base_dir = tmp_path / 'allowed'
    base_dir.mkdir()

    assert resolve_safe_error_file_path('../evil.json', str(base_dir)) is None
    assert resolve_safe_error_file_path('../../etc/passwd', str(base_dir)) is None


def test_absolute_escape_path_is_rejected(tmp_path):
    # An absolute path outside the allowed root must be rejected.
    base_dir = str(tmp_path / 'allowed')
    os.makedirs(base_dir)

    assert resolve_safe_error_file_path('/etc/passwd', base_dir) is None
    assert resolve_safe_error_file_path('/tmp/attacker/error.json', base_dir) is None


def test_missing_path_returns_none(tmp_path):
    assert resolve_safe_error_file_path(None, str(tmp_path)) is None
    assert resolve_safe_error_file_path('', str(tmp_path)) is None


def test_rejected_path_writes_no_file_outside_root(tmp_path):
    # WP-32488: a rejected out-of-bounds path must not produce any file outside the root.
    base_dir = tmp_path / 'allowed'
    base_dir.mkdir()
    outside_target = tmp_path / 'evil.json'

    safe_path = resolve_safe_error_file_path('../evil.json', str(base_dir))
    assert safe_path is None
    # Caller skips the write when None is returned, so nothing lands outside the root.
    assert not outside_target.exists()
