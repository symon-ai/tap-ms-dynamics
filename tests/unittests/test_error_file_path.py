import os

from tap_dynamics import sanitize_error_file_path


def test_error_file_path_inside_working_directory_is_allowed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert sanitize_error_file_path('error.json') == os.path.join(
        os.path.realpath(tmp_path), 'error.json')


def test_error_file_path_outside_working_directory_is_rejected(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert sanitize_error_file_path('../error.json') is None
    assert sanitize_error_file_path('/etc/passwd') is None


def test_error_file_path_through_symlink_is_rejected(tmp_path, monkeypatch):
    allowed_dir = tmp_path / 'allowed'
    outside_dir = tmp_path / 'outside'
    allowed_dir.mkdir()
    outside_dir.mkdir()
    (allowed_dir / 'link').symlink_to(outside_dir, target_is_directory=True)
    monkeypatch.chdir(allowed_dir)

    assert sanitize_error_file_path('link/error.json') is None
