from platform import system
from tempfile import gettempdir
from pathlib import Path
import pytest
from mingo import Database


def get_tmp() -> str:
    """
    Get path to temporary (tmp) directory in operating system
    """
    if system() == "Darwin":
        return "/tmp"
    else:
        return gettempdir()


@pytest.fixture()
def make_mock_source(request):
    """
    Create a temporary source file for testing.
    The content of the file is passed using the @pytest.mark.fixt_data
    decorator in the test function.
    """
    marker = request.node.get_closest_marker("fixt_data")
    if marker is None:
        raise ValueError("Missing content for mock source file")
    else:
        content = marker.args[0]
    tmp = get_tmp()
    source_file = Path(tmp, "mock_source.txt")
    source_file.write_text(content)
    yield source_file
    source_file.unlink()


@pytest.fixture()
def make_mock_database(monkeypatch):
    """
    Create temporary database for testing
    """
    monkeypatch.setattr("builtins.input", lambda _: "y")
    mock_db = Database("mock_database", True)
    yield mock_db
    mock_db.drop()
