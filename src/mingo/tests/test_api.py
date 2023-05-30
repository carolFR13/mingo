from mingo import Database
from mingo.tests.mock_data import make_mock_mingo
from pathlib import Path
import pytest


@pytest.mark.parametrize(
    ["id", "size_x", "size_y", "size_z", "abs_z", "abs_mat", "abs_thick"],
    [
        (1, 999, 999, 22, 0, "0", 0),
        pytest.param(None, 0, 0, 0, 0, "0", 0, marks=pytest.mark.xfail)
    ]
)
def test_get_plane_id(id: int, size_x: float, size_y: float, size_z: float,
                      abs_z: float, abs_mat: str, abs_thick: float) -> None:

    dbgen = make_mock_mingo()
    db = next(dbgen)

    assert db.get_plane_id(size_x, size_y, size_z,
                           abs_z, abs_mat, abs_thick) == id

    return None


@pytest.mark.parametrize(
    ["id", "ids", "zs"],
    [
        (1, (1, 2, 1, 3), (0, 100, 200, 400)),
        pytest.param(None, (1, 1, 1, 1), (0, 0, 0, 0), marks=pytest.mark.xfail)
    ]
)
def test_get_config_id(id: int, ids: tuple[int], zs: tuple[float]) -> None:

    dbgen = make_mock_mingo()
    db = next(dbgen)

    assert db.get_config_id(ids, zs) == id

    return None
