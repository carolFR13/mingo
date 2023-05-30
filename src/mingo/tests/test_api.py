from mingo import Database
from mingo.tests.mock_data import (
    MOCK_SOURCE_DATA, make_sources, make_mock_database
)
from pathlib import Path
import pytest
from mingo import Hit_distribution


@pytest.fixture(scope="module")
def make_mingo():

    db = Database("mock_database", ask_to_create=False)

    names = [key for key in MOCK_SOURCE_DATA.keys()]
    data_list = [MOCK_SOURCE_DATA[key] for key in names]

    sourcegen = make_sources(names, data_list)
    for source in sourcegen:
        db.fill(source)

    yield db
    db.drop()


@pytest.mark.parametrize(
    ["id", "size_x", "size_y", "size_z", "abs_z", "abs_mat", "abs_thick"],
    [
        (1, 999, 999, 22, 0, "0", 0),
        (None, 0, 0, 0, 0, "0", 0)
    ]
)
def test_get_plane_id(
    make_mingo, id, size_x: float, size_y: float, size_z: float, abs_z: float,
    abs_mat: str, abs_thick: float
) -> None:

    db = make_mingo

    if id is None:
        with pytest.raises(ValueError):
            assert db.get_plane_id(
                size_x, size_y, size_z, abs_z, abs_mat, abs_thick) == id
    else:
        assert db.get_plane_id(size_x, size_y, size_z,
                               abs_z, abs_mat, abs_thick) == id


@pytest.mark.parametrize(
    ["id", "ids", "zs"],
    [
        (1, (1, 2, 1, 3), (0, 100, 200, 400)),
        (None, (1, 1, 1, 1), (0, 0, 0, 0))
    ]
)
def test_get_config_id(make_mingo, id, ids, zs) -> None:

    db = make_mingo

    if id is None:
        with pytest.raises(ValueError):
            assert db.get_config_id(ids, zs) == id
    else:
        assert db.get_config_id(ids, zs) == id


def test_hit_distribution(make_mingo) -> None:

    db = make_mingo
    hits = Hit_distribution(db)
    hits.distribution(1, "")
    result = hits.dist_data[""]

    assert list(result.keys()) == [800, 1000]
    assert result[800].tolist() == [[24], [1]]
    assert result[1000].tolist() == [[42], [1]]
    assert result[800].shape == (2, 1)
    assert result[1000].shape == (2, 1)

    return None

# TODO: Add tests for the rest of Hit_distribution's methods
