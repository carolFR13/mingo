import pytest
from tempfile import gettempdir
from platform import system
from pathlib import Path
from mingo.tests.mock_data import MOCK_SOURCE_DATA
from mingo import *
import pandas as pd


@pytest.fixture(scope="module")
def make_sources():

    tmp = Path("/tmp") if system() == "Darwin" else Path(gettempdir())

    sources: list[Path] = []

    for key, data in MOCK_SOURCE_DATA.items():
        sources.append(tmp / f"{key}.txt")
        sources[-1].write_text(data)

    yield sources

    for source in sources:
        source.unlink()


@pytest.fixture(scope="module")
def make_database(make_sources: list[Path]):

    db = Database("mock_database", ask_to_create=False)
    sources = make_sources

    for source in sources:
        db.fill(source)

    yield db

    db.drop()


@pytest.mark.parametrize(
    "params", [
        (1, 999, 999, 22, 0, "0", 0),
        (2, 999, 999, 22, 22, "Pb", 10.4),
        (3, 999, 999, 22, 222, "Pb", 16.2),
        (4, 999, 999, 22, 22, "Pb", 16.2),
        (5, 999, 999, 22, 222, "Pb", 10.4),
        (None, 0, 0, 0, 0, "0", 0)
    ]
)
def test_get_plane_id(make_database: Database, params) -> None:
    """Get plane ID from configuration data"""

    db = make_database

    id, size_x, size_y, size_z, abs_z, abs_mat, abs_thick = params

    if id is None:
        with pytest.raises(ValueError):
            assert db.get_plane_id(
                size_x, size_y, size_z, abs_z, abs_mat, abs_thick) == id
    else:
        assert db.get_plane_id(
            size_x, size_y, size_z, abs_z, abs_mat, abs_thick) == id

    return None


@pytest.mark.parametrize(
    "params", [
        (1, (1, 2, 1, 3), (0, 100, 200, 400)),
        (2, (1, 4, 1, 5), (0, 100, 200, 400)),
        (None, (1, 2, 1, 1), (0, 100, 200, 400)),
        (None, (1, 2, 1, 3), (0, 100, 200, 300))
    ]
)
def test_get_config_id(make_database: Database, params) -> None:
    """Get config id from configuration data"""

    db = make_database

    id, id_list, z_list = params

    if id is None:
        with pytest.raises(ValueError):
            assert db.get_config_id(id_list, z_list) == id
    else:
        assert db.get_config_id(id_list, z_list) == id

    return None


def test_hit_distribution(make_database: Database) -> None:

    db = make_database

    hits = Hit_distribution(db)
    hits(1, "1016")
    hits(2, "1610")

    d10 = hits.dist_data["1016"]
    d16 = hits.dist_data["1610"]
    s10 = hits.stats_data["1016"].round(2)
    s16 = hits.stats_data["1610"].round(2)

    # Check distribution data
    assert list(d10.keys()) == [800, 1000]
    assert list(d16.keys()) == [800, 1000]
    assert d10[800].tolist() == [[13, 24, 38, 50], [1, 1, 1, 1]]
    assert d10[1000].tolist() == [[32, 42, 53, 70], [1, 1, 1, 1]]
    assert d16[800].tolist() == [[24, 32, 45, 47], [1, 1, 1, 1]]
    assert d16[1000].tolist() == [[27, 31, 38, 63], [1, 1, 1, 1]]

    # Check stats data
    assert isinstance(s10, pd.DataFrame)
    assert isinstance(s16, pd.DataFrame)
    assert s10["e_0"].tolist() == [800, 1000]
    assert s16["e_0"].tolist() == [800, 1000]
    assert s10["avg"].tolist() == [31.25, 49.25]
    assert s16["avg"].tolist() == [37, 39.75]
    assert s10["median"].tolist() == [31, 47.5]
    assert s16["median"].tolist() == [38.5, 34.5]
    assert s10["std"].tolist() == [13.99, 14.10]
    assert s16["std"].tolist() == [9.46, 13.99]
    assert s10["skewness"].tolist() == [0.04, 0.31]
    assert s16["skewness"].tolist() == [-0.24, 0.90]
    assert s10["kurtosis"].tolist() == [1.56, 1.75]
    assert s16["kurtosis"].tolist() == [1.35, 2.12]

    return None


def test_shower_depth(make_database: Database) -> None:

    db = make_database

    depth = Shower_depth(db)
    depth(1, "1016")
    depth(2, "1610")

    d10 = depth.dist_data["1016"]
    d16 = depth.dist_data["1610"]
    s10 = depth.stats_data["1016"].round(2)
    s16 = depth.stats_data["1610"].round(2)

    # Check distribution data
    assert list(d10.keys()) == [800, 1000]
    assert list(d16.keys()) == [800, 1000]
    assert d10[800].tolist() == [[200, 205, 215, 235], [1, 1, 1, 1]]
    assert d10[1000].tolist() == [[175, 210, 250, 265], [1, 1, 1, 1]]
    assert d16[800].tolist() == [[145, 180, 205, 225], [1, 1, 1, 1]]
    assert d16[1000].tolist() == [[170, 195, 200, 255], [1, 1, 1, 1]]

    # Check stats data
    assert isinstance(s10, pd.DataFrame)
    assert isinstance(s16, pd.DataFrame)
    assert s10["e_0"].tolist() == [800, 1000]
    assert s16["e_0"].tolist() == [800, 1000]
    assert s10["avg"].tolist() == [214.41, 225.77]
    assert s16["avg"].tolist() == [188.38, 205.07]
    assert s10["median"].tolist() == [209.24, 231.21]
    assert s16["median"].tolist() == [191.27, 198.41]
    assert s10["std"].tolist() == [13.64, 36.06]
    assert s16["std"].tolist() == [29.00, 30.43]
    assert s10["skewness"].tolist() == [0.9, -0.32]
    assert s16["skewness"].tolist() == [-0.24, 0.62]
    assert s10["kurtosis"].tolist() == [2.12, 1.58]
    assert s16["kurtosis"].tolist() == [1.68, 2.08]

    return None


def test_plane_hits(make_database: Database) -> None:

    db = make_database

    hits = Plane_hits(db, 3)
    hits(1, "1016")
    hits(2, "1610")

    d10 = hits.dist_data["1016"]
    d16 = hits.dist_data["1610"]
    s10 = hits.stats_data["1016"].round(2)
    s16 = hits.stats_data["1610"].round(2)

    # Check distribution data
    assert list(d10.keys()) == [800, 1000]
    assert list(d16.keys()) == [800, 1000]
    assert d10[800].tolist() == [[3, 10, 12, 26], [1, 1, 1, 1]]
    assert d10[1000].tolist() == [[8, 12, 22, 31], [1, 1, 1, 1]]
    assert d16[800].tolist() == [[9, 16, 17], [2, 1, 1]]
    assert d16[1000].tolist() == [[8, 16, 23], [2, 1, 1]]

    # Check stats data
    assert isinstance(s10, pd.DataFrame)
    assert isinstance(s16, pd.DataFrame)
    assert s10["e_0"].tolist() == [800, 1000]
    assert s16["e_0"].tolist() == [800, 1000]
    assert s10["avg"].tolist() == [12.75, 18.25]
    assert s16["avg"].tolist() == [12.75, 13.75]
    assert s10["median"].tolist() == [11, 17]
    assert s16["median"].tolist() == [12.5, 12]
    assert s10["std"].tolist() == [8.35, 8.95]
    assert s16["std"].tolist() == [3.77, 6.26]
    assert s10["skewness"].tolist() == [0.59, 0.28]
    assert s16["skewness"].tolist() == [0.03, 0.43]
    assert s10["kurtosis"].tolist() == [2.05, 1.52]
    assert s16["kurtosis"].tolist() == [1.04, 1.55]

    return None


def test_scattering(make_database: Database) -> None:

    db = make_database

    R = Scattering(db, 3)
    R(1, "1016")
    R(2, "1610")

    d10 = R.dist_data["1016"]
    d16 = R.dist_data["1610"]
    s10 = R.stats_data["1016"].round(2)
    s16 = R.stats_data["1610"].round(2)

    # Check distribution data
    assert list(d10.keys()) == [800, 1000]
    assert list(d16.keys()) == [800, 1000]
    assert d10[800].tolist() == [[30, 53, 89, 102], [1, 1, 1, 1]]
    assert d10[1000].tolist() == [[51, 70, 97, 142], [1, 1, 1, 1]]
    assert d16[800].tolist() == [[34, 70, 118, 128], [1, 1, 1, 1]]
    assert d16[1000].tolist() == [[25, 75, 99, 137], [1, 1, 1, 1]]

    # Check stats data
    assert isinstance(s10, pd.DataFrame)
    assert isinstance(s16, pd.DataFrame)
    assert s10["e_0"].tolist() == [800, 1000]
    assert s16["e_0"].tolist() == [800, 1000]
    assert s10["avg"].tolist() == [68.49, 89.96]
    assert s16["avg"].tolist() == [87.67, 83.80]
    assert s10["median"].tolist() == [71.05, 83.38]
    assert s16["median"].tolist() == [94.22, 86.99]
    assert s10["std"].tolist() == [28.44, 34.33]
    assert s16["std"].tolist() == [37.74, 40.61]
    assert s10["skewness"].tolist() == [-0.16, 0.46]
    assert s16["skewness"].tolist() == [-0.29, -0.21]
    assert s10["kurtosis"].tolist() == [1.39, 1.79]
    assert s16["kurtosis"].tolist() == [1.43, 1.84]

    return None
