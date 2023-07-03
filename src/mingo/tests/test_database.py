import pytest
from pathlib import Path
from platform import system
from tempfile import gettempdir
from mingo.tests.mock_data import MOCK_SOURCE_DATA
from mingo import Database
from sqlalchemy import MetaData, select


# ################################## NOTE ################################## #
#                                                                            #
#  These tests require the existance of a configuration file for MariaDB     #
#  The absolute path to this file is stored in CONFIG_FILE, which defaults   #
#  to '~/.my.cnf'. Update this variable with your own config file!           #
#                                                                            #
# ########################################################################## #


CONFIG_FILE = "~/.my.cnf"
EXPECTED_TABLES = ["config", "plane", "event", "hit"]
EXPECTED_COLUMNS = {
    "config": ["id", "fk_p1", "fk_p2",
               "fk_p3", "fk_p4", "z_p1", "z_p2", "z_p3", "z_p4"],
    "plane": ["id", "size_x",
              "size_y", "size_z", "abs_z", "abs_mat", "abs_thick"],
    "event": ["id", "fk_config", "particle", "e_0", "theta", "phi", "n_hits"],
    "hit": ["id", "fk_event", "plane", "x", "y", "z", "t"]
}
EXPECTED_CONFIG = [
    (1, 1, 2, 1, 3, 0, 100, 200, 400),
    (2, 1, 4, 1, 5, 0, 100, 200, 400)
]
EXPECTED_PLANE = [
    (999, 999, 22, 0, "0", 0),
    (999, 999, 22, 22, "Pb", 10.4),
    (999, 999, 22, 222, "Pb", 10.4),
    (999, 999, 22, 22, "Pb", 16.2),
    (999, 999, 22, 222, "Pb", 16.2)
]
EXPECTED_EVENT = [
    ("electron", 800, 0, 0, 24),
    ("electron", 1000, 0, 0, 63)
]
EXPECTED_HIT = [
    (2, 23.83, 3.95, 100, 0.3809),
    (2, -77.38, -26, 100.5, 0.583)
]


@pytest.fixture(scope="module")
def make_sources():

    tmp = Path("/tmp") if system() == "Darwin" else Path(gettempdir())

    key_list = list(MOCK_SOURCE_DATA.keys())

    sources: list[Path] = [
        tmp / f"{key_list[0]}.txt", tmp / f"{key_list[-1]}.txt"
    ]

    sources[0].write_text(MOCK_SOURCE_DATA[key_list[0]])
    sources[-1].write_text(MOCK_SOURCE_DATA[key_list[-1]])

    # sources: list[Path] = [
    #     tmp / f"{list(MOCK_SOURCE_DATA.keys())[0]}.txt",
    #     tmp / f"{list(MOCK_SOURCE_DATA.keys())[-1]}.txt"
    # ]

    # for key, data in MOCK_SOURCE_DATA.items():
    #     sources.append(tmp / f"{key}.txt")
    #     sources[-1].write_text(data)

    yield sources

    for source in sources:
        source.unlink()


@pytest.fixture(scope="module")
def make_database(make_sources: list[Path]):

    db = Database("mock_database", cnf_path=CONFIG_FILE, ask_to_create=False)
    sources = make_sources

    for source in sources:
        db.fill(source)

    yield db

    db.drop()


def check_database_structure(db: Database) -> None:

    # MetaData object exists and expected tables are present
    assert isinstance(db.meta, MetaData)
    assert list(db.meta.tables.keys()) == EXPECTED_TABLES

    # Check for expected columns
    assert list(db.config.c.keys()) == EXPECTED_COLUMNS["config"]
    assert list(db.plane.c.keys()) == EXPECTED_COLUMNS["plane"]
    assert list(db.event.c.keys()) == EXPECTED_COLUMNS["event"]
    assert list(db.hit.c.keys()) == EXPECTED_COLUMNS["hit"]

    return None


def check_database_content(db: Database) -> None:

    with db.engine.connect() as conn:

        # Check data in config table
        _data = conn.execute(select(db.config)).fetchall()
        # Output is independent of input order, no need to re-arrange
        _config_data = [_data[0], _data[-1]]
        for idx, config_data in enumerate(_config_data):
            assert config_data == EXPECTED_CONFIG[idx]
            assert isinstance(config_data[0], int)
            assert isinstance(config_data[1], int)
            assert isinstance(config_data[2], int)
            assert isinstance(config_data[3], int)
            assert isinstance(config_data[4], int)
            assert isinstance(config_data[5], float)
            assert isinstance(config_data[6], float)
            assert isinstance(config_data[7], float)
            assert isinstance(config_data[8], float)

        # Check data in plane table
        planes_data = conn.execute(
            select(db.plane)
            .order_by(db.plane.c.abs_thick)
            .order_by(db.plane.c.abs_z)
        )

        for idx, result in enumerate(planes_data):
            # EXPECTED_PLANE does not include plane id -> result[1:]
            assert EXPECTED_PLANE[idx] == result[1:]
            assert isinstance(result[0], int)
            assert isinstance(result[1], float)
            assert isinstance(result[2], float)
            assert isinstance(result[3], float)
            assert isinstance(result[4], float)
            assert isinstance(result[5], str)
            assert isinstance(result[6], float)

        # Check data in event table
        _data = conn.execute(
            select(db.event).order_by(db.event.c.e_0)).fetchall()
        _event_data = [_data[0], _data[-1]]
        for idx, event_data in enumerate(_event_data):
            assert EXPECTED_EVENT[idx] == event_data[2:]
            assert isinstance(event_data[0], int)
            assert isinstance(event_data[1], int)
            assert isinstance(event_data[2], str)
            assert isinstance(event_data[3], float)
            assert isinstance(event_data[4], float)
            assert isinstance(event_data[5], float)
            assert isinstance(event_data[6], int)

        # Check data in hit table
        _hit_data = [
            conn.execute(
                select(db.hit)
                .where(db.hit.c.fk_event == _event_data[0][0])
            ).fetchall()[-1],
            conn.execute(
                select(db.hit)
                .where(db.hit.c.fk_event == _event_data[-1][0])
            ).fetchall()[-1]
        ]
        for idx, hit_data in enumerate(_hit_data):
            assert hit_data[2:] == EXPECTED_HIT[idx]
            assert isinstance(hit_data[0], int)
            assert isinstance(hit_data[1], int)
            assert isinstance(hit_data[2], int)
            assert isinstance(hit_data[3], float)
            assert isinstance(hit_data[4], float)
            assert isinstance(hit_data[5], float)
            assert isinstance(hit_data[6], float)

    return None


def test_Database_create() -> None:
    """Create an empty database"""

    db = Database("mock_database", cnf_path=CONFIG_FILE, ask_to_create=False)

    try:
        check_database_structure(db)
    finally:
        db.drop()

    return None


def test_Database_load() -> None:
    """Load empty database and check structure"""

    _db = Database("mock_database", cnf_path=CONFIG_FILE, ask_to_create=False)
    db = Database(_db.engine.url.database, cnf_path=CONFIG_FILE)

    try:
        check_database_structure(db)
    finally:
        db.drop()

    return None


@pytest.mark.parametrize(
    "input_format",
    [
        "str-file path-file", "str-dir", "path-dir",
        "[str-file, str-file]", "(str-file, str-file)",
        "[path-file, path-file]", "(path-file, path-file)",
        "[str-file, path-file]", "(str-file, path-file)"
    ]
)
def test_Database_fill(make_sources: list[Path], input_format: str) -> None:
    """
    Create an empty database, fill it with two data-files using different
    input formats and check for expected content. Then load the database 
    into a new Database object and repeat tests to ensure that loading does
    not affect content or data types for any input format.
    """

    sources = make_sources
    source_A = sources[0]
    source_B = sources[-1]

    db = Database("mock_database", ask_to_create=False)

    try:
        match input_format:
            case "str-file path-file":
                db.fill(str(source_A))
                db.fill(source_B)
            case "str-dir":
                db.fill(str(source_A.parent))
            case "path-dir":
                db.fill(source_A.parent)
            case "[str-file, str-file]":
                db.fill([str(source_A), str(source_B)])
            case "(str-file, str-file)":
                db.fill((str(source_A), str(source_B)))
            case "[path-file, path-file]":
                db.fill([source_A, source_B])
            case "(path-file, path-file)":
                db.fill((source_A, source_B))
            case "[str-file, path-file]":
                db.fill([str(source_A), source_B])
            case "(str-file, path-file)":
                db.fill((str(source_A), source_B))
            case _:
                raise ValueError("Unexpected input format")

        # Perform tests on filled database
        check_database_content(db)

        # Repeat tests on loaded database
        # No need to set ask_to_create = False if database is loaded
        loaded_db = Database(db.engine.url.database)
        check_database_content(loaded_db)

    finally:
        db.drop()

    return None


def test_plane_id_uniqueness() -> None:
    """Ensure uniqueness and continuity of plane IDs"""

    db = Database("mock_database", ask_to_create=False)

    try:
        planes = [
            (None, 0, 0, 0, 0, "0", 0),
            (None, 1, 0, 0, 0, "0", 0),
            (None, 2, 0, 0, 0, "0", 0),
            (None, 3, 0, 0, 0, "0", 0),
            (None, 4, 0, 0, 0, "0", 0),
            (None, 5, 0, 0, 0, "0", 0)
        ]

        expected_plane_data = [
            (1, 0, 0, 0, 0, "0", 0),
            (2, 1, 0, 0, 0, "0", 0),
            (3, 2, 0, 0, 0, "0", 0),
            (4, 3, 0, 0, 0, "0", 0),
            (5, 4, 0, 0, 0, "0", 0)
        ]

        # Test single inserts: u(nique) + d(uplicate) + u
        first, = db.insert_plane(planes[0])
        second, = db.insert_plane(planes[0])
        third, = db.insert_plane(planes[1])
        assert first == expected_plane_data[0]
        assert second == first
        assert third == expected_plane_data[1]

        # Test batch insert: [u, d, u] + u
        fourth = db.insert_plane([planes[2], planes[2], planes[3]])
        fifth, = db.insert_plane(planes[4])
        assert fourth[0] == expected_plane_data[2]
        assert fourth[1] == expected_plane_data[2]
        assert fourth[2] == expected_plane_data[3]
        assert fifth == expected_plane_data[4]
    finally:
        db.drop()

    return None


def test_config_id_uniqueness() -> None:
    """Ensure uniqueness and continuity of config IDs"""

    db = Database("mock_database", ask_to_create=False)

    try:
        planes = [(None, 0, 0, 0, 0, "0", 0), (None, 1, 0, 0, 0, "0", 0)]
        db.insert_plane(planes)

        configs = [
            (None, 1, 1, 1, 1, 0, 0, 0, 0),
            (None, 1, 1, 1, 2, 0, 0, 0, 0),
            (None, 1, 1, 2, 1, 0, 0, 0, 0),
            (None, 1, 2, 1, 1, 0, 0, 0, 0),
            (None, 2, 1, 1, 1, 0, 0, 0, 0)
        ]

        expected_result = [
            (1, 1, 1, 1, 1, 0, 0, 0, 0),
            (2, 1, 1, 1, 2, 0, 0, 0, 0),
            (3, 1, 1, 2, 1, 0, 0, 0, 0),
            (4, 1, 2, 1, 1, 0, 0, 0, 0),
            (5, 2, 1, 1, 1, 0, 0, 0, 0)

        ]

        # Test single inserts: u + d + u
        first, = db.insert_config(configs[0])
        first_duplicated, = db.insert_config(configs[0])
        second, = db.insert_config(configs[1])
        assert first == expected_result[0]
        assert first_duplicated == expected_result[0]
        assert second == expected_result[1]

        # Test batch inserts: [u, d, u] + u
        third, third_duplicated, fourth = db.insert_config(
            [configs[2], configs[2], configs[3]]
        )
        fifth, = db.insert_config(configs[4])
        assert third == expected_result[2]
        assert third_duplicated == third
        assert fourth == expected_result[3]
        assert fifth == expected_result[4]
    finally:
        db.drop()

    return None
