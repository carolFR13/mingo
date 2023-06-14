from mingo import Database
from typing import Union
import pytest
from sqlalchemy import Table, MetaData, select
from mingo.tests.mock_data import (
    MOCK_SOURCE_DATA,
    make_sources, make_mock_mingo
)

EXPECTED_CONFIG = (1, 1, 2, 1, 3, 0, 100, 200, 400)
EXPECTED_PLANE = [
    (1, 999, 999, 22, 0, "0", 0),
    (2, 999, 999, 22, 22, "Pb", 10.4),
    (3, 999, 999, 22, 222, "Pb", 16.2)
]
EXPECTED_EVENT = (1, 1, "electron", 800, 0, 0, 24)
EXPECTED_HIT = (24, 1, 2, 23.83, 3.95, 100, 0.3809)


@pytest.mark.parametrize(
    [
        "use_cnf", "cnf_path", "drivername", "username", "password", "host",
        "port", "database"
    ],
    [
        (True, "~/.my.cnf", "mariadb+mysqldb", None, None, None, None, None),
        (True, "not-found", "mariadb+mysqldb", None, None, None, None, None)
    ]
)
def test_create_engine(
        use_cnf: bool,
        cnf_path: str,
        drivername: str,
        username: Union[str, None],
        password: Union[str, None],
        host: Union[str, None],
        port: Union[int, None],
        database: Union[str, None]
) -> None:

    # TODO: Handle errors due to missing driver or cnf file
    # TODO: Handle errors due to incorrect username, password, port or host

    return None


def test_create_database(make_mock_database: Database) -> None:
    """
    Ensure that an unexisting database is properly created
    """

    db = make_mock_database

    # MetaData object exists and all tables are present
    assert isinstance(db.meta, MetaData)
    table_list = [table for table in db.meta.tables.keys()]
    assert table_list == ["config", "plane", "event", "hit"]

    # All tables are instances of Table
    assert isinstance(db.config, Table)
    assert isinstance(db.plane, Table)
    assert isinstance(db.event, Table)
    assert isinstance(db.hit, Table)

    # Each table has the expected number of columns
    assert len(db.config.c.keys()) == 9
    assert len(db.plane.c.keys()) == 7
    assert len(db.event.c.keys()) == 7
    assert len(db.hit.c.keys()) == 7

    return None


def test_load_database(make_mock_database: Database) -> None:
    """
    Ensure that an existing database is properly loaded
    """

    mock_db = make_mock_database

    db = Database(mock_db.engine.url.database, True)

    # MetaData object exists and all tables are present
    assert isinstance(db.meta, MetaData)
    table_list = [table for table in db.meta.tables.keys()]
    assert table_list == ["config", "plane", "event", "hit"]

    # All tables are instances of Table
    assert isinstance(db.config, Table)
    assert isinstance(db.plane, Table)
    assert isinstance(db.event, Table)
    assert isinstance(db.hit, Table)

    # Each table has the expected number of columns
    assert len(db.config.c.keys()) == 9
    assert len(db.plane.c.keys()) == 7
    assert len(db.event.c.keys()) == 7
    assert len(db.hit.c.keys()) == 7

    return None


def test_fill_database(make_mock_database: Database) -> None:
    """
    Ensure that the database is properly filled and data is not corrupted
    """

    _source = make_sources("10-16-800.txt", MOCK_SOURCE_DATA["10-16-800"])
    source = next(_source)

    db = make_mock_database
    db.fill(source)

    with db.engine.connect() as conn:

        # Check data in config table
        config_data, = conn.execute(select(db.config))
        assert config_data == EXPECTED_CONFIG
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
        planes_data = conn.execute(select(db.plane))

        for idx, result in enumerate(planes_data):
            assert EXPECTED_PLANE[idx] == result
            assert isinstance(result[0], int)
            assert isinstance(result[1], float)
            assert isinstance(result[2], float)
            assert isinstance(result[3], float)
            assert isinstance(result[4], float)
            assert isinstance(result[5], str)
            assert isinstance(result[6], float)

        # Check data in event table
        event_data, = conn.execute(select(db.event).where(db.event.c.id == 1))
        assert event_data == EXPECTED_EVENT
        assert isinstance(event_data[0], int)
        assert isinstance(event_data[1], int)
        assert isinstance(event_data[2], str)
        assert isinstance(event_data[3], float)
        assert isinstance(event_data[4], float)
        assert isinstance(event_data[5], float)
        assert isinstance(event_data[6], int)

        # Check data in hit table
        hit_data = list(
            conn.execute(select(db.hit).where(db.hit.c.fk_event == 1))
        )
        assert len(hit_data) == EXPECTED_EVENT[-1]
        assert hit_data[-1] == EXPECTED_HIT
        assert isinstance(hit_data[-1][0], int)
        assert isinstance(hit_data[-1][1], int)
        assert isinstance(hit_data[-1][2], int)
        assert isinstance(hit_data[-1][3], float)
        assert isinstance(hit_data[-1][4], float)
        assert isinstance(hit_data[-1][5], float)
        assert isinstance(hit_data[-1][6], float)

    return None


def test_fill_existing_database(make_mock_database: Database) -> None:
    """
    Ensure that a filled database is properly loaded
    """
    _source = make_sources("mock_source.txt", MOCK_SOURCE_DATA["10-16-800"])
    source = next(_source)

    mock_db = make_mock_database
    mock_db.fill(source)
    db = Database(mock_db.engine.url.database)

    with db.engine.connect() as conn:

        # Check data in config table
        config_data, = conn.execute(select(db.config))
        assert config_data == EXPECTED_CONFIG
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
        planes_data = conn.execute(select(db.plane))

        for idx, result in enumerate(planes_data):
            assert EXPECTED_PLANE[idx] == result
            assert isinstance(result[0], int)
            assert isinstance(result[1], float)
            assert isinstance(result[2], float)
            assert isinstance(result[3], float)
            assert isinstance(result[4], float)
            assert isinstance(result[5], str)
            assert isinstance(result[6], float)

        # Check data in event table
        event_data, = conn.execute(select(db.event).where(db.event.c.id == 1))
        assert event_data == EXPECTED_EVENT
        assert isinstance(event_data[0], int)
        assert isinstance(event_data[1], int)
        assert isinstance(event_data[2], str)
        assert isinstance(event_data[3], float)
        assert isinstance(event_data[4], float)
        assert isinstance(event_data[5], float)
        assert isinstance(event_data[6], int)

        # Check data in hit table
        hit_data = list(
            conn.execute(select(db.hit).where(db.hit.c.fk_event == 1))
        )
        assert len(hit_data) == EXPECTED_EVENT[-1]
        assert hit_data[-1] == EXPECTED_HIT
        assert isinstance(hit_data[-1][0], int)
        assert isinstance(hit_data[-1][1], int)
        assert isinstance(hit_data[-1][2], int)
        assert isinstance(hit_data[-1][3], float)
        assert isinstance(hit_data[-1][4], float)
        assert isinstance(hit_data[-1][5], float)
        assert isinstance(hit_data[-1][6], float)

    return None


def test_plane_uniqueness(make_mock_database: Database) -> None:

    db = make_mock_database

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

    return None


def test_config_uniqueness(make_mock_database: Database) -> None:

    db = make_mock_database

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

    return None


def test_mingo():

    dbgen = make_mock_mingo()
    db = next(dbgen)

    id = db.get_plane_id(999, 999, 22, 0, "0", 0)

    assert id == 1
