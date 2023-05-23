from mingo import Database
from typing import Union
import pytest
from pathlib import Path
from sqlalchemy import Table, MetaData, select
from mock_data import make_mock_source, make_mock_database
import time

MOCK_SOURCE = """HEADER
    CASE
        Particle []: gamma (0), electron (1), muon (2), neutron (3), proton (4)
        Number of events []:
        Emin [MeV]:
        Emax [MeV]:
        E distribution: constant (0), gaussian (1), exponential (2)
        Theta min [deg]:
        Theta max [deg]:
        Detector plane - X dimension [mm]:
        Detector plane - Y dimension [mm]:
        Detector plane - Z dimension [mm]:
    ACTIVE PLANES
        Plane 1 - Z coordinate [mm]: 0 by definition
        Plane 2 - Z coordinate [mm]:
        Plane 3 - Z coordinate [mm]:
        Plane 4 - Z coordinate [mm]:
    PASSIVE PLANES
        Plane 1 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 2 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 3 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 4 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 1 - Thickness [mm]:
        Plane 2 - Thickness [mm]:
        Plane 3 - Thickness [mm]:
        Plane 4 - Thickness [mm]:
        Plane 1 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 2 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 3 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 4 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
   EVENT
        Event number []:
        Initial energy [MeV]:
        Initial X [mm]: Measured from center of plane, positive to the right
        Initial Y [mm]: Measured from center of plane, right handed frame
        Initial Z [mm]: 0 by definition
        Initial theta [deg]:
        Initial phi [deg]:
        Number of hits []:
    HIT
        Plane number []: First is 1, last is 4
        X [mm]: Measured from center of plane, positive to the right
        Y [mm]: Measured from center of plane, right handed frame
        Z [mm]: Measured downwards from first active plane
        Time since first impact [ns]:
DATA
1	10000	800	800	0	0	0	999	999	22
0	100	200	400
null	22	null	222	null	10.4	null	16.2	null	0	null	0
1	800 	0.00000	0.00000	0.00000	0	0	24
1	0.00000	0.00000	0.00000	0
2	-0.71720	-9.39800	100.00000	0.3692
3	-14.78000	-26.62000	200.00000	0.7113
4	-122.10000	46.55000	420.20000	1.589
3	47.82000	-10.86000	216.90000	0.901
4	-100.70000	-186.50000	419.50000	1.767
4	-44.30000	-322.90000	400.00000	2.005
2	-9.68600	-2.48300	100.00000	0.3695
3	-32.41000	32.50000	200.00000	0.7339
3	0.00046	22.88000	200.50000	0.8806
2	-0.97830	-7.40000	100.00000	0.3683
3	-9.57700	-24.90000	200.00000	0.7084
3	-9.66400	-25.09000	201.10000	0.712
2	-1.49600	0.55360	100.00000	0.367
3	0.88750	6.63500	200.00000	0.7014
3	-4.35600	1.73800	221.10000	0.7711
3	-4.35600	1.73800	221.10000	0.7711
3	-4.35600	1.73800	221.10000	0.7711
4	102.80000	-154.90000	400.00000	1.65
2	206.70000	-33.64000	112.30000	1.626
3	135.20000	-11.92000	209.30000	1.218
3	0.00259	2.35100	206.40000	0.7218
2	68.31000	-21.56000	100.00000	0.4779
2	23.83000	3.95000	100.00000	0.3809
"""

EXPECTED_DETECTOR = (1, 999, 999, 22)
EXPECTED_PLANE = [
    (1, 1, 1, 0, None, None, None),
    (2, 1, 2, 100, 22, "Pb", 10.4),
    (3, 1, 3, 200, None, None, None),
    (4, 1, 4, 400, 222, "Pb", 16.2)
]
EXPECTED_EVENT = (1, 1, "electron", 800, 0, 0, 24)
EXPECTED_HIT = (24, 1, 2, 2, 23.83, 3.95, 100, 0.3809)


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
    assert table_list == ["detector", "plane", "event", "hit"]

    # All tables are instances of Table
    assert isinstance(db.detector, Table)
    assert isinstance(db.plane, Table)
    assert isinstance(db.event, Table)
    assert isinstance(db.hit, Table)

    # Each table has the expected number of columns
    assert len(db.detector.c.keys()) == 4
    assert len(db.plane.c.keys()) == 7
    assert len(db.event.c.keys()) == 7
    assert len(db.hit.c.keys()) == 8

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
    assert table_list == ["detector", "plane", "event", "hit"]

    # All tables are instances of Table
    assert isinstance(db.detector, Table)
    assert isinstance(db.plane, Table)
    assert isinstance(db.event, Table)
    assert isinstance(db.hit, Table)

    # Each table has the expected number of columns
    assert len(db.detector.c.keys()) == 4
    assert len(db.plane.c.keys()) == 7
    assert len(db.event.c.keys()) == 7
    assert len(db.hit.c.keys()) == 8

    return None


@pytest.mark.fixt_data(MOCK_SOURCE)
def test_fill_database(make_mock_source, make_mock_database) -> None:
    """
    Ensure that the database is properly filled and data is not corrupted
    """

    source = make_mock_source
    db = make_mock_database
    db.fill(source)

    with db.engine.connect() as conn:

        # Check data in detector table
        detector_data, = conn.execute(select(db.detector))
        assert detector_data == EXPECTED_DETECTOR

        # Check data in plane table
        planes_data = conn.execute(select(db.plane))
        for row, expected_row in zip(planes_data, EXPECTED_PLANE):
            assert row == expected_row

        # Check data in event table
        event_data, = conn.execute(select(db.event))
        assert event_data == EXPECTED_EVENT

        # Check data in hit table
        hit_data = list(conn.execute(select(db.hit)))
        assert len(hit_data) == EXPECTED_EVENT[-1]
        assert hit_data[-1] == EXPECTED_HIT

    return None
