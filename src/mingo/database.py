from typing import Union
from pathlib import Path
from sqlalchemy import (
    URL, create_engine, MetaData, Table, Column, ForeignKey, Integer, Double,
    Enum, insert, select, func
)
import sqlalchemy_utils as sqlutils
from .errors import FormatError

DETECTOR_COLS = ["id", "plane_size_x", "plane_size_y", "plane_size_z"]
PLANE_COLS = ["id", "fk_detector", "num", "z", "abs_z", "abs_mat", "abs_thick"]
EVENT_COLS = ["id", "fk_detector", "particle", "e_0", "theta", "phi", "n_hits"]
HIT_COLS = ["id", "fk_event", "fk_plane", "plane", "x", "y", "z", "t"]
HEADER_LINES = 45

MATERIAL = {"0": "Pb", "1": "Fe", "2": "W", "3": "Polyethylene", "null": None}
MATERIAL_ENUM = Enum("Pb", "Fe", "W", "Polyethylene")
PARTICLE = {
    "0": "gamma", "1": "electron", "2": "muon", "3": "neutron", "4": "proton"
}
PARTICLE_ENUM = Enum("gamma", "electron", "muon", "neutron", "proton")


class Database:
    """
    Interface for a database with simulation results.

    Allows to access, create, fill from source file and delete a database with
    an internal schema suitable for Mingo's simulations results.

    :param use_cnf: Whether to use a default configuration file to access
    the database
    :param cnf_path: Absolute path to mariadb configuration file
    :param username: Mariadb username
    :param password: Password for mariadb user
    :param host: Name of the host
    :param port: Port number
    :param database: Name of the database
    :return None:
    """

    def __init__(
        self,
        database: Union[str, None],
        use_cnf: bool = True,
        cnf_path: str = "~/.my.cnf",
        drivername: str = "mariadb+mysqldb",
        username: Union[str, None] = None,
        password: Union[str, None] = None,
        host: Union[str, None] = None,
        port: Union[int, None] = None,
    ) -> None:

        # Create database engine
        if use_cnf:
            url = URL.create(
                drivername=drivername,
                database=database,
                query={"read_default_file": cnf_path}
            )
        else:
            url = URL.create(
                drivername=drivername,
                username=username,
                password=password,
                host=host,
                port=port,
                database=database
            )
        self.engine = create_engine(url, echo=False)

        # Don't attempt to create database if database is None
        if self.engine.url.database is None:
            print("No database selected")
            return None

        # Load or create database
        if sqlutils.database_exists(url):
            self.meta = MetaData()
            self.detector = self.autoload("detector", DETECTOR_COLS)
            self.plane = self.autoload("plane", PLANE_COLS)
            self.event = self.autoload("event", EVENT_COLS)
            self.hit = self.autoload("hit", HIT_COLS)
        else:
            do: str = input(
                f"Database '{url.database}' not found. "
                "Do you want to create it? (y/n): "
            )
            if do == "y":
                print(f"Creating database {url.database}")
                self.create()
            else:
                print("End execution")
                exit(0)

        return None

    def autoload(self, table: str, expected_cols: list[str]) -> Table:
        """
        Load table from existing database and check its columns

        :param table: Name of the table
        :param expected_cols: List of expected columns
        :return Table: SQLAlchemy's core table object
        """

        output = Table(table, self.meta, autoload_with=self.engine)

        if [c.name for c in output.columns] == expected_cols:
            return output
        else:
            raise FormatError(
                f"Unexpected columns in table '{table}' of "
                f"'{self.engine.url.database}' database"
            )

    def create(self) -> None:
        """
        Create empty database and table objects
        """

        sqlutils.create_database(self.engine.url)
        assert sqlutils.database_exists(self.engine.url)

        self.meta = MetaData()

        self.detector = Table(
            "detector",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("plane_size_x", Double, nullable=False),
            Column("plane_size_y", Double, nullable=False),
            Column("plane_size_z", Double, nullable=False),
        )

        self.plane = Table(
            "plane",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("fk_detector", Integer, ForeignKey("detector.id")),
            Column("num", Integer, nullable=False),
            Column("z", Double, nullable=False),
            Column("abs_z", Double, nullable=True),
            Column("abs_mat", MATERIAL_ENUM, nullable=True),
            Column("abs_thick", Double, nullable=True)
        )

        self.event = Table(
            "event",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("fk_detector", Integer, ForeignKey("detector.id")),
            Column("particle", PARTICLE_ENUM, nullable=False),
            Column("e_0", Double, nullable=False),
            Column("theta", Double, nullable=False),
            Column("phi", Double, nullable=False),
            Column("n_hits", Integer, nullable=False)
        )

        self.hit = Table(
            "hit",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("fk_event", Integer, ForeignKey("event.id")),
            Column("fk_plane", Integer, ForeignKey("plane.id")),
            Column("plane", Integer, nullable=False),
            Column("x", Double, nullable=True),
            Column("y", Double, nullable=True),
            Column("z", Double, nullable=True),
            Column("t", Double, nullable=True)
        )

        print(
            "Creating tables: detector, plane, event and hit "
            f"in {self.engine.url.database}"
        )
        self.meta.create_all(self.engine, checkfirst=True)

        return None

    def drop(self) -> None:
        """
        Drop database
        """

        if sqlutils.database_exists(self.engine.url):
            print(f"Dropping database: {self.engine.url.database}")
            sqlutils.drop_database(self.engine.url)
        else:
            raise FileNotFoundError(
                f"Database '{self.engine.url.database}' not found"
            )

        return None

    def fill(self, source_file: Path) -> None:
        """
        Fill database from source file

        :param source: Absolute or relative path to source file
        :return None:
        """
        if not sqlutils.database_exists(self.engine.url):
            raise FileNotFoundError(
                f"Database '{self.engine.url.database}' not found"
            )

        print(
            f"Filling '{self.engine.url.database}' database with "
            f"data from {str(source_file)}."
        )

        with open(source_file, "r") as source:

            # Jump to data section
            for _ in range(HEADER_LINES):
                source.readline()
            if not source.readline() == "DATA\n":
                raise FormatError(
                    f"Unexpected header in source file '{source_file.name}'"
                )

            # Insert detector data
            case_data = source.readline()[:-1].split("\t")
            if case_data[0] in PARTICLE.keys():
                particle = PARTICLE[case_data[0]]
            else:
                raise KeyError(
                    f"Unexpected particle id {case_data[0]}"
                )

            detector_values = {
                "plane_size_x": float(case_data[7]),
                "plane_size_y": float(case_data[8]),
                "plane_size_z": float(case_data[9])
            }

            with self.engine.connect() as conn:
                detector_id, = conn.scalars(
                    insert(self.detector).returning(self.detector.c.id),
                    detector_values
                )
                conn.commit()

            # Insert plane data
            plane_z = source.readline()[:-1].split("\t")
            passive_plane_data = source.readline()[:-1].split("\t")
            plane_values: list[dict[str, Union[None, str, float, int]]] = []

            for idx, (z, a_z, a_t, a_m) in enumerate(zip(
                plane_z,
                passive_plane_data[:4],
                passive_plane_data[4:8],
                passive_plane_data[8:]
            )):
                plane_values.append({"fk_detector": detector_id})
                plane_values[idx]["num"] = idx + 1
                plane_values[idx]["z"] = float(z)
                if a_z == "null":
                    plane_values[idx]["abs_z"] = None
                else:
                    plane_values[idx]["abs_z"] = float(a_z)
                if a_t == "null":
                    plane_values[idx]["abs_thick"] = None
                else:
                    plane_values[idx]["abs_thick"] = float(a_t)
                if a_m in MATERIAL.keys():
                    plane_values[idx]["abs_mat"] = MATERIAL[a_m]
                else:
                    raise KeyError(f"Unexpected material id {a_m}")

            with self.engine.connect() as conn:
                p1_id, p2_id, p3_id, p4_id = conn.scalars(
                    insert(self.plane).returning(self.plane.c.id),
                    plane_values
                )
                conn.commit()

                # Get id for the last event in database
                _event_id = conn.scalar(select(func.max(self.event.c.id)))
                event_id = _event_id if _event_id is not None else 0
                plane_id = {"1": p1_id, "2": p2_id, "3": p3_id, "4": p4_id}

            # Insert events and values
            event_list: list[dict[str, Union[int, float, str, None]]] = []
            hit_list: list[dict[str, Union[int, float, None]]] = []
            for idx, line in enumerate(source.readlines()):
                data = line[:-1].split("\t")
                match len(data):
                    case 8:
                        event_list.append({
                            "fk_detector": detector_id,
                            "particle": particle,
                            "e_0": float(data[1]),
                            "theta": float(data[5]),
                            "phi": float(data[6]),
                            "n_hits": float(data[7])
                        })
                        event_id += 1
                    case 5:
                        hit_list.append({
                            "fk_event": event_id,
                            "fk_plane": plane_id[data[0]],
                            "plane": int(data[0]),
                            "x": float(data[1]),
                            "y": float(data[2]),
                            "z": float(data[3]),
                            "t": float(data[4])
                        })
                    case _:
                        raise FormatError(
                            "Unexpected format in line "
                            f"{idx + 2 + HEADER_LINES} of {source_file}"
                        )

            with self.engine.connect() as conn:
                conn.execute(insert(self.event), event_list)
                conn.execute(insert(self.hit), hit_list)
                conn.commit()

        return None
