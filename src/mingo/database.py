from typing import Iterable, Sequence, Mapping
from pathlib import Path
from sqlalchemy import (
    URL, create_engine, MetaData, Table, Column, Integer, Float,
    select, func, UniqueConstraint, ForeignKeyConstraint
)
from sqlalchemy.dialects.postgresql import ENUM, insert
from sqlalchemy.exc import OperationalError
import sqlalchemy_utils as sqlutils
from dataclasses import dataclass, asdict
from .errors import DatabaseCreationError, DataFileFormatError

# Expected length of the header section of source files
HEADER_LINES = 45

# Valid extensions for data-files and directories
VALID_EXTENSIONS = (".txt", ".csv", "")

# Valid values for absorption plane materials and primary particles
MATERIAL = ("Pb", "Fe", "W", "Polyethylene")
PARTICLE = ("gamma", "electron", "muon", "neutron", "proton")


@dataclass(frozen=True)
class DBInput:
    """Custom type for input of Database class

    :param database: Name of the database
    :param drivername: Dialect and driver to be used by SQLAlchemy
    :param username: User name
    :param password: Password
    :param host: Host name
    :param port: Port number
    """

    database: str
    drivername: str = "postgresql+psycopg2"
    username: str | None = None
    password: str | None = None
    host: str | None = None
    port: int | None = None


class Database:
    """
    Python interface for a database with simulation results.

    Allows to access, create, fill from source file and delete a database with
    an internal schema suitable for Mingo's simulations results.

    :param data: User and host information to access database
    :param ask_to_create: Whether to require for permission to create database
    """

    @dataclass(frozen=True)
    class ConfigInput:
        """Custom type for input of config tables

        :param fk_p1: ID of plane 1
        :param fk_p2: ID of plane 2
        :param fk_p3: ID of plane 3
        :param fk_p4: ID of plane 4
        :param z_p1: Z coordinate of plane 1
        :param z_p2: Z coordinate of plane 2
        :param z_p3: Z coordinate of plane 3
        :param z_p4: Z coordinate of plane 4
        """
        fk_p1: int
        fk_p2: int
        fk_p3: int
        fk_p4: int
        z_p1: float
        z_p2: float
        z_p3: float
        z_p4: float

    @dataclass(frozen=True)
    class PlaneInput:
        """Custom type for input of plane tables

        :param size_x: X dimension of the active plane
        :param size_y: Y dimension of the active plane
        :param size_z: Z dimension of the active plane
        :param abs_z: Z coordinate of the absorption plane
        :param abs_mat: Material of the absorption plane
        :param abs_thick: Thickness of the absorption plane
        """
        size_x: float
        size_y: float
        size_z: float
        abs_z: float | None
        abs_mat: str | None
        abs_thick: float | None

    @dataclass(frozen=True)
    class EventInput:
        """Custom type for input of event tables

        :param fk_config: ID of detector configuration
        :param particle: Type of particle
        :param e_0: Energy of the primary cosmic ray
        :param theta: Theta angle (?)
        :param phi: Phi angle (?)
        :param n_hits: Number of hits produced by the primary cosmic ray
        """
        fk_config: int
        particle: str
        e_0: float
        theta: float
        phi: float
        n_hits: int

    @dataclass(frozen=True)
    class HitInput:
        """Custom type for input of hit tables

        :param fk_event: ID of the event that produced the hit
        :param plane: Plane where the hit was detected
        :param x: X coordinate of the hit
        :param y: Y coordinate of the hit
        :param z: Z coordinate of the plane where the hit was detected
        :param t: Instant of detection 
        """
        fk_event: int
        plane: int
        x: float
        y: float
        z: float
        t: float

    def __init__(self, data: DBInput, ask_to_create: bool = True) -> None:

        # Create database engine
        _url = URL.create(data.drivername, data.username,
                          data.password, data.host, data.port, data.database)
        self.engine = create_engine(_url, echo=False)

        # Load or create database
        if sqlutils.database_exists(self.engine.url):
            self._make_meta(create=False)
        else:
            _input_str = (f"Database {self.engine.url.database}"
                          " not found. Do you want to create it? (y/n)")

            if ask_to_create and input(_input_str) != "y":
                print("End of execution")
                exit(0)
            else:
                print(f"Creating database: {self.engine.url.database}")
                self._make_meta()

        return None

    def _make_meta(self, create: bool = True) -> None:
        """Create MetaData and Table objects for database

        :param create: Whether to create a new database
        :return None:
        """

        # Create database if requested and ensure it exists
        if create:
            try:
                sqlutils.create_database(self.engine.url)
            except OperationalError:
                raise DatabaseCreationError(
                    "Failed to create database.\nThis might be caused by "
                    "incorrect username, password, host, drivers..."
                ) from None
        assert sqlutils.database_exists(self.engine.url)

        # Define enumerate types for particles and materials
        _particle_enum = ENUM("gamma", "electron", "muon",
                              "neutron", "proton", name="particle_enum")
        _material_enum = ENUM("Pb", "Fe", "W",
                              "Polyethylene", "0", name="material_enum")

        self.meta = MetaData()

        self.config = Table(
            "config",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("fk_p1", Integer, nullable=False),
            Column("fk_p2", Integer, nullable=False),
            Column("fk_p3", Integer, nullable=False),
            Column("fk_p4", Integer, nullable=False),
            Column("z_p1", Float, nullable=False),
            Column("z_p2", Float, nullable=False),
            Column("z_p3", Float, nullable=False),
            Column("z_p4", Float, nullable=False),
            ForeignKeyConstraint(["fk_p1"], ["plane.id"], ondelete="CASCADE"),
            ForeignKeyConstraint(["fk_p2"], ["plane.id"], ondelete="CASCADE"),
            ForeignKeyConstraint(["fk_p3"], ["plane.id"], ondelete="CASCADE"),
            ForeignKeyConstraint(["fk_p4"], ["plane.id"], ondelete="CASCADE"),
            UniqueConstraint("fk_p1", "fk_p2", "fk_p3", "fk_p4",
                             name="config_uniqueness",
                             postgresql_nulls_not_distinct=True)
        )

        self.plane = Table(
            "plane",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("size_x", Float, nullable=False),
            Column("size_y", Float, nullable=False),
            Column("size_z", Float, nullable=False),
            Column("abs_z", Float, nullable=True),              # 0 = NULL
            Column("abs_mat", _material_enum, nullable=True),   # "0" = NULL
            Column("abs_thick", Float, nullable=True),          # 0 = NULL
            UniqueConstraint("size_x", "size_y", "size_z", "abs_z",
                             "abs_mat", "abs_thick", name="plane_uniqueness",
                             postgresql_nulls_not_distinct=True)
        )

        self.event = Table(
            "event",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("fk_config", Integer),
            Column("particle", _particle_enum, nullable=False),
            Column("e_0", Float, nullable=False),
            Column("theta", Float, nullable=False),
            Column("phi", Float, nullable=False),
            Column("n_hits", Integer, nullable=False),
            ForeignKeyConstraint(
                ["fk_config"], ["config.id"], ondelete="CASCADE")
        )

        self.hit = Table(
            "hit",
            self.meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("fk_event", Integer),
            Column("plane", Integer, nullable=False),
            Column("x", Float, nullable=True),
            Column("y", Float, nullable=True),
            Column("z", Float, nullable=True),
            Column("t", Float, nullable=True),
            ForeignKeyConstraint(
                ["fk_event"], ["event.id"], ondelete="CASCADE")
        )

        if create:
            self.meta.create_all(self.engine, checkfirst=True)

        return None

    def _fill_input_handler(
            self, sources: str | Path | Iterable[str | Path]) -> list[Path]:
        """Turn any valid input for fill into a list of paths to data files"""

        def __add_source(source: Path, source_list: list[Path]) -> list[Path]:
            """Get paths to data-files from input and add them to list"""

            # Coarse filter for undesired files
            if source.suffix not in VALID_EXTENSIONS:
                return source_list

            if source.is_file():
                # Check format and add to list
                with source.open() as file:
                    if file.readline() == "HEADER\n":
                        source_list.append(source)
            elif source.is_dir():
                # Iterate over items in dir and add them to list
                for _source in source.iterdir():
                    __add_source(_source, source_list)
            else:
                raise ValueError(f"Invalid input: {source}")

            return source_list

        _sources: list[Path] = []
        if isinstance(sources, str):
            _sources = __add_source(Path(sources), _sources)
        elif isinstance(sources, Path):
            _sources = __add_source(sources, _sources)
        elif isinstance(sources, Iterable):
            for source in sources:
                if isinstance(source, str):
                    _sources = __add_source(Path(source), _sources)
                elif isinstance(source, Path):
                    _sources = __add_source(source, _sources)
                else:
                    raise TypeError(f"Unexpected type: {source}")
        else:
            raise TypeError(f"Unexpected type: {sources}")

        return _sources

    def _fill(self, source_file: Path) -> None:
        """Read data from data-file and insert it into the database

        :param source: Path to source file
        """

        # Ensure that database exists before proceeding
        if not sqlutils.database_exists(self.engine.url):
            raise FileNotFoundError(
                f"Database '{self.engine.url.database}' not found"
            )

        with open(source_file, "r") as source:

            # Jump to data section
            for _ in range(HEADER_LINES):
                source.readline()
            if not source.readline() == "DATA\n":
                raise DataFileFormatError(
                    f"Unexpected header in source file '{source_file.name}'"
                )

            # Read case and plane data from source file
            config_data = source.readline()[:-1].split("\t")
            active_plane_data = source.readline()[:-1].split("\t")
            passive_plane_data = source.readline()[:-1].split("\t")

            # Get particle type
            if config_data[0] == "null":
                particle = None
            else:
                particle = PARTICLE[int(config_data[0])]

            # Get plane dimensions
            size_x: float = float(config_data[7])
            size_y: float = float(config_data[8])
            size_z: float = float(config_data[9])

            # Insert planes
            plane_values: list[dict[str, str | float | None]] = []

            for _a_z, _a_t, _a_m in zip(passive_plane_data[:4],
                                        passive_plane_data[4:8],
                                        passive_plane_data[8:]):

                a_z = None if _a_z == "null" else float(_a_z)
                a_t = None if _a_t == "null" else float(_a_t)
                a_m = None if _a_m == "null" else MATERIAL[int(_a_m)]

                plane_values.append({"size_x": size_x,
                                     "size_y": size_y,
                                     "size_z": size_z,
                                     "abs_z": a_z,
                                     "abs_mat": a_m,
                                     "abs_thick": a_t})

            fk_p_list = self._insert_plane(plane_values)

            # Insert config
            config_values = {"fk_p1": fk_p_list[0],
                             "fk_p2": fk_p_list[1],
                             "fk_p3": fk_p_list[2],
                             "fk_p4": fk_p_list[3],
                             "z_p1": float(active_plane_data[0]),
                             "z_p2": float(active_plane_data[1]),
                             "z_p3": float(active_plane_data[2]),
                             "z_p4": float(active_plane_data[3])}

            config_id = self._insert_config(config_values)

            with self.engine.connect() as conn:
                # Get the id of the last event in the database
                _event_id, = conn.scalars(select(func.max(self.event.c.id)))
                event_id = _event_id if _event_id is not None else 0

            # Insert events and values
            event_list: list[Mapping[str, int | float | str | None]] = []
            hit_list: list[Mapping[str, int | float | None]] = []
            for idx, line in enumerate(source.readlines()):
                data = line[: -1].split("\t")
                size = len(data)
                if size == 8:
                    event_list.append({
                        "fk_config": config_id,
                        "particle": particle,
                        "e_0": float(data[1]),
                        "theta": float(data[5]),
                        "phi": float(data[6]),
                        "n_hits": float(data[7])
                    })
                    event_id += 1
                elif size == 5:
                    hit_list.append({
                        "fk_event": event_id,
                        "plane": int(data[0]),
                        "x": float(data[1]),
                        "y": float(data[2]),
                        "z": float(data[3]),
                        "t": float(data[4])
                    })
                else:
                    raise DataFileFormatError(
                        "Unexpected format in line "
                        f"{idx + 2 + HEADER_LINES} of {source_file}"
                    )

            with self.engine.connect() as conn:
                conn.execute(insert(self.event), event_list)
                conn.execute(insert(self.hit), hit_list)
                conn.commit()

        return None

    def batch_fill(self, sources: Path | Iterable[Path]) -> None:
        """DEPRECATED: Use fill instead"""
        self.fill(sources)

        return None

    def fill(self, sources: str | Path | Iterable[str | Path]) -> None:
        """Fill database using data-files (API)

        :param sources: Data files to be used to fill the database or path
        to the directory where they are contained
        """

        # Take given input and turn it into a list of paths to data-files
        source_list = self._fill_input_handler(sources)

        for source in source_list:
            print(
                f"Filling {self.engine.url.database} with {source.parent.name}"
                f"/{source.name}"
            )
            self._fill(source)

        return None

    def drop(self) -> None:
        """Drop database"""

        if sqlutils.database_exists(self.engine.url):
            print(f"Dropping database: {self.engine.url.database}")
            sqlutils.drop_database(self.engine.url)
        else:
            raise FileNotFoundError(
                f"Database '{self.engine.url.database}' not found"
            )

        return None

    def _insert_plane(self,
                      data: list[dict[str, float | str | None]]) -> list[int]:
        """Insert row into plane table"""

        fk_p_list: list[int] = []

        with self.engine.connect() as conn:

            for row in data:

                # Return id if insert succeeds, None otherwise
                _id = conn.scalar(insert(self.plane)
                                  .values(row)
                                  .on_conflict_do_nothing()
                                  .returning(self.plane.c.id))

                if isinstance(_id, int):
                    id = _id
                elif _id is None:
                    tmp_id = conn.scalars(
                        select(self.plane.c.id)
                        .where(
                            self.plane.c.size_x == row["size_x"],
                            self.plane.c.size_y == row["size_y"],
                            self.plane.c.size_z == row["size_z"],
                            self.plane.c.abs_z == row["abs_z"],
                            self.plane.c.abs_mat == row["abs_mat"],
                            self.plane.c.abs_thick == row["abs_thick"]
                        )
                    ).fetchall()
                    id, = tmp_id
                else:       # Unexpected result
                    raise TypeError("Unexpected type for plane id. "
                                    f"Expected int or None, got {type(_id)}")

                fk_p_list.append(id)

                conn.commit()

        return fk_p_list

    def _insert_config(self, data: dict[str, int | float]) -> int:
        """Insert row into config table"""

        with self.engine.connect() as conn:

            _id = conn.scalar(insert(self.config)
                              .values(data)
                              .on_conflict_do_nothing()
                              .returning(self.config.c.id))

            if isinstance(_id, int):
                config_id = _id
            elif _id is None:
                config_id, = conn.scalars(
                    select(self.config.c.id)
                    .where(
                        self.config.c.fk_p1 == data["fk_p1"],
                        self.config.c.fk_p2 == data["fk_p2"],
                        self.config.c.fk_p3 == data["fk_p3"],
                        self.config.c.fk_p4 == data["fk_p4"]
                    )
                )
            else:       # Unexpected result
                raise TypeError("Unexpected type for config id"
                                f". Expected int or None, got {type(_id)}")

            conn.commit()

        return config_id

    def insert_plane(self, values: PlaneInput | Sequence[PlaneInput]):
        """Manually insert data into plane table

        :param values: Values to insert
        :return: Inserted data
        """

        # Turn input into list of dicts
        if isinstance(values, self.PlaneInput):
            values = [values]
        _values = [asdict(value) for value in values]

        # Insert planes and get their ids
        ids = self._insert_plane(_values)

        # Get all the data from the inserted planes
        with self.engine.connect() as conn:
            return [conn.execute(select(self.plane)
                                 .where(self.plane.c.id == id)).fetchone()
                    for id in ids]

    def insert_config(self, values: ConfigInput | Sequence[ConfigInput]):
        """Manually insert data into config table

        :param values: Values to insert
        :return: Inserted data
        """

        # Turn input into list of dicts
        if isinstance(values, self.ConfigInput):
            values = [values]
        _values = [asdict(value) for value in values]

        # Insert configurations and get their ids
        ids = [self._insert_config(value) for value in _values]

        # Get all the data from the inserted configurations
        with self.engine.connect() as conn:
            return [conn.execute(select(self.config)
                                 .where(self.config.c.id == id)).fetchone()
                    for id in ids]

    def get_plane_id(self, plane: PlaneInput) -> int:
        """Get the id of the plane matching a given configuration

        Raises ValueError if given configuration is not found

        :param plane: Configuration of the desired plane
        :return: ID of plane matching given configuration
        """

        err_msg = "not enough values to unpack (expected 1, got 0)"

        with self.engine.connect() as conn:
            try:
                id, = conn.scalars(
                    select(self.plane.c.id).where(
                        self.plane.c.size_x == plane.size_x,
                        self.plane.c.size_y == plane.size_y,
                        self.plane.c.size_z == plane.size_z,
                        self.plane.c.abs_z == plane.abs_z,
                        self.plane.c.abs_mat == plane.abs_mat,
                        self.plane.c.abs_thick == plane.abs_thick
                    )
                )
                if not isinstance(id, int):
                    raise TypeError("Unexpected type for plane id")
            except ValueError as err:
                if err.args[0] == err_msg:
                    raise ValueError("Plane configuration not found")
                else:
                    raise err

        return id

    def get_config_id(self, config: ConfigInput) -> int:
        """
        Get the id of the detector matching a given configuration

        Raises ValueError if given configuration is not found

        :param config: Configuration of the desired detector
        :return: ID of detector matching given configuration
        """

        err_msg = "not enough values to unpack (expected 1, got 0)"

        with self.engine.connect() as conn:
            try:
                id, = conn.scalars(
                    select(self.config.c.id).where(
                        self.config.c.fk_p1 == config.fk_p1,
                        self.config.c.fk_p2 == config.fk_p2,
                        self.config.c.fk_p3 == config.fk_p3,
                        self.config.c.fk_p4 == config.fk_p4,
                        self.config.c.z_p1 == config.z_p1,
                        self.config.c.z_p2 == config.z_p2,
                        self.config.c.z_p3 == config.z_p3,
                        self.config.c.z_p4 == config.z_p4
                    )
                )
                if not isinstance(id, int):
                    raise TypeError("Unexpected type for config id")
            except ValueError as err:
                if err.args[0] == err_msg:
                    raise ValueError("Detector configuration not found")
                else:
                    raise err

        return id
