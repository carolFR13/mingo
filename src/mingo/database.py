from typing import Union, Iterable, Sequence
from pathlib import Path
from sqlalchemy import (
    URL, create_engine, MetaData, Table, Column, ForeignKey, Integer, Double,
    Enum, select, func, UniqueConstraint, text
)
from sqlalchemy.dialects.mysql import insert
import sqlalchemy_utils as sqlutils
from .errors import FormatError
from tqdm import tqdm as bar


# Expected extension of the header section of source files
HEADER_LINES = 45

# Valid values for absorption plane materials and primary particles
MATERIAL = ("Pb", "Fe", "W", "Polyethylene")
MATERIAL_ENUM = Enum("Pb", "Fe", "W", "Polyethylene", "0")
PARTICLE = ("gamma", "electron", "muon", "neutron", "proton")
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
    :param ask_to_create: Whether to ask for permission to create database
    if it is not found
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
        ask_to_create: bool = True
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
            self.make_meta(create=False)
        else:
            if ask_to_create:
                do: str = input(
                    f"Database '{url.database}' not found. "
                    "Do you want to create it? (y/n): "
                )
            else:
                do = "y"
            if do == "y":
                print(f"Creating database {url.database}")
                self.make_meta()
            else:
                print("End execution")
                exit(0)

        return None

    def make_meta(self, create: bool = True) -> None:
        """
        Create MetaData and Table objects for database and, if desired,
        create database with mingo schema

        :param create: Whether to create a new database with mingo schema
        """

        if create:
            sqlutils.create_database(self.engine.url)
        assert sqlutils.database_exists(self.engine.url)

        self.meta = MetaData()

        self.config = Table(
            "config",
            self.meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("fk_p1", Integer, ForeignKey("plane.id"), nullable=False),
            Column("fk_p2", Integer, ForeignKey("plane.id"), nullable=False),
            Column("fk_p3", Integer, ForeignKey("plane.id"), nullable=False),
            Column("fk_p4", Integer, ForeignKey("plane.id"), nullable=False),
            Column("z_p1", Double, nullable=False),
            Column("z_p2", Double, nullable=False),
            Column("z_p3", Double, nullable=False),
            Column("z_p4", Double, nullable=False),
            UniqueConstraint("fk_p1", "fk_p2", "fk_p3", "fk_p4")
        )

        self.plane = Table(
            "plane",
            self.meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("size_x", Double, nullable=False),
            Column("size_y", Double, nullable=False),
            Column("size_z", Double, nullable=False),
            Column("abs_z", Double, nullable=True),             # 0 = NULL
            Column("abs_mat", MATERIAL_ENUM, nullable=False),   # "0" = NULL
            Column("abs_thick", Double, nullable=False),        # 0 = NULL
            UniqueConstraint(
                "size_x", "size_y", "size_z", "abs_z", "abs_mat", "abs_thick"
            )
        )

        self.event = Table(
            "event",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("fk_config", Integer, ForeignKey("config.id")),
            Column("particle", PARTICLE_ENUM, nullable=False),
            Column("e_0", Double, nullable=False),
            Column("theta", Double, nullable=False),
            Column("phi", Double, nullable=False),
            Column("n_hits", Integer, nullable=False)
        )

        self.hit = Table(
            "hit",
            self.meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("fk_event", Integer, ForeignKey("event.id")),
            Column("plane", Integer, nullable=False),
            Column("x", Double, nullable=True),
            Column("y", Double, nullable=True),
            Column("z", Double, nullable=True),
            Column("t", Double, nullable=True)
        )

        if create:
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

    def batch_fill(self, sources: Union[Path, Iterable[Path]]) -> None:
        """
        Fill database from a set of source files.

        :param sources: Path to directory with sources or list of paths
        to source files. Directories are assumed to just contain sources.
        """

        if isinstance(sources, Path) and sources.is_dir():
            _sources = [source for source in sources.iterdir()]
        elif isinstance(sources, Iterable):
            _sources = list(sources)
        else:
            raise TypeError(
                f"Unexpected input type: {type(sources)}. "
                f"Must be Path or Iterable[Path]."
            )
        print(f"Filling {self.engine.url.database} with source files:")
        for source in _sources:
            if not source.is_file():
                raise ValueError(f"{str(source)} is not a file!")
            else:
                self.fill(source)

        return None

    def fill(self, source_file: Union[str, Path]) -> None:
        """
        Fill database from source file

        NOTE: Since mariadb treats each NULL value as unique, the absence
        of an absorption plane is represented by zero values in
        the 'abs_z', 'abs_mat' and 'abs_thick' columns of the plane table.
        Zero is an unfeasible value for these three parameters for the
        following reasons:
            - 'abs_thick' must be greater than 0 for any real plane.
            - 'abs_mat' is the name of a material, which cannot be 0.
            - 'abs_z' must be greater than 'abs_thick'.

        :param source: Absolute or relative path to source file
        :return None:
        """
        # Cast input to Path if needed
        if isinstance(source_file, str):
            source_file = Path(source_file)

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
                raise FormatError(
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

            # Get plane dimensions and absorption plane properties
            plane_values: list[dict[str, Union[None, str, float, int]]] = []
            plane_dimensions = {
                "x": float(config_data[7]),
                "y": float(config_data[8]),
                "z": float(config_data[9])
            }
            for idx, (a_z, a_t, a_m) in enumerate(zip(
                passive_plane_data[:4],
                passive_plane_data[4:8],
                passive_plane_data[8:]
            )):
                plane_values.append({
                    "size_x": plane_dimensions["x"],
                    "size_y": plane_dimensions["y"],
                    "size_z": plane_dimensions["z"],
                    "abs_z": 0 if a_z == "null" else float(a_z),
                    "abs_mat": "0" if a_m == "null" else MATERIAL[int(a_m)],
                    "abs_thick": 0 if a_t == "null" else float(a_t)
                })

            fk_p_list = self.insert_plane(plane_values, return_all=False)

            # Get z coordinates and configuration ids of detection modules
            config_values = {
                "fk_p1": fk_p_list[0],
                "fk_p2": fk_p_list[1],
                "fk_p3": fk_p_list[2],
                "fk_p4": fk_p_list[3],
                "z_p1": float(active_plane_data[0]),
                "z_p2": float(active_plane_data[1]),
                "z_p3": float(active_plane_data[2]),
                "z_p4": float(active_plane_data[3])
            }

            config_id, = self.insert_config(config_values, return_all=False)
            if not isinstance(config_id, int):
                raise TypeError("ID has unexpected type")

            with self.engine.connect() as conn:

                # Get the id of the last event in the database
                _event_id, = conn.scalars(select(func.max(self.event.c.id)))
                event_id = _event_id if _event_id is not None else 0

            # Insert events and values
            event_list: list[dict[str, Union[int, float, str, None]]] = []
            hit_list: list[dict[str, Union[int, float, None]]] = []
            for idx, line in enumerate(
                bar(
                    source.readlines(),
                    desc=source_file.parent.name + "/" + source_file.name,
                    ascii=True
                )
            ):
                data = line[:-1].split("\t")
                match len(data):
                    case 8:
                        event_list.append({
                            "fk_config": config_id,
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

    def insert_config(self, values, return_all: bool = True):
        """
        Insert data into config table and handle id count when uniqueness
        constraints are violated

        :param values: Values to insert
        :param return_all: Whether to return all the columns of the inserted
        rows (True) or just their ids (False)
        :return: List of inserted rows or list of IDs of inserted rows
        """

        stmt = insert(self.config).values(values)
        stmt = stmt.on_duplicate_key_update(z_p1=stmt.inserted.z_p1)
        stmt = stmt.returning(self.config)

        n_values = 1 if isinstance(values, dict) else len(values)

        with self.engine.connect() as conn:

            _id, = conn.scalars(select(func.max(self.config.c.id)))
            id = _id if _id is not None else 0

            if return_all:
                result = conn.execute(stmt).fetchall()
                id_count = max([row[0] for row in result])
            else:
                result = [row[0] for row in conn.execute(stmt)]
                id_count = max(result)

            if id_count != id + n_values:
                conn.execute(
                    text("alter table config auto_increment = :x"),
                    {"x": id_count}
                )
            conn.commit()

        return result

    def insert_plane(self, values, return_all: bool = True):
        """
        Insert data into plane table and handle id count when uniqueness
        constraints are violated

        :param values: Values to insert
        :param return_all: Whether to return all the columns of the inserted
        rows (True) or just their ids (False)
        :return: List of inserted rows or list of IDs of inserted rows
        """

        stmt = insert(self.plane).values(values)
        stmt = stmt.on_duplicate_key_update(size_x=stmt.inserted.size_x)
        stmt = stmt.returning(self.plane)

        n_values = 1 if isinstance(values, dict) else len(values)

        with self.engine.connect() as conn:

            _id, = conn.scalars(select(func.max(self.plane.c.id)))
            id = _id if _id is not None else 0

            if return_all:
                result = conn.execute(stmt).fetchall()
                id_count = max([row[0] for row in result])
            else:
                result = [row[0] for row in conn.execute(stmt)]
                id_count = max(result)

            if id_count != id + n_values:
                conn.execute(
                    text("alter table plane auto_increment = :x;"),
                    {"x": id_count}
                )
            conn.commit()

        return result

    def get_plane_id(self, size_x: float, size_y: float, size_z: float,
                     abs_z: float, abs_mat: str, abs_thick: float) -> int:
        """
        Get the id of the plane matching a given configuration

        Raises ValueError if given configuration is not found

        :param size_x: X dimension of active plane [mm]
        :param size_y: Y dimension of active plane [mm]
        :param size_z: Z dimension of active plane [mm]
        :param abs_z: Z coordinate of absorption plane (0 if null) [mm]
        :param abs_mat: Material of absorption plane ('0' if null) [ ]
        :param abs_thick: Thickness of absorption plane (0 if null) [mm]
        :return: ID of plane matching given configuration
        """

        err_msg = "not enough values to unpack (expected 1, got 0)"

        with self.engine.connect() as conn:

            try:
                id, = conn.scalars(
                    select(self.plane.c.id).where(
                        self.plane.c.size_x == size_x,
                        self.plane.c.size_y == size_y,
                        self.plane.c.size_z == size_z,
                        self.plane.c.abs_z == abs_z,
                        self.plane.c.abs_mat == abs_mat,
                        self.plane.c.abs_thick == abs_thick
                    )
                )
                if isinstance(id, int):
                    return id
                else:
                    raise TypeError("Unexpected type for plane id")
            except ValueError as err:
                if err.args[0] == err_msg:
                    raise ValueError("Plane configuration not found")
                else:
                    raise err

    def get_config_id(self, ids: Sequence[int], zs: Sequence[float]) -> int:
        """
        Get the id of the detector matching a given configuration

        Raises ValueError if given configuration is not found

        :param ids: The ids of the four detection modules
        :param zs: The Z coordinates of the active planes
        :return: ID of detector matching given configuration
        """

        err_msg = "not enough values to unpack (expected 1, got 0)"

        with self.engine.connect() as conn:

            try:
                id, = conn.scalars(
                    select(self.config.c.id).where(
                        self.config.c.fk_p1 == ids[0],
                        self.config.c.fk_p2 == ids[1],
                        self.config.c.fk_p3 == ids[2],
                        self.config.c.fk_p4 == ids[3],
                        self.config.c.z_p1 == zs[0],
                        self.config.c.z_p2 == zs[1],
                        self.config.c.z_p3 == zs[2],
                        self.config.c.z_p4 == zs[3]
                    )
                )
                if isinstance(id, int):
                    return id
                else:
                    raise TypeError("Unexpected type for config id")

            except ValueError as err:
                if err.args[0] == err_msg:
                    raise ValueError("Detector configuration not found")
                else:
                    raise err
