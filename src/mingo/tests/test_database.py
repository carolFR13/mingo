from mingo import Database
from typing import Union
import pytest
from pathlib import Path


@pytest.mark.parametrize(
    ["use_cnf", "username", "password", "host", "port", "database"],
    [
        (True, None, None, None, None, "test_database"),
    ]
)
def test_create_engine(
        monkeypatch,
        use_cnf: bool,
        username: Union[str, None],
        password: Union[str, None],
        host: Union[str, None],
        port: Union[int, None],
        database: Union[str, None]
) -> None:

    db = Database(
        use_cnf=use_cnf,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database
    )

    assert db.engine.url.database == database

    return None


if __name__ == "__main__":

    db = Database(True, database="mock_database")
    db.fill(Path(
        "/Users/alfonso/home/mingo/data/10_16/simple_electrons_at_800MeV.txt"
    ))
#     db.drop()
