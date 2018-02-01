from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.movie.movie import Movie
import arrow
from mock import patch, MagicMock
from qmdb.database.database import Database
import pytest
import sqlite3
import os
from shutil import copyfile


def test_database_init_existing_file():
    db = Database('test/fixtures/testdb.sqlite')
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == [123]


def test_database_init_bad_file():
    with pytest.raises(sqlite3.OperationalError):
        db = Database('test/fixtures/testdb_bad.sqlite')


def test_database_init_from_scratch_new():
    filename = 'test/fixtures/testdb_new.sqlite'
    db = Database(filename)
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == []
    os.remove(filename)


def test_database_init_from_scratch_existing():
    filename = 'test/fixtures/testdb_existing.sqlite'
    db = Database(filename)
    assert list(db.movies.keys()) == [123]
    db = Database(filename, from_scratch=True)
    assert list(db.movies.keys()) == []
    os.remove(filename)
    copyfile('test/fixtures/testdb.sqlite', filename)
