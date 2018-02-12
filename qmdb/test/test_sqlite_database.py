import os
import sqlite3
from shutil import copyfile

import pytest

from qmdb.database.database import SQLiteDatabase, MovieNotInDatabaseError
from qmdb.movie.movie import Movie


def test_database_init_existing_file():
    db = SQLiteDatabase('test/fixtures/testdb.sqlite')
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == [1234]


def test_database_init_bad_file():
    with pytest.raises(sqlite3.OperationalError):
        db = SQLiteDatabase('test/fixtures/testdb_bad.sqlite')


def test_database_init_from_scratch_new():
    filename = 'test/fixtures/testdb_new.sqlite'
    db = SQLiteDatabase(filename)
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == []
    os.remove(filename)


def test_database_init_from_scratch_existing():
    filename = 'test/fixtures/testdb_existing.sqlite'
    copyfile('test/fixtures/testdb.sqlite', filename)
    db = SQLiteDatabase(filename)
    assert list(db.movies.keys()) == [1234]
    db = SQLiteDatabase(filename, from_scratch=True)
    assert list(db.movies.keys()) == []
    os.remove(filename)


def test_add_new_movie():
    filename = 'test/fixtures/testdb_add_movie.sqlite'
    copyfile('test/fixtures/testdb.sqlite', filename)
    db = SQLiteDatabase(filename)
    new_movie = {'crit_id': 12345,
                 'title': 'Pulp Fiction'}
    db.set_movie(Movie(new_movie))
    db = SQLiteDatabase(filename)
    assert 12345 in list(db.movies.keys())
    assert db.movies[12345].title == 'Pulp Fiction'
    os.remove(filename)


def test_update_existing_movie():
    filename = 'test/fixtures/testdb_add_movie.sqlite'
    copyfile('test/fixtures/testdb.sqlite', filename)
    db = SQLiteDatabase(filename)
    existing_movie = {'crit_id': 1234,
                      'title': 'The Matrix 2'}
    db.set_movie(Movie(existing_movie))
    assert db.movies[1234].year == 1999
    db = SQLiteDatabase(filename)
    assert 1234 in list(db.movies.keys())
    assert db.movies[1234].title == 'The Matrix 2'
    assert db.movies[1234].year == 1999
    os.remove(filename)


def test_insert_new_movie_do_not_update():
    filename = 'test/fixtures/testdb_add_movie.sqlite'
    copyfile('test/fixtures/testdb.sqlite', filename)
    db = SQLiteDatabase(filename)
    existing_movie = {'crit_id': 1234,
                      'title': 'The Matrix 2'}
    db.set_movie(Movie(existing_movie), overwrite=False)
    db = SQLiteDatabase(filename)
    assert 1234 in list(db.movies.keys())
    assert db.movies[1234].title == 'The Matrix'
    assert db.movies[1234].year == 1999
    os.remove(filename)


def test_get_existing_movie():
    filename = 'test/fixtures/testdb.sqlite'
    db = SQLiteDatabase(filename)
    movie = db.get_movie(1234)
    assert movie.title == 'The Matrix'
    assert movie.year == 1999


def test_get_non_existent_movie():
    filename = 'test/fixtures/testdb.sqlite'
    db = SQLiteDatabase(filename)
    with pytest.raises(MovieNotInDatabaseError) as e_info:
        movie = db.get_movie(12345)