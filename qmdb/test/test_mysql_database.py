from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.movie.movie import Movie
import arrow
from mock import patch, MagicMock
from qmdb.database.database import MySQLDatabase, MovieNotInDatabaseError
import pytest
import sqlite3
import os
from shutil import copyfile
import pymysql


def create_copy_of_table(src, tgt, schema='qmdb_test'):
    db = MySQLDatabase(schema=schema)
    db.remove_table(tgt)
    db.connect()
    db.c.execute("create table {} as select * from {}".format(tgt, src))
    db.close()


def test_database_init_existing_file():
    db = MySQLDatabase(schema='qmdb_test')
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == [1234]


def test_database_init_from_scratch_new():
    # TODO: make this test more useful
    db = MySQLDatabase(schema='qmdb_test', movies_table='new_movies')
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == []
    db.remove_table('new_movies')


def test_database_init_from_scratch_existing():
    create_copy_of_table('movies', 'movies_copy')
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_copy')
    assert list(db.movies.keys()) == [1234]
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_copy', from_scratch=True)
    assert list(db.movies.keys()) == []
    db.remove_table('movies_copy')


def test_add_new_movie():
    create_copy_of_table('movies', 'movies_copy')
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_copy')
    new_movie = {'crit_id': 12345,
                 'crit_popularity_page': 1,
                 'crit_url': 'blahblah',
                 'title': 'Pulp Fiction',
                 'date_added': arrow.now()}
    db.set_movie(Movie(new_movie))
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_copy')
    assert 12345 in list(db.movies.keys())
    assert db.movies[12345].title == 'Pulp Fiction'
    db.remove_table('movies_copy')


def test_update_existing_movie():
    create_copy_of_table('movies', 'movies_copy')
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_copy')
    existing_movie = {'crit_id': 1234,
                      'title': 'The Matrix 2'}
    db.set_movie(Movie(existing_movie))
    assert db.movies[1234].year == 1999
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_copy')
    assert 1234 in list(db.movies.keys())
    assert db.movies[1234].title == 'The Matrix 2'
    assert db.movies[1234].year == 1999
    db.remove_table('movies_copy')


def test_insert_new_movie_do_not_update():
    create_copy_of_table('movies', 'movies_copy')
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_copy')
    existing_movie = {'crit_id': 1234,
                      'title': 'The Matrix 2'}
    db.set_movie(Movie(existing_movie), overwrite=False)
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_copy')
    assert 1234 in list(db.movies.keys())
    assert db.movies[1234].title == 'The Matrix'
    assert db.movies[1234].year == 1999
    db.remove_table('movies_copy')


def test_get_existing_movie():
    db = MySQLDatabase(schema='qmdb_test')
    movie = db.get_movie(1234)
    assert movie.title == 'The Matrix'
    assert movie.year == 1999


def test_get_non_existent_movie():
    db = MySQLDatabase(schema='qmdb_test')
    with pytest.raises(MovieNotInDatabaseError) as e_info:
        movie = db.get_movie(12345)