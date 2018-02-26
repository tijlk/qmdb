import arrow
import pytest

from qmdb.database.database import MySQLDatabase, MovieNotInDatabaseError
from qmdb.movie.movie import Movie
from qmdb.utils.utils import create_copy_of_table
import pymysql
from qmdb.test.test_utils import create_test_tables, remove_test_tables
from qmdb.interfaces.imdb import IMDBScraper


def test_database_init_existing_file():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == [1234, 49141]
    remove_test_tables(db)


def test_database_init_from_scratch_new():
    db = MySQLDatabase(schema='qmdb_test', from_scratch=True)
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == []
    remove_test_tables(db)


def test_database_init_from_scratch_existing():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    assert list(db.movies.keys()) == [1234, 49141]
    db = MySQLDatabase(schema='qmdb_test', from_scratch=True)
    assert list(db.movies.keys()) == []
    remove_test_tables(db)


def test_add_new_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    new_movie = {'crit_id': 12345,
                 'crit_popularity_page': 1,
                 'crit_url': 'blahblah',
                 'title': 'Pulp Fiction',
                 'poster_url': 'http://blahblah',
                 'date_added': arrow.now()}
    db.set_movie(Movie(new_movie))
    db = MySQLDatabase(schema='qmdb_test')
    assert 12345 in list(db.movies.keys())
    assert db.movies[12345].title == 'Pulp Fiction'
    remove_test_tables(db)


def test_add_weird_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    new_movie = {'crit_id': 60326,
                 'crit_popularity_page': 79,
                 'crit_url': 'https://www.criticker.com/film/alg305-cengi/',
                 'title': 'Çalgı çengi',
                 'year': 2011,
                 'date_added': arrow.now()}
    db.set_movie(Movie(new_movie))
    db = MySQLDatabase(schema='qmdb_test')
    assert 60326 in list(db.movies.keys())
    assert db.movies[60326].title == 'Çalgı çengi'
    remove_test_tables(db)


def test_update_existing_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    existing_movie = {'crit_id': 1234,
                      'title': 'The Matrix 2',
                      'genres': ['Action', 'Sci-Fi']}
    movie = db.movies[existing_movie['crit_id']]
    movie.update_from_dict(existing_movie)
    db.set_movie(movie)
    db.connect()
    db.c.execute("select * from genres where crit_id = %s", [existing_movie['crit_id']])
    results = db.c.fetchall()
    db.close()
    assert set([result['genre'] for result in results]) == {'Action', 'Sci-Fi'}
    assert db.movies[1234].year == 1999
    db = MySQLDatabase(schema='qmdb_test')
    assert 1234 in list(db.movies.keys())
    assert db.movies[1234].title == 'The Matrix 2'
    assert db.movies[1234].year == 1999
    assert db.movies[1234].genres == ['Action', 'Sci-Fi']
    remove_test_tables(db)


def test_get_existing_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    movie = db.get_movie(1234)
    assert movie.title == 'The Matrix'
    assert movie.year == 1999
    remove_test_tables(db)


def test_get_non_existent_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    with pytest.raises(MovieNotInDatabaseError) as e_info:
        movie = db.get_movie(12345)
    remove_test_tables(db)


def test_add_column():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    db.add_column('test_column', 'mediumint', table_name='movies', after='crit_id')
    db.connect()
    db.c.execute("""
        SELECT column_name, 
               column_type 
          FROM information_schema.columns
         WHERE table_name='movies' and table_schema='qmdb_test'
        """)
    columns = db.c.fetchall()
    db.close()
    assert columns[1]['column_name'] == 'test_column'
    assert columns[1]['column_type'] == 'mediumint(9)'
    db.add_column('imdbid', 'integer', table_name='movies')
    remove_test_tables(db)


def test_add_columns(mocker):
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    mocker.patch.object(db, 'add_column')
    db.add_columns([{'column_name': 'test_column1', 'column_datatype': 'mediumint'},
                    {'column_name': 'test_column2', 'column_datatype': 'bigint'}],
                   table_name=['movies_copy', 'movies_copy2'])
    assert db.add_column.call_count == 4
    assert db.add_column.call_args_list[3][0] == ('test_column2', 'bigint')
    assert db.add_column.call_args_list[3][1] == {'table_name': 'movies_copy2', 'after': None}
    db.add_columns([{'column_name': 'test_column1', 'column_datatype': 'mediumint'},
                    {'column_name': 'test_column2', 'column_datatype': 'bigint'}],
                   table_name='movies_copy')
    assert db.add_column.call_count == 6
    assert db.add_column.call_args_list[5][0] == ('test_column2', 'bigint')
    assert db.add_column.call_args_list[5][1] == {'table_name': 'movies_copy', 'after': None}
    remove_test_tables(db)


def test_create_insert_multiple_records_sql():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    d = {'crit_id': 123,
         'n_rows': 3,
         'rank': [1, 2, 3],
         'tagline': ['Hello', 'Hi', 'Hey']}
    sql, values = db.create_insert_multiple_records_sql('taglines', d)
    remove_test_tables(db)


def test_movie_to_dict_languages():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    movie = Movie({'crit_id': 1234, 'languages': ['English', 'Spanish']})
    d = db.movie_to_dict_languages(movie)
    assert d == {'crit_id': 1234,
                 'n_rows': 2,
                 'language': ['English', 'Spanish'],
                 'rank': [1, 2]}


def test_update_multiple_records():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    d = {'crit_id': 1234,
         'n_rows': 2,
         'language': ['English', 'Spanish'],
         'rank': [1, 2]}
    db.update_multiple_records('languages', d)
    db.load()
    assert db.movies[1234].languages == ['English', 'Spanish']
    assert db.movies[49141].languages == ['English']
    remove_test_tables(db)


def test_load_languages(mocker):
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    mocker.patch.object(db, 'load_or_initialize', lambda x: None)
    movies = {1234: dict(),
              49141: dict()}
    db.connect()
    db.load_languages(movies)
    db.close()
    assert movies[1234]['languages'] == ['English', 'French']
    assert movies[49141]['languages'] == ['English']
    remove_test_tables(db)


def test_load_persons(mocker):
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    mocker.patch.object(db, 'load_or_initialize', lambda x: None)
    movies = {1234: dict(),
              49141: dict()}
    db.connect()
    db.load_persons(movies)
    db.close()
    assert [e['name'] for e in movies[1234]['director']] == ['Lana Wachowski', 'J.J. Abrams']
    assert [e['name'] for e in movies[1234]['cast']] == ['Anthony Hopkins', 'Tom Cruise']
    assert [e['name'] for e in movies[49141]['director']] == ['Steven Spielberg']
    assert [e['name'] for e in movies[49141]['cast']] == ['Natalie Portman']
    remove_test_tables(db)


def test_parse_store_load_persons():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test')
    imdb_scraper = IMDBScraper()
    imdbid = '3315342'
    movie_info = imdb_scraper.process_main_info(imdbid)
    movie_info.update({'crit_id': 1234,
                       'title': 'Logan',
                       'year': 2017,
                       'crit_url': 'http://www.criticker.com/film/Logan/',
                       'date_added': '2018-01-01'})
    movie = Movie(movie_info)
    db.set_movie(movie)
    db = MySQLDatabase(schema='qmdb_test')
    assert db.movies[1234].cast[0]['name'] == 'Hugh Jackman'
    assert db.movies[1234].director[0]['name'] == 'James Mangold'
    assert db.movies[1234].writer[1]['name'] == 'Scott Frank'
