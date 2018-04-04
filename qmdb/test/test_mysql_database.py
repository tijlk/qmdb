import arrow
import pytest

from qmdb.database.database import MySQLDatabase, MovieNotInDatabaseError, max_date
from qmdb.interfaces.imdb import IMDBScraper
from qmdb.movie.movie import Movie
from qmdb.test.test_utils import create_test_tables, remove_test_tables
from qmdb.utils.utils import no_internet


def test_max_date():
    assert max_date(['2018-02-05 23:01:58+01:00', '2018-02-04 23:01:58+01:00', None]) \
           == arrow.get('2018-02-05 23:01:58+01:00')
    assert max_date([None, None]) is None


def test_database_init_existing_file():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == [1234, 49141]
    remove_test_tables(db)


def test_database_init_from_scratch_new():
    db = MySQLDatabase(schema='qmdb_test', from_scratch=True, env='test')
    assert db.c is not None
    assert isinstance(db.movies, dict)
    print(db.movies.keys())
    assert list(db.movies.keys()) == []
    remove_test_tables(db)


def test_database_init_from_scratch_existing():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    assert list(db.movies.keys()) == [1234, 49141]
    db = MySQLDatabase(schema='qmdb_test', from_scratch=True, env='test')
    assert list(db.movies.keys()) == []
    remove_test_tables(db)


def test_add_new_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    new_movie = {'crit_id': 12345,
                 'crit_popularity_page': 1,
                 'crit_url': 'blahblah',
                 'title': 'Pulp Fiction',
                 'poster_url': 'http://blahblah',
                 'date_added': arrow.now()}
    db.set_movie(Movie(new_movie))
    db = MySQLDatabase(schema='qmdb_test', env='test')
    assert 12345 in list(db.movies.keys())
    assert db.movies[12345].title == 'Pulp Fiction'
    remove_test_tables(db)


def test_add_weird_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    new_movie = {'crit_id': 60326,
                 'crit_popularity_page': 79,
                 'crit_url': 'https://www.criticker.com/film/alg305-cengi/',
                 'title': 'Çalgı çengi',
                 'year': 2011,
                 'date_added': arrow.now()}
    db.set_movie(Movie(new_movie))
    db = MySQLDatabase(schema='qmdb_test', env='test')
    assert 60326 in list(db.movies.keys())
    assert db.movies[60326].title == 'Çalgı çengi'
    remove_test_tables(db)


def test_update_existing_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
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
    db = MySQLDatabase(schema='qmdb_test', env='test')
    assert 1234 in list(db.movies.keys())
    assert db.movies[1234].title == 'The Matrix 2'
    assert db.movies[1234].year == 1999
    assert db.movies[1234].genres == ['Action', 'Sci-Fi']
    remove_test_tables(db)


def test_get_existing_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    movie = db.get_movie(1234)
    assert movie.title == 'The Matrix'
    assert movie.year == 1999
    remove_test_tables(db)


def test_get_non_existent_movie():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    with pytest.raises(MovieNotInDatabaseError) as e_info:
        movie = db.get_movie(12345)
    remove_test_tables(db)


def test_add_column():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    db.connect()
    db.add_column('test_column', 'mediumint', table_name='movies', after='crit_id')
    db.c.execute("""
        SELECT column_name, 
               column_type 
          FROM information_schema.columns
         WHERE table_name='movies' and table_schema='qmdb_test'
        """)
    columns = db.c.fetchall()
    assert columns[1]['column_name'] == 'test_column'
    assert columns[1]['column_type'] == 'mediumint(9)'
    db.add_column('imdbid', 'integer', table_name='movies')
    db.close()
    remove_test_tables(db)


def test_add_columns(mocker):
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    mocker.patch.object(db, 'add_column')
    db.add_columns([{'column_name': 'test_column1', 'column_datatype': 'mediumint'},
                    {'column_name': 'test_column2', 'column_datatype': 'bigint'}],
                   table_name=['movies_copy', 'movies_copy2'])
    assert db.add_column.call_count == 4
    assert db.add_column.call_args_list[3][0] == ('test_column2', 'bigint')
    assert db.add_column.call_args_list[3][1] == {'table_name': 'movies_copy2', 'after': None, 'first': None}
    db.add_columns([{'column_name': 'test_column1', 'column_datatype': 'mediumint'},
                    {'column_name': 'test_column2', 'column_datatype': 'bigint'}],
                   table_name='movies_copy')
    assert db.add_column.call_count == 6
    assert db.add_column.call_args_list[5][0] == ('test_column2', 'bigint')
    assert db.add_column.call_args_list[5][1] == {'table_name': 'movies_copy', 'after': None, 'first': None}
    remove_test_tables(db)


def test_create_insert_multiple_records_sql():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    d = {'crit_id': 123,
         'n_rows': 3,
         'rank': [1, 2, 3],
         'tagline': ['Hello', 'Hi', 'Hey']}
    sql, values = db.create_insert_multiple_records_sql('taglines', d)
    remove_test_tables(db)


def test_movie_to_dict_languages():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    movie = Movie({'crit_id': 1234, 'languages': ['English', 'Spanish']})
    d = db.movie_to_dict_languages(movie)
    assert d == {'crit_id': 1234,
                 'n_rows': 2,
                 'language': ['English', 'Spanish'],
                 'rank': [1, 2]}


def test_update_multiple_records():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
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
    db = MySQLDatabase(schema='qmdb_test', env='test')
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
    db = MySQLDatabase(schema='qmdb_test', env='test')
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


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_parse_store_load_persons():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
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
    db = MySQLDatabase(schema='qmdb_test', env='test')
    assert db.movies[1234].cast[0]['name'] == 'Hugh Jackman'
    assert db.movies[1234].director[0]['name'] == 'James Mangold'
    assert db.movies[1234].writer[1]['name'] == 'Scott Frank'


def test_load_netflix_genres():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    db.connect()
    db.load_netflix_genres()
    assert db.netflix_genres == {1: {'genre_names': ['All Action', 'All Anime'],
                                     'movies_updated': arrow.get('2018-02-04 23:01:58+01:00')},
                                 2: {'genre_names': ['All Anime'],
                                     'movies_updated': None}}


def test_set_netflix_genres():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    db.netflix_genres = {1: {'genre_names': ['All Action', 'Belgian Movies'],
                             'movies_updated': arrow.get('2018-02-05 23:01:58+01:00')},
                         3: {'genre_names': ['Japanese Anime'],
                             'movies_updated': None}}
    db.set_netflix_genres()
    db.connect()
    db.load_netflix_genres()
    db.close()
    assert db.netflix_genres == {1: {'genre_names': ['All Action', 'Belgian Movies'],
                                     'movies_updated': arrow.get('2018-02-05 23:01:58+01:00')},
                                 3: {'genre_names': ['Japanese Anime'],
                                     'movies_updated': None}}


def test_create_imdbid_to_crit_id_dict():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    db.movies = {1: Movie({'crit_id': 1,
                           'imdbid': 101}),
                 2: Movie({'crit_id': 2,
                           'imdbid': 102}),
                 3: Movie({'crit_id': 3,
                           'imdbid': 102}),
                 4: Movie({'crit_id': 4,
                           'imdbid': None})}
    db.create_imdbid_to_crit_id_dict()
    assert db.imdbid_to_critid == {101: 1, 102: {2, 3}}


def test_add_missing_columns():
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    db.columns_netflix_genres = {
            'genreid': 'int(10) unsigned',
            'genre_name': 'varchar(128)',
            'test_column': 'int(10) unsigned',
            'movies_updated': 'varchar(32)'
        }
    db.connect()
    db.add_missing_columns('netflix_genres')
    db.c.execute("""
        SELECT
            column_name,
            column_type
        FROM information_schema.columns 
        WHERE table_name='netflix_genres'
          AND table_schema='qmdb_test'
        """)
    actual_cols = db.c.fetchall()
    actual_cols_dict = {d['column_name']: d['column_type'] for d in actual_cols}
    assert actual_cols_dict == db.columns_netflix_genres
    assert list(actual_cols_dict.keys()) == list(db.columns_netflix_genres)
