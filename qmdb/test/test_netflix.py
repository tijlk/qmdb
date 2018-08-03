import pytest
import requests_mock
from mock import patch

from qmdb.database.database import MySQLDatabase
from qmdb.interfaces.netflix import NetflixScraper, NoUnogsRequestsRemaining
from qmdb.test.test_utils import create_test_tables, remove_test_tables, read_file, side_effect
from qmdb.utils.utils import no_internet
import requests
import arrow


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_authurl():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    netflix_scraper = NetflixScraper(db)
    netflix_scraper.get_authurl()
    assert isinstance(netflix_scraper.authURL, str)
    assert len(netflix_scraper.authURL) > 20


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_do_unogs_request():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    netflix_scraper = NetflixScraper(db)
    rjson = netflix_scraper.do_unogs_request("https://unogs-unogs-v1.p.mashape.com/api.cgi?t=genres")
    assert rjson['COUNT'] == '517'
    assert len(rjson['ITEMS']) == 517
    remove_test_tables(db)


@patch.object(NetflixScraper, 'do_unogs_request',
              new=side_effect(lambda *args: {'ITEMS': [{'All Action': [4, 2, 1]}, {'All Anime': [2, 3]}]}))
def test_get_genre_ids():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    netflix_scraper = NetflixScraper(db)
    netflix_scraper.get_genre_ids()
    assert db.netflix_genres == {1: {'genre_names': ['All Action'], 'movies_updated': None},
                                 2: {'genre_names': ['All Action', 'All Anime'], 'movies_updated': None},
                                 3: {'genre_names': ['All Anime'], 'movies_updated': None},
                                 4: {'genre_names': ['All Action'], 'movies_updated': None}}
    remove_test_tables(db)


def test_unogs_movie_info_to_dict():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    netflix_scraper = NetflixScraper(db)
    movie_info = netflix_scraper.unogs_movie_info_to_dict(
        {'netflixid': '80192014', 'download': '0', 'imdbid': 'tt4797160', 'rating': '0', 'title': 'Larceny'})
    assert set(movie_info.keys()) == {'netflix_id', 'netflix_title', 'netflix_rating', 'imdbid', 'unogs_updated'}
    assert movie_info['netflix_id'] == 80192014
    assert movie_info['netflix_title'] == 'Larceny'
    assert movie_info['netflix_rating'] == 0.0
    assert movie_info['imdbid'] == 4797160
    movie_info = netflix_scraper.unogs_movie_info_to_dict(
        {'netflixid': '80192014', 'download': '0', 'imdbid': 'notfound', 'rating': '0', 'title': 'Larceny'})
    assert movie_info is None
    movie_info = netflix_scraper.unogs_movie_info_to_dict(
        {'netflixid': '80192014', 'download': '0', 'imdbid': 'tt4797160', 'rating': '', 'title': 'Larceny'})
    assert movie_info['netflix_rating'] is None
    remove_test_tables(db)


@patch.object(NetflixScraper, 'get_critid_from_imdbid', new=side_effect(lambda *args: 123))
def test_get_movies_for_genre_page():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    netflix_scraper = NetflixScraper(db)
    with requests_mock.Mocker() as m:
        url = 'https://unogs-unogs-v1.p.mashape.com/aaapi.cgi?q={query}-!1800,2050-!0,5-!0,10-!10673-!Any-!Any-!Any-' \
              '!Any-!{downloadable}&t=ns&cl=67&st=adv&ob=Relevance&p=1&sa=and'
        headers = {'X-RateLimit-requests-Remaining': '100'}
        m.get(url, text=read_file('test/fixtures/unogs-get-genre-page.html'), headers=headers)
        nr_pages, movies = netflix_scraper.get_movies_for_genre_page(10673, country_code=67, pagenr=1)
    assert nr_pages == 1
    assert len(movies) == 59
    assert set(movies[0].keys()) == {'netflix_id', 'netflix_title', 'netflix_rating',
                                     'imdbid', 'crit_id', 'unogs_updated'}
    remove_test_tables(db)


@patch.object(NetflixScraper, 'get_movies_for_genre_page', new=side_effect(lambda *args, **kwargs: (2, [1])))
def test_get_movies_for_genre():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    db.netflix_genres = {1: None}
    netflix_scraper = NetflixScraper(db)
    movies = netflix_scraper.get_movies_for_genre(2)
    assert movies == [1, 1]
    remove_test_tables(db)


@patch.object(arrow, 'now', new=side_effect(lambda *args: arrow.get('2018-04-01')))
@patch.object(NetflixScraper, 'get_movies_for_genre',
              new=side_effect(lambda *args, **kwargs: [{'netflix_id': '80192014',  'netflix_title': 'Larceny',
                                                        'netflix_rating': 4.8, 'imdbid': 123456, 'crit_id': 1234}]))
def test_get_movies_for_genres():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    db.netflix_genres = {1: {'genre_names': ['Action', 'Sci-Fi'],
                             'movies_updated': arrow.get('2018-03-28')},
                         2: {'genre_names': ['Sci-Fi', 'Drama'],
                             'movies_updated': arrow.get('2018-03-20')},
                         3: {'genre_names': ['Drama'],
                             'movies_updated': None},
                         4: {'genre_names': ['Romance'],
                             'movies_updated': arrow.get('2018-03-17')}}
    netflix_scraper = NetflixScraper(db)
    netflix_scraper.get_movies_for_genres()
    assert (arrow.now() - db.netflix_genres[1]['movies_updated']).days == 4
    assert arrow.now() == db.netflix_genres[2]['movies_updated']
    assert arrow.now() == db.netflix_genres[3]['movies_updated']
    assert arrow.now() == db.netflix_genres[4]['movies_updated']
    assert [call[0][0] for call in netflix_scraper.get_movies_for_genre.call_args_list] == [3, 4, 2]
    assert db.movies[1234].netflix_rating == 4.8
    remove_test_tables(db)


@patch.object(arrow, 'now', new=side_effect(lambda *args: arrow.get('2018-04-01')))
@patch.object(NetflixScraper, 'get_movies_for_genre', new=side_effect(NoUnogsRequestsRemaining))
@patch.object(MySQLDatabase, 'save_movies', new=side_effect(None))
def test_get_movies_for_genres_norequests():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    db.netflix_genres = {1: {'genre_names': ['Action', 'Sci-Fi'],
                             'movies_updated': arrow.get('2018-03-28')},
                         2: {'genre_names': ['Sci-Fi', 'Drama'],
                             'movies_updated': arrow.get('2018-03-20')},
                         3: {'genre_names': ['Drama'],
                             'movies_updated': None},
                         4: {'genre_names': ['Romance'],
                             'movies_updated': arrow.get('2018-03-17')}}
    netflix_scraper = NetflixScraper(db)
    netflix_scraper.get_movies_for_genres()
    assert db.netflix_genres[1]['movies_updated'] == arrow.get('2018-03-28')
    assert db.netflix_genres[2]['movies_updated'] == arrow.get('2018-03-20')
    assert db.netflix_genres[3]['movies_updated'] is None
    assert db.netflix_genres[4]['movies_updated'] == arrow.get('2018-03-17')
    assert [call[0][0] for call in netflix_scraper.get_movies_for_genre.call_args_list] == [3]
    assert db.save_movies.call_count == 0
    assert db.movies[1234].netflix_rating is None
    remove_test_tables(db)

