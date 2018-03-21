import pytest
from mock import patch

from qmdb.interfaces.netflix import NetflixScraper
from qmdb.test.test_utils import side_effect
from qmdb.utils.utils import no_internet
from qmdb.test.test_utils import create_test_tables, remove_test_tables
from qmdb.database.database import MySQLDatabase


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
    assert db.netflix_genres == {1: None, 2: None, 3: None, 4: None}
    remove_test_tables(db)


def test_unogs_movie_info_to_dict():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    netflix_scraper = NetflixScraper(db)
    movie_info = netflix_scraper.unogs_movie_info_to_dict(
        {'netflixid': '80192014', 'download': '0', 'imdbid': 'tt4797160', 'rating': '0', 'title': 'Larceny'})
    assert movie_info == {'netflix_id': 80192014, 'netflix_title': 'Larceny', 'netflix_rating': 0.0, 'imdbid': 4797160}
    movie_info = netflix_scraper.unogs_movie_info_to_dict(
        {'netflixid': '80192014', 'download': '0', 'imdbid': 'notfound', 'rating': '0', 'title': 'Larceny'})
    assert movie_info == {'netflix_id': 80192014, 'netflix_title': 'Larceny', 'netflix_rating': 0.0, 'imdbid': None}
    remove_test_tables(db)


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_movies_for_genre_page():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    netflix_scraper = NetflixScraper(db)
    nr_pages, movies = netflix_scraper.get_movies_for_genre_page(10673, country_code=67, pagenr=1)
    assert nr_pages == 1
    assert len(movies) == 60
    assert list(movies[0].keys()) == ['netflix_id', 'netflix_title', 'netflix_rating', 'imdbid']
    remove_test_tables(db)


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_movies_for_genre():
    create_test_tables(variant='updates')
    db = MySQLDatabase(schema='qmdb_test', env='tst')
    db.netflix_genres = {1: None}
    netflix_scraper = NetflixScraper(db)
    movies = netflix_scraper.get_movies_for_genre(10673, country_code=67)
    assert len(movies) == 60
    assert list(movies[0].keys()) == ['netflix_id', 'netflix_title', 'netflix_rating', 'imdbid']
    remove_test_tables(db)
