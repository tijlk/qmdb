import os

import arrow
import pytest
from mock import patch

from qmdb.database.database import MySQLDatabase
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.movie.movie import Movie
from qmdb.utils.utils import no_internet
from qmdb.test.test_utils import create_test_tables, remove_test_tables


def test_config_cookies():
    crit_scraper = CritickerScraper()
    assert isinstance(crit_scraper.cookies, dict)
    assert 'uid2' in crit_scraper.cookies


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_cookies_work():
    crit_scraper = CritickerScraper()


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_movie_list_page():
    pagenr = 1
    popularity = 9
    min_year = 2000
    criticker_scraper = CritickerScraper()
    movies, nr_pages = criticker_scraper.get_movie_list_popularity_page(pagenr=pagenr, popularity=popularity, min_year=min_year)
    assert len(movies) == 60
    assert isinstance(movies[0], dict)
    assert isinstance(movies[0]['date_added'], arrow.Arrow)
    assert isinstance(nr_pages, int)


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_movie_list(mocker):
    # TODO: improve this test. Why is mocker not working????
    mocker.patch.object(CritickerScraper, 'get_movies_of_popularity', lambda *args, **kwargs: [1]*120)
    start_popularity = 1
    criticker_scraper = CritickerScraper()
    movies = criticker_scraper.get_movies(start_popularity=start_popularity)
    assert len(movies) == 1200


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_movie_info():
    crit_scraper = CritickerScraper()
    crit_url = 'https://www.criticker.com/film/Pulp-Fiction/'
    movie_info = crit_scraper.get_movie_info(crit_url)
    assert isinstance(movie_info, dict)
    assert 'imdbid' in movie_info
    assert movie_info['imdbid'] == 110912
    assert movie_info['poster_url'] == 'https://www.criticker.com/img/films/posters/Pulp-Fiction.jpg'
    assert movie_info['crit_votes'] >= 25900


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_digits_in_trailer_id():
    # Trailer has digits in the id
    crit_scraper = CritickerScraper()
    crit_url = 'https://www.criticker.com/film/The-Matrix/'
    movie_info = crit_scraper.get_movie_info(crit_url)
    assert isinstance(movie_info, dict)
    assert 'imdbid' in movie_info


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_no_trailer():
    # No trailer
    crit_scraper = CritickerScraper()
    crit_url = 'https://www.criticker.com/film/Daens/'
    movie_info = crit_scraper.get_movie_info(crit_url)
    assert isinstance(movie_info, dict)
    assert 'imdbid' in movie_info
    assert movie_info['trailer_url'] is None


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_no_rating_of_my_own():
    # No rating of my own
    crit_scraper = CritickerScraper()
    crit_url = 'https://www.criticker.com/film/The-Mask/'
    movie_info = crit_scraper.get_movie_info(crit_url)
    assert isinstance(movie_info, dict)
    assert 'imdbid' in movie_info


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_no_poster():
    # No poster
    crit_scraper = CritickerScraper()
    crit_url = 'https://www.criticker.com/film/8-Tire-on-the-Ice/'
    movie_info = crit_scraper.get_movie_info(crit_url)
    assert isinstance(movie_info, dict)
    assert 'imdbid' in movie_info
    assert movie_info['poster_url'] is None


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_no_votes():
    crit_scraper = CritickerScraper()
    crit_url = 'https://www.criticker.com/film/16-Fathoms-Deep/'
    movie_info = crit_scraper.get_movie_info(crit_url)
    assert isinstance(movie_info, dict)
    assert 'imdbid' in movie_info
    assert movie_info['crit_votes'] == 0


def test_criticker_refresh_movie(mocker):
    # Test valid movie info
    movie = Movie({'crit_id': 123, 'crit_url': 'http://blahblah'})
    crit_scraper = CritickerScraper()
    mocker.patch.object(crit_scraper, 'get_movie_info', lambda *args: {'imdbid': 123456, 'crit_updated': arrow.now()})
    movie = crit_scraper.refresh_movie(movie)
    assert movie.imdbid == 123456
    # Test invalid movie info
    mocker.patch.object(crit_scraper, 'get_movie_info', lambda *args: 1234)
    movie = crit_scraper.refresh_movie(movie)
    assert movie is None


def test_get_year_from_movielist_title():
    criticker_scraper = CritickerScraper()
    movielist_title = 'The Matrix (1999)'
    assert criticker_scraper.get_year_from_movielist_title(movielist_title) == 1999


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_add_criticker_movies_to_db():
    start_popularity = 8
    criticker_scraper = CritickerScraper()
    movies = criticker_scraper.get_movies(start_popularity=start_popularity, debug=True)
    db = MySQLDatabase(schema='qmdb_test', from_scratch=True)
    for movie in movies:
        db.set_movie(Movie(movie))
    db = MySQLDatabase(schema='qmdb_test')
    assert 98304 in db.movies
    assert db.movies[98304].title == 'Interstellar'
    assert db.movies[98304].year == 2014
    assert db.movies[98304].crit_url == 'https://www.criticker.com/film/Interstellar/'
    remove_test_tables(db)

