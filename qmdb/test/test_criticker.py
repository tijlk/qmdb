from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.movie.movie import Movie
import arrow
from mock import patch, MagicMock
from qmdb.database.database import SQLiteDatabase
from qmdb.utils.utils import no_internet
import pytest
import os


def test_config_cookies():
    crit_scraper = CritickerScraper()
    assert isinstance(crit_scraper.cookies, dict)
    assert 'uid2' in crit_scraper.cookies


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_cookies_work():
    crit_scraper = CritickerScraper()


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_movie_list_page():
    pagenr = 2
    min_popularity = 3
    criticker_scraper = CritickerScraper()
    movies, nr_pages = criticker_scraper.get_movie_list_page(pagenr=pagenr, min_popularity=min_popularity)
    assert len(movies) == 60
    assert isinstance(movies[0], dict)
    assert isinstance(movies[0]['date_added'], arrow.Arrow)
    assert isinstance(nr_pages, int)


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_movie_list():
    min_popularity = 10
    criticker_scraper = CritickerScraper()
    movies = criticker_scraper.get_movies(min_popularity=min_popularity, debug=True)
    assert len(movies) > 60
    assert len(set([movie['crit_id'] for movie in movies])) == 120


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_movie_info():
    crit_scraper = CritickerScraper()
    crit_url = 'https://www.criticker.com/film/Pulp-Fiction/'
    movie_info = crit_scraper.get_movie_info(crit_url)
    assert isinstance(movie_info, dict)
    assert 'imdbid' in movie_info
    assert movie_info['imdbid'] == 110912


def fake_get_movie_info(self, crit_url):
    return {'imdbid': 123456, 'crit_updated': arrow.now()}


@patch.object(CritickerScraper, 'get_movie_info', fake_get_movie_info)
def test_criticker_refresh_movie():
    movie = Movie({'crit_id': 123, 'crit_url': 'http://blahblah'})
    crit_scraper = CritickerScraper()
    crit_scraper.refresh_movie(movie)
    assert movie.imdbid == 123456


def test_get_year_from_movielist_title():
    criticker_scraper = CritickerScraper()
    movielist_title = 'The Matrix (1999)'
    assert criticker_scraper.get_year_from_movielist_title(movielist_title) == 1999


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_add_criticker_movies_to_db():
    min_popularity = 10
    criticker_scraper = CritickerScraper()
    movies = criticker_scraper.get_movies(min_popularity=min_popularity, debug=True)
    filename = 'test/fixtures/testdb_criticker_movies.sqlite'
    db = SQLiteDatabase(filename, from_scratch=True)
    for movie in movies:
        db.set_movie(Movie(movie))
    db = SQLiteDatabase(filename)
    assert 1129 in db.movies
    assert db.movies[1129].title == 'Forrest Gump'
    assert db.movies[1129].year == 1994
    assert db.movies[1129].crit_url == 'https://www.criticker.com/film/Forrest-Gump/'
    os.remove(filename)