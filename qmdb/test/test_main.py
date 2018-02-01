from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.movie.movie import Movie
import arrow
from mock import patch, MagicMock
import pytest
from qmdb.utils.utils import no_internet


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_rturl_from_imdbid():
    matrix_url = OMDBScraper.imdbid_to_rturl(133093)
    print(matrix_url)
    assert matrix_url == 'http://www.rottentomatoes.com/m/matrix/'


@pytest.mark.skip(reason='Not implemented yet')
def test_bad_connection():
    pass


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_movie_list_page():
    pagenr = 2
    min_popularity = 3
    criticker_scraper = CritickerScraper()
    movies, nr_pages = criticker_scraper.get_movie_list_page(pagenr=pagenr, min_popularity=min_popularity)
    assert len(movies) == 60
    assert isinstance(movies[0], dict)
    assert isinstance(nr_pages, int)


def test_get_year_from_movielist_title():
    criticker_scraper = CritickerScraper()
    movielist_title = 'The Matrix (1999)'
    assert criticker_scraper.get_year_from_movielist_title(movielist_title) == 1999


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_criticker_movie_list():
    min_popularity=10
    criticker_scraper = CritickerScraper()
    movies = criticker_scraper.get_movies(min_popularity=min_popularity, debug=True)
    assert len(movies) > 60


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