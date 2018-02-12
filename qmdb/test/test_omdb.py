import pytest
from mock import patch

from qmdb.interfaces.omdb import OMDBScraper, InvalidIMDbIdError
from qmdb.movie.movie import Movie
from qmdb.utils.utils import no_internet


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_imdbid_to_rturl_valid_imdbid():
    omdb_scraper = OMDBScraper()
    tomato_url = omdb_scraper.imdbid_to_rturl(133093)
    assert tomato_url == 'http://www.rottentomatoes.com/m/matrix/'


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_imdbid_to_rturl_invalid_imdbid():
    omdb_scraper = OMDBScraper()
    with pytest.raises(InvalidIMDbIdError):
        tomato_url = omdb_scraper.imdbid_to_rturl(12345678)
        assert tomato_url is None
    with pytest.raises(InvalidIMDbIdError):
        tomato_url = omdb_scraper.imdbid_to_rturl(None)
        assert tomato_url is None


@patch.object(OMDBScraper, 'imdbid_to_rturl', lambda self, imdbid: 'http://www.rottentomatoes.com/m/the-matrix/')
def test_refresh_movie():
    omdb_scraper = OMDBScraper()
    movie = Movie({'crit_id': 1234, 'imdbid': 133093})
    movie = omdb_scraper.refresh_movie(movie)
    assert movie.tomato_url == 'http://www.rottentomatoes.com/m/the-matrix/'