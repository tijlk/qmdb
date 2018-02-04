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
