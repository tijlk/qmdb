from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.movie.movie import Movie
import arrow
from mock import patch, MagicMock
from qmdb.database.database import Database
from qmdb.utils.utils import no_internet
import pytest


def test_config_cookies():
    crit_scraper = CritickerScraper()
    assert isinstance(crit_scraper.cookies, dict)
    assert 'uid2' in crit_scraper.cookies


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_cookies_work():
    crit_scraper = CritickerScraper()
