import pytest
from mock import patch

from qmdb.utils.utils import no_internet
from qmdb.interfaces.passthepopcorn import PassThePopcornScraper


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_movie_info():
    ptp_scraper = PassThePopcornScraper()
    movie_info = ptp_scraper.get_movie_info(6769208)
    assert movie_info['ptp_url'] == 'https://passthepopcorn.me/torrents.php?id=165546'
    assert movie_info['ptp_hd_available'] is True
