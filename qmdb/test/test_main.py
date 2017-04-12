from qmdb.interfaces.omdb import OMDBScraper


def test_rturl_from_imdbid():
    matrix_url = OMDBScraper.imdbid_to_rturl(133093)
    print(matrix_url)
    assert matrix_url == 'http://www.rottentomatoes.com/m/matrix/'
