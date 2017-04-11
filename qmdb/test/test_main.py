from qmdb.interfaces.omdb import imdbid_to_rturl


def test_rturl_from_imdbid():
    matrix_url = imdbid_to_rturl(133093)
    print(matrix_url)
    assert matrix_url == 'http://www.rottentomatoes.com/m/matrix/'
