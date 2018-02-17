import datetime as dt

import arrow
import pytest

from qmdb.interfaces.imdb import IMDBScraper
from qmdb.utils.utils import no_internet
from qmdb.movie.movie import Movie


def test_process_release_date():
    imdb_scraper = IMDBScraper()
    date_dict = imdb_scraper.process_release_date('Czech Republic::5 August 1999')
    assert date_dict['country'] == 'Czech Republic'
    assert date_dict['date'] == arrow.get(dt.datetime(1999, 8, 5))
    assert date_dict['tags'] is None
    date_dict = imdb_scraper.process_release_date('Argentina::2 October 2003 (re-release)')
    assert date_dict['country'] == 'Argentina'
    assert date_dict['date'] == arrow.get(dt.datetime(2003, 10, 2))
    assert date_dict['tags'] == ['re-release']
    date_dict = imdb_scraper.process_release_date('Portugal::9 June 1999 (Oporto)\n (premiere)')
    assert date_dict['country'] == 'Portugal'
    assert date_dict['date'] == arrow.get(dt.datetime(1999, 6, 9))
    assert date_dict['tags'] == ['Oporto', 'premiere']
    date_dict = imdb_scraper.process_release_date('USA::24 March 1999 (Westwood, California)\n (premiere)')
    assert date_dict['country'] == 'USA'
    assert date_dict['date'] == arrow.get(dt.datetime(1999, 3, 24))
    assert date_dict['tags'] == ['Westwood, California', 'premiere']


def test_process_release_info(mocker):
    imdb_scraper = IMDBScraper()
    release_data = {'data': {'release dates': ['USA::24 March 1999 (Westwood, California)\n (premiere)',
                                      'USA::28 March 1999 (Westwood, California)\n (limited)',
                                      'USA::30 March 1999']}}
    mocker.patch.object(imdb_scraper.ia, 'get_movie_release_dates', lambda *args: release_data)
    info = imdb_scraper.process_release_info(123)
    assert info['original_release_date'] == arrow.get(dt.datetime(1999, 3, 30))
    assert info['dutch_release_date'] is None
    release_data = {'data': {'release dates': ['Netherlands::4 April 1999 (premiere)',
                                      'USA::24 March 1999 (Westwood, California)\n (premiere)',
                                      'USA::28 March 1999 (Westwood, California)\n (limited)']}}
    mocker.patch.object(imdb_scraper.ia, 'get_movie_release_dates', lambda *args: release_data)
    info = imdb_scraper.process_release_info(123)
    assert info['original_release_date'] == arrow.get(dt.datetime(1999, 3, 28))
    assert info['dutch_release_date'] == arrow.get(dt.datetime(1999, 4, 4))
    release_data = {'data': {'release dates': ['Netherlands::4 April 1999 (premiere)',
                                      'Netherlands::8 April 1999',
                                      'USA::25 March 1999']}}
    mocker.patch.object(imdb_scraper.ia, 'get_movie_release_dates', lambda *args: release_data)
    info = imdb_scraper.process_release_info(123)
    assert info['original_release_date'] == arrow.get(dt.datetime(1999, 3, 25))
    assert info['dutch_release_date'] == arrow.get(dt.datetime(1999, 4, 8))
    release_data = {'data': {'release dates': ['USA::24 March 1999 (Westwood, California)\n (premiere)',
                                      'Bangladesh::27 March 1999',
                                      'Netherlands::5 April 1999 (limited)',
                                      'Netherlands::2 April 1999 (premiere)']}}
    mocker.patch.object(imdb_scraper.ia, 'get_movie_release_dates', lambda *args: release_data)
    info = imdb_scraper.process_release_info(123)
    assert info['original_release_date'] == arrow.get(dt.datetime(1999, 3, 27))
    assert info['dutch_release_date'] == arrow.get(dt.datetime(1999, 4, 5))
    release_data = {'data': {'release dates': ['Afghanistan::29 March 1999',
                                      'Bangladesh::27 March 1999']}}
    mocker.patch.object(imdb_scraper.ia, 'get_movie_release_dates', lambda *args: release_data)
    info = imdb_scraper.process_release_info(123)
    assert info['original_release_date'] == arrow.get(dt.datetime(1999, 3, 27))
    assert info['dutch_release_date'] is None


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_movie_info_matrix():
    imdb_scraper = IMDBScraper()
    imdbid = '0133093'
    movie_info = imdb_scraper.process_main_info(imdbid)
    assert isinstance(movie_info, dict)
    assert movie_info['genres'] == {'Action', 'Sci-Fi'}
    assert movie_info['imdb_rating'] == 8.7
    assert movie_info['imdb_votes'] >= 1375000
    movie_info = imdb_scraper.process_release_info(imdbid)
    assert movie_info['original_release_date'] == arrow.get(dt.datetime(1999, 3, 31))
    assert movie_info['dutch_release_date'] == arrow.get(dt.datetime(1999, 6, 17))
    movie_info = imdb_scraper.process_metacritic_info(imdbid)
    assert movie_info['metacritic_score'] == 73
    movie_info = imdb_scraper.process_keywords_info(imdbid)
    assert 'watermelon' in movie_info['keywords']
    movie_info = imdb_scraper.process_vote_details_info(imdbid)
    assert movie_info['vote_details']['non us users']['rating'] - \
           movie_info['vote_details']['us users']['rating'] == pytest.approx(0.1, 0.0001)


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_movie_info_foreign_movie():
    imdb_scraper = IMDBScraper()
    movie_info = imdb_scraper.process_release_info('0087622')
    assert isinstance(movie_info, dict)
    assert movie_info['original_title'] == 'De lift'


def test_refresh_movie():
    imdb_scraper = IMDBScraper()
    movie = Movie({'crit_id': 1234, 'imdbid': 133093})
    imdb_scraper.refresh_movie(movie)
