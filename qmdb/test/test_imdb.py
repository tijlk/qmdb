import datetime as dt

import arrow
import pytest
from mock import patch

from qmdb.interfaces.imdb import IMDBScraper
from qmdb.movie.movie import Movie
from qmdb.test.test_utils import side_effect, load_obj, save_obj
from qmdb.utils.utils import no_internet
from imdb.parser.http import IMDbHTTPAccessSystem


@patch.object(IMDBScraper, 'process_main_info',
              new=side_effect(lambda x: {'imdb_title': 'The Matrix', 'imdb_votes': 2000}))
def test_refresh_movie():
    imdb_scraper = IMDBScraper()
    movie = Movie({'crit_id': 1234, 'imdbid': 133093})
    imdb_scraper.refresh_movie(movie)
    assert imdb_scraper.process_main_info.call_count == 1
    assert imdb_scraper.process_main_info.call_args_list[0][0] == ('0133093',)
    assert movie.imdb_votes == 2000
    assert movie.crit_id == 1234
    assert movie.imdb_title == 'The Matrix'


def test_person_to_dict():
    class Person:
        pass
    imdb_scraper = IMDBScraper()
    person = Person()
    person.data = {'name': 'Tijl Kindt'}
    person.personID = 1234
    person_dict = imdb_scraper.person_to_dict(person)
    assert person_dict == {'canonical_name': 'Kindt, Tijl',
                           'name': 'Tijl Kindt',
                           'person_id': 1234}


def test_remove_duplicate_dicts():
    l = [{'a': 3, 'b': 4},
         {'a': 1, 'b': 2},
         {'a': 3, 'b': 4},
         {'a': 1, 'b': 2},
         {'a': 5, 'b': 6}]
    imdb_scraper = IMDBScraper()
    new_l = imdb_scraper.remove_duplicate_dicts(l)
    assert new_l == [{'a': 3, 'b': 4}, {'a': 1, 'b': 2}, {'a': 5, 'b': 6}]


@patch.object(IMDbHTTPAccessSystem, 'get_movie_main',
              new=side_effect(lambda x: load_obj('imdb-main-info-the-matrix')))
def test_process_main_info():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_main_info('0133093')
    assert info['imdb_title'] == 'The Matrix'
    assert info['imdb_year'] == 1999
    assert info['kind'] == 'movie'
    assert info['cast'][0]['name'] == 'Keanu Reeves'
    assert len(info['cast']) == 39
    assert info['director'][0]['name'] == 'Lana Wachowski'
    assert len(info['director']) == 2
    assert info['writer'][0]['name'] == 'Lilly Wachowski'
    assert len(info['writer']) == 2
    assert info['genres'] == ['Action', 'Sci-Fi']
    assert info['runtime'] == 136
    assert info['countries'] == ['United States']
    assert info['imdb_rating'] == 8.7
    assert info['imdb_votes'] == 1379790
    assert info['plot_storyline'][:10] == 'Thomas A. '
    assert info['languages'] == ['English']


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
def test_get_movie_info_foreign_movie():
    imdb_scraper = IMDBScraper()
    movie_info = imdb_scraper.process_release_info('0087622')
    assert isinstance(movie_info, dict)
    assert movie_info['original_title'] == 'De lift'


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_movie_info_languages():
    imdb_scraper = IMDBScraper()
    movie_info = imdb_scraper.process_main_info('0211915')
    assert set(movie_info['languages']) == {'English', 'Russian', 'French'}
    movie_info = imdb_scraper.process_main_info('3315342')
    assert set(movie_info['languages']) == {'English', 'Spanish'}


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_get_movie_info_taglines():
    imdb_scraper = IMDBScraper()
    movie_info = imdb_scraper.process_taglines_info('3315342')
    assert isinstance(movie_info, dict)
    assert movie_info['taglines'] == ['His time has come']


