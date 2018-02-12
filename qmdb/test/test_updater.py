import datetime as dt
import time

import arrow
import pytest
from mock import patch

from qmdb.database.database import MySQLDatabase
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.updater import Updater
from qmdb.movie.utils import humanized_time
from qmdb.utils.utils import create_copy_of_table
from qmdb.utils.utils import no_internet
import mock
import pytest
from pytest_mock import mocker


def test_get_movies_stats():
    db = MySQLDatabase()
    updater = Updater()
    updater.get_movies_stats(db)
    print(updater.years)
    print(updater.crit_pop_pages)
    movie = db.movies[49141]
    print(humanized_time(movie.date_added), humanized_time(movie.crit_updated), humanized_time(movie.omdb_updated))
    updates = updater.calculate_next_updates(movie)
    print(updates)
    for update in updates:
        print(humanized_time(update['next_update']))


def test_calculate_frequency_score():
    updater = Updater()
    score = updater.calculate_period_score(11, {'a_parameter': 0.91866, 'b_parameter': 0.35824})
    assert score == pytest.approx(8.748, 0.01)


def test_parameters():
    updater = Updater()
    b_parameter = updater.b_parameter(13, 90, median_period=10, max_period=100)
    assert b_parameter == pytest.approx(0.35824, 0.01)
    a_parameter = updater.a_parameter(13, b_parameter, median_period=10)
    assert a_parameter == pytest.approx(0.91866, 0.01)
    print(a_parameter, b_parameter)


def test_calculate_final_period():
    updater = Updater()
    update_period = updater.calculate_update_period(10, 15)
    assert update_period == pytest.approx(11.7608, 0.01)
    update_period = updater.calculate_update_period(10, 10)
    assert update_period == pytest.approx(10, 0.01)
    update_period = updater.calculate_update_period(10, 200, crit_pop_pages_power=0)
    assert update_period == pytest.approx(10, 0.01)
    update_period = updater.calculate_update_period(10, 200, crit_pop_pages_power=1/100)
    assert update_period == pytest.approx(10, 0.01)


def test_calculate_next_update():
    updater = Updater()
    next_update = updater.calculate_next_update(arrow.get(dt.datetime(2018, 1, 1)),
                                                arrow.get(dt.datetime(2018, 2, 1)),
                                                2, weibull_lambda=10000)
    assert next_update.timestamp > arrow.get(dt.datetime(2018, 2, 14)).timestamp
    assert next_update.timestamp < arrow.get(dt.datetime(2018, 2, 16)).timestamp
    next_update = updater.calculate_next_update(arrow.get(dt.datetime(2018, 1, 1)),
                                                None,
                                                100, weibull_lambda=10000)
    assert next_update.timestamp > arrow.get(dt.datetime(2018, 1, 7)).timestamp
    assert next_update.timestamp < arrow.get(dt.datetime(2018, 1, 9)).timestamp


@patch.object(CritickerScraper, 'get_movie_info',
              lambda x, y: {'imdbid': 12345, 'crit_updated': arrow.get('2019-01-01 00:00:00+01:00')})
@patch.object(OMDBScraper, 'imdbid_to_rturl', lambda x, y: 'http://www.rottentomatoes.com/m/the-matrix/')
@patch.object(arrow, 'now', lambda: arrow.get('2019-01-01 00:00:00+01:00'))
def test_get_update_sequence():
    create_copy_of_table('movies_updates', 'movies_updates_copy')
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_updates_copy')
    updater = Updater()
    updater.get_movies_stats(db)
    sorted_seq, uph = updater.get_update_sequence(db, multiplier_criticker=1, multiplier_omdb=1, weibull_lambda=10000)
    assert len(sorted_seq) == 10
    for e in sorted_seq:
        assert isinstance(e, dict)
    assert len([e for e in sorted_seq if e['source'] == 'criticker']) == 5
    assert len([e for e in sorted_seq if e['source'] == 'OMDB']) == 5


@pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
def test_update_source():
    create_copy_of_table('movies', 'movies_copy')
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_copy')
    updater = Updater()
    crit_id = 1234
    updater.update_source(db, {'source': 'criticker', 'crit_id': crit_id})
    assert db.movies[crit_id].crit_updated >= arrow.now().shift(minutes=-1)
    updater.update_source(db, {'source': 'OMDB', 'crit_id': crit_id})
    assert db.movies[crit_id].omdb_updated >= arrow.now().shift(minutes=-1)


d = [{'source': 'criticker', 'crit_id': 1234,  'next_update': arrow.get('2018-01-01 01:40:00'), 'update_period': 1.0},
     {'source': 'OMDB',      'crit_id': 1234,  'next_update': arrow.get('2018-01-01 08:24:00'), 'update_period': 5.0},
     {'source': 'criticker', 'crit_id': 49141, 'next_update': arrow.get('2018-01-01 13:26:00'), 'update_period': 8.0},
     {'source': 'OMDB',      'crit_id': 49141, 'next_update': arrow.get('2018-01-03 19:11:00'), 'update_period': 40.0},
     {'source': 'criticker', 'crit_id': 2345,  'next_update': arrow.get('2018-01-05 11:30:00'), 'update_period': 64.0},
     {'source': 'criticker', 'crit_id': 4567,  'next_update': arrow.get('2018-01-08 00:00:00'), 'update_period': 1.0},
     {'source': 'OMDB',      'crit_id': 2345,  'next_update': arrow.get('2018-01-23 09:34:00'), 'update_period': 320.0},
     {'source': 'OMDB',      'crit_id': 4567,  'next_update': arrow.get('2018-02-04 23:56:00'), 'update_period': 5.0},
     {'source': 'criticker', 'crit_id': 5678,  'next_update': arrow.get('2018-02-26 00:06:00'), 'update_period': 8.0},
     {'source': 'OMDB',      'crit_id': 5678,  'next_update': arrow.get('2018-10-08 00:30:00'), 'update_period': 40.0}]


def test_update_movies(mocker):
    mocker.patch.object(Updater, 'get_update_sequence', lambda x, y, **kwargs: (d, 0.0163))
    mocker.patch.object(time, 'sleep', lambda x: None)
    mocker.patch.object(Updater, 'update_source', lambda x, y, z: None)
    create_copy_of_table('movies_updates', 'movies_updates_copy')
    db = MySQLDatabase(schema='qmdb_test', movies_table='movies_updates_copy')
    updater = Updater()
    updater.update_movies(db, multiplier_criticker=1, multiplier_omdb=1, weibull_lambda=10000)
    # TODO: change this unit test to test get_update_sequence, assert timings of next updates
    pass


def test_updatersdf():
    db = MySQLDatabase()
    updater = Updater()
    updater.get_movies_stats(db)
    crit_update1, omdb_update1 = updater.calculate_next_updates(db.movies[126642])
    crit_update2, omdb_update2 = updater.calculate_next_updates(db.movies[6839])
    pass