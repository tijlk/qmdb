from qmdb.database.database import MySQLDatabase
import arrow
import mock
import pickle


def create_test_tables(variant='normal', env='tst'):
    db = MySQLDatabase(schema='qmdb_test', from_scratch=True, env=env)
    if variant == 'normal':
        movies_records = [{'crit_id': 1234,
                           'crit_popularity': 10,
                           'crit_url': 'http://www.criticker.com/film/The-Matrix/',
                           'title': 'The Matrix',
                           'year': 1999,
                           'imdbid': 133093,
                           'date_added': arrow.get('2018-02-04 23:01:58+01:00')},
                          {'crit_id': 49141,
                           'crit_popularity': 1,
                           'crit_url': 'http://www.criticker.com/film/Inception/',
                           'title': 'Inception',
                           'year': 2010,
                           'date_added': arrow.get('2018-02-04 23:01:58+01:00')}]
        languages_records = [{'crit_id': 1234,
                              'language': 'English',
                              'rank': 1},
                             {'crit_id': 1234,
                              'language': 'French',
                              'rank': 2},
                             {'crit_id': 49141,
                              'language': 'English',
                              'rank': 1}]
        persons_records = [{'crit_id': 1234,
                            'role': 'cast',
                            'name': 'Tom Cruise',
                            'rank': 2,
                            'canonical_name': 'Cruise, Tom',
                            'person_id': 13},
                           {'crit_id': 1234,
                            'role': 'director',
                            'name': 'J.J. Abrams',
                            'rank': 2,
                            'canonical_name': 'Abrams, J.J.',
                            'person_id': 14},
                           {'crit_id': 49141,
                            'role': 'director',
                            'name': 'Steven Spielberg',
                            'rank': 1,
                            'canonical_name': 'Spielberg, Steven',
                            'person_id': 15},
                           {'crit_id': 1234,
                            'role': 'cast',
                            'name': 'Anthony Hopkins',
                            'rank': 1,
                            'canonical_name': 'Hopkins, Anthony',
                            'person_id': 16},
                           {'crit_id': 1234,
                            'role': 'director',
                            'name': 'Lana Wachowski',
                            'rank': 1,
                            'canonical_name': 'Wachowski',
                            'person_id': 17},
                           {'crit_id': 49141,
                            'role': 'cast',
                            'name': 'Natalie Portman',
                            'rank': 1,
                            'canonical_name': 'Portman, Natalie',
                            'person_id': 18}]
    elif variant == 'updates':
        movies_records = [{'crit_id': 1234,
                           'crit_popularity': 10,
                           'crit_url': 'http://www.criticker.com/film/The-Matrix/',
                           'title': 'The Matrix',
                           'year': 2017,
                           'imdbid': 133093,
                           'date_added': arrow.get('2018-01-01 00:00:00+01:00')},
                          {'crit_id': 2345,
                           'crit_popularity': 0,
                           'crit_url': 'http://blahblah',
                           'title': 'This Is A Movie',
                           'year': 1930,
                           'imdbid': 123456,
                           'date_added': arrow.get('2018-01-01 00:00:00+01:00')},
                          {'crit_id': 4567,
                           'crit_popularity': 10,
                           'crit_url': 'http://blahblah2',
                           'title': 'The Social Network',
                           'year': 2017,
                           'imdbid': 456789,
                           'tomato_url': 'http://rottentomatoes',
                           'date_added': arrow.get('2018-01-01 00:00:00+01:00'),
                           'omdb_updated': arrow.get('2018-01-01 00:00:00+01:00'),
                           'criticker_updated': arrow.get('2018-01-01 00:00:00+01:00')},
                          {'crit_id': 5678,
                           'crit_popularity': 5,
                           'crit_url': 'http://blahblah3',
                           'title': '2001: A Space Odyssey',
                           'year': 2000,
                           'imdbid': 567890,
                           'tomato_url': 'http://rottentomatoes2',
                           'date_added': arrow.get('2018-01-01 00:00:00+01:00'),
                           'omdb_updated': arrow.get('2018-01-01 00:00:00+01:00'),
                           'criticker_updated': arrow.get('2018-01-01 00:00:00+01:00')},
                          {'crit_id': 6789,
                           'crit_popularity': 0,
                           'crit_url': 'http://blahblah4',
                           'title': 'Metropolis',
                           'year': 1930,
                           'imdbid': 678901,
                           'tomato_url': 'http://rottentomatoes3',
                           'date_added': arrow.get('2018-01-01 00:00:00+01:00'),
                           'omdb_updated': arrow.get('2018-01-01 00:00:00+01:00'),
                           'criticker_updated': arrow.get('2018-01-01 00:00:00+01:00')},
                          {'crit_id': 49141,
                           'crit_popularity': 5,
                           'crit_url': 'http://www.criticker.com/film/Inception/',
                           'title': 'Inception',
                           'year': 2000,
                           'date_added': arrow.get('2018-01-01 00:00:00+01:00')}]
        languages_records = []
        persons_records = []
    else:
        movies_records = []
        languages_records = []
        persons_records = []
    for rec in movies_records:
        db.update_single_record('movies', rec)
    for rec in languages_records:
        db.update_single_record('languages', rec)
    for rec in persons_records:
        db.update_single_record('persons', rec)


def remove_test_tables(db):
    for tbl in ['countries', 'genres', 'keywords', 'languages', 'movies', 'persons', 'taglines', 'vote_details']:
        db.remove_table(table_name=tbl)


def read_file(file):
    with open(file, 'r') as myfile:
        data = myfile.read().replace('\n', ' ')
        return data


def side_effect(fn):
    return mock.MagicMock(side_effect=fn)


def save_obj(obj, name, path='test/fixtures/', protocol=pickle.HIGHEST_PROTOCOL):
    with open(path + name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, protocol)


def load_obj(name, path='test/fixtures/'):
    with open(path + name + '.pkl', 'rb') as f:
        return pickle.load(f)