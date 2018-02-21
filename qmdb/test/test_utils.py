from qmdb.database.database import MySQLDatabase
import arrow


def create_test_tables(variant='normal'):
    db = MySQLDatabase(schema='qmdb_test', from_scratch=True)
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
    else:
        movies_records = []
    for rec in movies_records:
        db.update_single_record('movies', rec)


def remove_test_tables(db):
    for tbl in ['countries', 'genres', 'keywords', 'languages', 'movies', 'persons', 'taglines', 'vote_details']:
        db.remove_table(table_name=tbl)
