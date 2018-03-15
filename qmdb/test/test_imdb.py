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
    assert isinstance(info, dict)
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


def test_parse_imdb_votes():
    imdb_scraper = IMDBScraper()
    assert imdb_scraper.parse_imdb_votes(None) is None
    assert imdb_scraper.parse_imdb_votes('1000') == 1000
    assert imdb_scraper.parse_imdb_votes('(2,345)') == 2345
    assert imdb_scraper.parse_imdb_votes('xdfw2,345)') is None


def test_process_release_date():
    imdb_scraper = IMDBScraper()
    date_dict = imdb_scraper.process_release_date('Czech Republic::5 August 1999')
    assert isinstance(date_dict, dict)
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


def test_get_release_data():
    imdb_scraper = IMDBScraper()
    release_info = load_obj('imdb-release-dates-info-the-matrix')['data']['release dates']
    original_release_date, dutch_release_date = imdb_scraper.get_release_date(release_info)
    assert original_release_date == arrow.get(dt.datetime(1999, 3, 31))
    assert dutch_release_date == arrow.get(dt.datetime(1999, 6, 17))
    release_info = ['USA::24 March 1999 (Westwood, California)\n (premiere)',
                    'USA::28 March 1999 (Westwood, California)\n (limited)',
                    'USA::30 March 1999']
    original_release_date, dutch_release_date = imdb_scraper.get_release_date(release_info)
    assert original_release_date == arrow.get(dt.datetime(1999, 3, 30))
    assert dutch_release_date is None
    release_info = ['Netherlands::4 April 1999 (premiere)',
                    'USA::24 March 1999 (Westwood, California)\n (premiere)',
                    'USA::28 March 1999 (Westwood, California)\n (limited)']
    original_release_date, dutch_release_date = imdb_scraper.get_release_date(release_info)
    assert original_release_date == arrow.get(dt.datetime(1999, 3, 28))
    assert dutch_release_date == arrow.get(dt.datetime(1999, 4, 4))
    release_info = ['Netherlands::4 April 1999 (premiere)',
                    'Netherlands::8 April 1999',
                    'USA::25 March 1999']
    original_release_date, dutch_release_date = imdb_scraper.get_release_date(release_info)
    assert original_release_date == arrow.get(dt.datetime(1999, 3, 25))
    assert dutch_release_date == arrow.get(dt.datetime(1999, 4, 8))
    release_info = ['USA::24 March 1999 (Westwood, California)\n (premiere)',
                    'Bangladesh::27 March 1999',
                    'Netherlands::5 April 1999 (limited)',
                    'Netherlands::2 April 1999 (premiere)']
    original_release_date, dutch_release_date = imdb_scraper.get_release_date(release_info)
    assert original_release_date == arrow.get(dt.datetime(1999, 3, 27))
    assert dutch_release_date == arrow.get(dt.datetime(1999, 4, 5))
    release_info = ['Italy::21 December 1968',
                    'Italy::24 December 1968 (Turin)',
                    'USA::28 May 1969 (New York, New York City)',
                    'USA::4 July 1969',
                    'Netherlands::3 November 2016 (re-release)']
    original_release_date, dutch_release_date = imdb_scraper.get_release_date(release_info)
    assert original_release_date == arrow.get(dt.datetime(1968, 12, 21))
    assert dutch_release_date is None
    release_info = ['Afghanistan::29 March 1999',
                    'Bangladesh::27 March 1999']
    original_release_date, dutch_release_date = imdb_scraper.get_release_date(release_info)
    assert original_release_date == arrow.get(dt.datetime(1999, 3, 27))
    assert dutch_release_date is None
    release_info = None
    original_release_date, dutch_release_date = imdb_scraper.get_release_date(release_info)
    assert original_release_date is None
    assert dutch_release_date is None


@patch.object(IMDbHTTPAccessSystem, 'get_movie_release_dates',
              new=side_effect(lambda x: load_obj('imdb-release-dates-info-the-matrix')))
@patch.object(IMDBScraper, 'get_release_date',
              new=side_effect(lambda x: (arrow.get(dt.datetime(1999, 3, 31)), arrow.get(dt.datetime(1999, 6, 17)))))
def test_process_release_info(mocker):
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_release_info('0133093')
    assert isinstance(info, dict)
    assert info['original_release_date'] == arrow.get(dt.datetime(1999, 3, 31))
    assert info['dutch_release_date'] == arrow.get(dt.datetime(1999, 6, 17))
    assert info['original_title'] is None


def test_process_title():
    imdb_scraper = IMDBScraper()
    assert imdb_scraper.process_title('Argentina::Matrix') == \
           {'country': 'Argentina', 'title': 'Matrix', 'tag': None}
    assert imdb_scraper.process_title('Canada (French title)::La matrice') == \
           {'country': 'Canada', 'title': 'La matrice', 'tag': 'French title'}
    assert imdb_scraper.process_title('Finland (video box title)::Matrix') == \
           {'country': 'Finland', 'title': 'Matrix', 'tag': 'video box title'}


def test_get_english_original_title():
    imdb_scraper = IMDBScraper()
    akas_matrix = ['Argentina::Matrix', 'Belgium (French title)::Matrix', 'Finland (video box title)::Matrix',
                   'Japan (English title)::Matrix', 'Panama (alternative title)::La matriz']
    english_title, original_title = imdb_scraper.get_english_original_title(akas_matrix)
    assert english_title is None
    assert original_title is None
    akas_intouchables = ['(original title)::Intouchables', 'Argentina::Amigos intocables',
                         'Bulgaria (Bulgarian title)::Недосегаемите',
                         'Europe (festival title) (English title)::Untouchable', 'World-wide::The Intouchables']
    english_title, original_title = imdb_scraper.get_english_original_title(akas_intouchables)
    assert english_title == 'The Intouchables'
    assert original_title == 'Intouchables'
    akas_le_fils = ['(original title)::Le fils', 'Argentina::El hijo', 'Bulgaria (Bulgarian title)::Синът',
                    'World-wide (English title)::The Son']
    english_title, original_title = imdb_scraper.get_english_original_title(akas_le_fils)
    assert english_title == 'The Son'
    assert original_title == 'Le fils'
    akas_on_body_and_soul = ['(original title)::Teströl és lélekröl', 'Argentina::En cuerpo y alma',
                             'Bulgaria (Bulgarian title)::За тялото и душата',
                             'World-wide (English title)::On Body and Soul']
    english_title, original_title = imdb_scraper.get_english_original_title(akas_on_body_and_soul)
    assert english_title == 'On Body and Soul'
    assert original_title == 'Teströl és lélekröl'
    akas_the_lift = ['(original title)::De lift', 'Argentina::El ascensor', 'Philippines (English title)::The Lift',
                     'Soviet Union (Russian title)::Лифт', 'USA::The Lift']
    english_title, original_title = imdb_scraper.get_english_original_title(akas_the_lift)
    assert english_title == 'The Lift'
    assert original_title == 'De lift'


@patch.object(IMDbHTTPAccessSystem, 'get_movie_release_dates',
              new=side_effect(lambda x: load_obj('imdb-release-dates-info-the-matrix')))
@patch.object(IMDBScraper, 'get_release_date',
              new=side_effect(lambda x: (arrow.get(dt.datetime(1999, 3, 31)), arrow.get(dt.datetime(1999, 6, 17)))))
@patch.object(IMDBScraper, 'get_english_original_title',
              new=side_effect(lambda x: ('The Intouchables', 'Intouchables')))
def test_process_release_info_with_original_title(mocker):
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_release_info('0133093')
    assert isinstance(info, dict)
    assert info['original_release_date'] == arrow.get(dt.datetime(1999, 3, 31))
    assert info['dutch_release_date'] == arrow.get(dt.datetime(1999, 6, 17))
    assert info['original_title'] == 'Intouchables'
    assert info['english_title'] == 'The Intouchables'


@patch.object(IMDbHTTPAccessSystem, 'get_movie_critic_reviews',
              new=side_effect(lambda x: {'data': {'metacritic url': 'blahblah', 'metascore': '73'}}))
def test_process_metacritic_info_present():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_metacritic_info('0133093')
    assert isinstance(info, dict)
    assert info['metacritic_score'] == 73


@patch.object(IMDbHTTPAccessSystem, 'get_movie_critic_reviews', new=side_effect(lambda x: {'data': {}}))
def test_process_metacritic_info_not_present():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_metacritic_info('0123456')
    assert isinstance(info, dict)
    assert info['metacritic_score'] is None


@patch.object(IMDbHTTPAccessSystem, 'get_movie_keywords', new=side_effect(lambda x: {'data':
              {'keywords': ['artificial-reality', 'artificial-reality', 'post-apocalypse']}}))
def test_process_keywords_present():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_keywords_info('0123456')
    assert isinstance(info, dict)
    assert info['keywords'] == ['artificial-reality', 'post-apocalypse']


@patch.object(IMDbHTTPAccessSystem, 'get_movie_keywords', new=side_effect(lambda x: {'data': {}}))
def test_process_keywords_not_present():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_keywords_info('0123456')
    assert isinstance(info, dict)
    assert info['keywords'] is None


@patch.object(IMDbHTTPAccessSystem, 'get_movie_taglines', new=side_effect(lambda x: {'data':
              {'taglines': ['Free your mind', 'In a world of 1s and 0s...are you a zero, or The One?']}}))
def test_get_movie_info_taglines_present():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_taglines_info('0123456')
    assert isinstance(info, dict)
    assert info['taglines'] == ['Free your mind', 'In a world of 1s and 0s...are you a zero, or The One?']


@patch.object(IMDbHTTPAccessSystem, 'get_movie_taglines', new=side_effect(lambda x: {'data': {}}))
def test_get_movie_info_taglines_present():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_taglines_info('0123456')
    assert isinstance(info, dict)
    assert info['taglines'] is None


@patch.object(IMDbHTTPAccessSystem, 'get_movie_vote_details',
              new=side_effect(lambda x: load_obj('imdb-vote-details-info-the-matrix')))
def test_process_vote_details_info():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_vote_details_info('0133093')
    assert isinstance(info, dict)
    assert info['vote_details']['imdb users'] == {'votes': 1379920, 'rating': 8.7}
    assert info['vote_details']['non us users'] == {'votes': 604641, 'rating': 8.7}


@patch.object(IMDbHTTPAccessSystem, 'get_movie_vote_details', new=side_effect(lambda x: {'data': {}}))
def test_process_vote_details_info():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_vote_details_info('1630029')
    assert isinstance(info, dict)
    assert info['vote_details'] is None


def test_strip_author_from_plot():
    imdb_scraper = IMDBScraper()
    assert imdb_scraper.strip_author_from_plot('This is a plot.::Tijl Kindt') == 'This is a plot.'
    assert imdb_scraper.strip_author_from_plot('This is a plot.') == 'This is a plot.'


@patch.object(IMDbHTTPAccessSystem, 'get_movie_plot', new=side_effect(lambda x: load_obj('imdb-plot-info-the-matrix')))
def test_process_plot_info_present():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_plot_info('0133093')
    assert isinstance(info, dict)
    assert info['plot_summary'] == 'A computer hacker learns from mysterious rebels about the true nature of his ' \
                                   'reality and his role in the war against its controllers.'


@patch.object(IMDbHTTPAccessSystem, 'get_movie_plot', new=side_effect(lambda x: {'data': {}}))
def test_process_plot_info_present():
    imdb_scraper = IMDBScraper()
    info = imdb_scraper.process_plot_info('0133093')
    assert isinstance(info, dict)
    assert info['plot_summary'] is None