import os

import arrow
import pytest
from mock import patch
import mock

from qmdb.database.database import MySQLDatabase
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.movie.movie import Movie
from qmdb.utils.utils import no_internet
from qmdb.test.test_utils import create_test_tables, remove_test_tables, read_file, side_effect
import requests
import requests_mock
from bs4 import BeautifulSoup, ResultSet, Tag


session = requests.Session()
adapter = requests_mock.Adapter()
session.mount('mock', adapter)


def test_config_cookies():
    crit_scraper = CritickerScraper()
    assert isinstance(crit_scraper.cookies, dict)
    assert 'uid2' in crit_scraper.cookies


def test_get_movie_info():
    crit_scraper = CritickerScraper()
    with requests_mock.mock() as m:
        m.get('http://www.criticker.com/film/The-Matrix/',
              text=read_file('test/fixtures/criticker-the-matrix.html'))
        movie_info = crit_scraper.get_movie_info('http://www.criticker.com/film/The-Matrix/')
    assert movie_info['poster_url'] == 'https://www.criticker.com/img/films/posters/The-Matrix.jpg'
    assert movie_info['imdbid'] == 133093
    assert movie_info['crit_rating'] == pytest.approx(7.71, 0.3)
    assert movie_info['crit_votes'] == pytest.approx(27493, 1000)
    assert movie_info['crit_myratings']['tijl'] == 93
    assert movie_info['crit_mypsis']['tijl'] == pytest.approx(80, 10)
    assert movie_info['trailer_url'] == 'https://www.youtube.com/watch?v=vKQi3bBA1y8'


def test_get_movie_info_no_trailer():
    crit_scraper = CritickerScraper()
    with requests_mock.mock() as m:
        m.get('http://www.criticker.com/film/Daens/',
              text=read_file('test/fixtures/criticker-daens.html'))
        movie_info = crit_scraper.get_movie_info('http://www.criticker.com/film/Daens/')
    assert movie_info['trailer_url'] is None


def test_get_year_from_movielist_title():
    crit_scraper = CritickerScraper()
    movielist_title = 'The Matrix (1999)'
    assert crit_scraper.get_year_from_movielist_title(movielist_title) == 1999


def test_get_movielist_movie_attributes():
    crit_scraper = CritickerScraper()
    raw_html = read_file('test/fixtures/criticker-normal-movie-in-movie-list.html')
    html_info = BeautifulSoup(raw_html, "lxml").find('li')
    movie_info = crit_scraper.get_movielist_movie_attributes(html_info)
    assert set(movie_info.keys()) == {'crit_id', 'crit_url', 'title', 'year', 'date_added', 'crit_mypsis'}
    assert movie_info['crit_mypsis'] == {'tijl': 55}
    assert movie_info['crit_id'] == 26496
    assert movie_info['crit_url'] == 'https://www.criticker.com/film/Issiz-adam/'
    assert movie_info['title'] == 'Issiz adam'
    assert movie_info['year'] == 2008
    assert 'date_added' in movie_info
    assert arrow.get(movie_info['date_added']).humanize() == 'just now'

    raw_html = read_file('test/fixtures/criticker-rated-movie-in-movie-list.html')
    html_info = BeautifulSoup(raw_html, "lxml").find('li')
    movie_info = crit_scraper.get_movielist_movie_attributes(html_info)
    assert set(movie_info.keys()) == {'crit_id', 'crit_url', 'title', 'year', 'date_added', 'crit_myratings'}
    assert movie_info['crit_myratings'] == {'tijl': 61}

    raw_html = read_file('test/fixtures/criticker-nopsi-movie-in-movie-list.html')
    html_info = BeautifulSoup(raw_html, "lxml").find('li')
    movie_info = crit_scraper.get_movielist_movie_attributes(html_info)
    assert set(movie_info.keys()) == {'crit_id', 'crit_url', 'title', 'year', 'date_added'}


def test_get_movie_list_html():
    crit_scraper = CritickerScraper()
    with requests_mock.mock() as m:
        m.get('https://www.criticker.com/films/?filter=or&view=all',
              text=read_file('test/fixtures/criticker-movie-list.html'))
        movie_list, nr_pages = crit_scraper.get_movie_list_html('https://www.criticker.com/films/?filter=or&view=all')
    assert nr_pages == 2283
    assert len(movie_list) == 60
    assert isinstance(movie_list, ResultSet)
    assert isinstance(movie_list[0], Tag)


@patch.object(CritickerScraper, 'get_movie_list_html', new=side_effect(lambda x: ([1, 2, 3, 4, 154, 1000], 6)))
@patch.object(CritickerScraper, 'get_movielist_movie_attributes', new=side_effect(lambda x, **kwargs: {'crit_id': x}))
def test_get_movie_list_popularity_page(mocker):
    crit_scraper = CritickerScraper()
    movies, nr_pages = crit_scraper.get_movie_list_page('https://www.criticker.com/films/?filter=n9zp9zf2000zor&p=1',
                                                        pagenr=1, popularity=9)
    assert nr_pages == 6
    assert len(movies) == 5
    assert movies[0] == {'crit_id': 1}


@patch.object(CritickerScraper, 'get_movie_list_popularity_page', new=side_effect(lambda **kwargs: ([1, 2, 3], 3)))
def test_get_movies_of_popularity(mocker):
    crit_scraper = CritickerScraper()
    movies = crit_scraper.get_movies_of_popularity(popularity=8, min_year=2000)
    assert movies == [1, 2, 3, 1, 2, 3, 1, 2, 3]
    assert crit_scraper.get_movie_list_popularity_page.call_count == 4
    assert crit_scraper.get_movie_list_popularity_page.call_args_list[0][1] == {'min_year': 2000, 'popularity': 8}
    assert crit_scraper.get_movie_list_popularity_page.call_args_list[1][1] == \
           {'min_year': 2000, 'popularity': 8, 'pagenr': 1}


def test_fibonacci():
    crit_scraper = CritickerScraper()
    assert crit_scraper.fibonacci(0) == 0
    assert crit_scraper.fibonacci(1) == 1
    assert crit_scraper.fibonacci(2) == 1
    assert crit_scraper.fibonacci(3) == 2
    assert crit_scraper.fibonacci(5) == 5


@patch.object(arrow, 'now', new=side_effect(lambda: arrow.get('2018-01-01')))
@patch.object(CritickerScraper, 'get_movies_of_popularity', new=side_effect(lambda **kwargs: [1]))
@patch.object(CritickerScraper, 'save_movies')
def test_get_movies(mocker):
    crit_scraper = CritickerScraper()
    crit_scraper.get_movies('db', start_popularity=8)
    save_movies_call_args = crit_scraper.save_movies.call_args_list[0][0]
    assert save_movies_call_args[0] == 'db'
    assert save_movies_call_args[1] == [1, 1, 1]
    assert crit_scraper.get_movies_of_popularity.call_args_list[0][1] ==\
           {'debug': False, 'min_year': 2013, 'popularity': 10}
    assert crit_scraper.get_movies_of_popularity.call_args_list[1][1] ==\
           {'debug': False, 'min_year': 2016, 'popularity': 9}
    assert crit_scraper.get_movies_of_popularity.call_args_list[2][1] ==\
           {'debug': False, 'min_year': 2018, 'popularity': 8}


#
#
# @pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
# def test_get_criticker_no_trailer():
#     # No trailer
#     crit_scraper = CritickerScraper()
#     crit_url = 'https://www.criticker.com/film/Daens/'
#     movie_info = crit_scraper.get_movie_info(crit_url)
#     assert isinstance(movie_info, dict)
#     assert 'imdbid' in movie_info
#     assert movie_info['trailer_url'] is None
#
#
# @pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
# def test_get_criticker_no_rating_of_my_own():
#     # No rating of my own
#     crit_scraper = CritickerScraper()
#     crit_url = 'https://www.criticker.com/film/The-Mask/'
#     movie_info = crit_scraper.get_movie_info(crit_url)
#     assert isinstance(movie_info, dict)
#     assert 'imdbid' in movie_info
#
#
# @pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
# def test_get_criticker_no_poster():
#     # No poster
#     crit_scraper = CritickerScraper()
#     crit_url = 'https://www.criticker.com/film/8-Tire-on-the-Ice/'
#     movie_info = crit_scraper.get_movie_info(crit_url)
#     assert isinstance(movie_info, dict)
#     assert 'imdbid' in movie_info
#     assert movie_info['poster_url'] is None
#
#
# @pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
# def test_get_criticker_no_votes():
#     crit_scraper = CritickerScraper()
#     crit_url = 'https://www.criticker.com/film/16-Fathoms-Deep/'
#     movie_info = crit_scraper.get_movie_info(crit_url)
#     assert isinstance(movie_info, dict)
#     assert 'imdbid' in movie_info
#     assert movie_info['crit_votes'] == 0
#
#
# def test_criticker_refresh_movie(mocker):
#     # Test valid movie info
#     movie = Movie({'crit_id': 123, 'crit_url': 'http://blahblah'})
#     crit_scraper = CritickerScraper()
#     mocker.patch.object(crit_scraper, 'get_movie_info', lambda *args: {'imdbid': 123456, 'crit_updated': arrow.now()})
#     movie = crit_scraper.refresh_movie(movie)
#     assert movie.imdbid == 123456
#     # Test invalid movie info
#     mocker.patch.object(crit_scraper, 'get_movie_info', lambda *args: 1234)
#     movie = crit_scraper.refresh_movie(movie)
#     assert movie is None
#
#
# @pytest.mark.skipif(no_internet(), reason='There is no internet connection.')
# def test_add_criticker_movies_to_db():
#     start_popularity = 8
#     criticker_scraper = CritickerScraper()
#     movies = criticker_scraper.get_movies(start_popularity=start_popularity, debug=True)
#     db = MySQLDatabase(schema='qmdb_test', from_scratch=True)
#     for movie in movies:
#         db.set_movie(Movie(movie))
#     db = MySQLDatabase(schema='qmdb_test')
#     assert 98304 in db.movies
#     assert db.movies[98304].title == 'Interstellar'
#     assert db.movies[98304].year == 2014
#     assert db.movies[98304].crit_url == 'https://www.criticker.com/film/Interstellar/'
#     remove_test_tables(db)
#
