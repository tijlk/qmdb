import arrow
import pytest
import requests
import requests_mock
from bs4 import BeautifulSoup, ResultSet, Tag
from mock import patch

from qmdb.interfaces.criticker import CritickerScraper
from qmdb.test.test_utils import read_file, side_effect, create_test_tables, remove_test_tables
from qmdb.database.database import MySQLDatabase

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
    assert movie_info['my_ratings']['tijl']['rating'] == 93
    assert movie_info['my_ratings']['tijl']['psi'] == pytest.approx(80, 10)
    assert movie_info['trailer_url'] == 'https://www.youtube.com/watch?v=vKQi3bBA1y8'


def test_get_movie_info_no_trailer():
    crit_scraper = CritickerScraper()
    with requests_mock.mock() as m:
        m.get('http://www.criticker.com/film/Daens/',
              text=read_file('test/fixtures/criticker-daens.html'))
        movie_info = crit_scraper.get_movie_info('http://www.criticker.com/film/Daens/')
    assert movie_info['trailer_url'] is None


def test_get_movie_info_no_rating_of_my_own():
    crit_scraper = CritickerScraper()
    with requests_mock.mock() as m:
        m.get('http://www.criticker.com/film/The-Mask/',
              text=read_file('test/fixtures/criticker-the-mask.html'))
        movie_info = crit_scraper.get_movie_info('http://www.criticker.com/film/The-Mask/')
    assert movie_info['my_ratings']['tijl'].get('rating') is None


def test_get_movie_info_no_poster():
    crit_scraper = CritickerScraper()
    with requests_mock.mock() as m:
        m.get('https://www.criticker.com/film/8-Tire-on-the-Ice/',
              text=read_file('test/fixtures/criticker-8-tire-on-the-ice.html'))
        movie_info = crit_scraper.get_movie_info('https://www.criticker.com/film/8-Tire-on-the-Ice/')
    assert movie_info.get('poster_url') is None


def test_get_movie_info_no_votes():
    crit_scraper = CritickerScraper()
    with requests_mock.mock() as m:
        m.get('https://www.criticker.com/film/16-Fathoms-Deep/',
              text=read_file('test/fixtures/criticker-16-fathoms-deep.html'))
        movie_info = crit_scraper.get_movie_info('https://www.criticker.com/film/16-Fathoms-Deep/')
    assert movie_info.get('crit_votes') == 0


def test_get_year_from_movielist_title():
    crit_scraper = CritickerScraper()
    movielist_title = 'The Matrix (1999)'
    assert crit_scraper.get_year_from_movielist_title(movielist_title) == 1999


def test_get_movielist_movie_attributes():
    crit_scraper = CritickerScraper()
    raw_html = read_file('test/fixtures/criticker-normal-movie-in-movie-list.html')
    html_info = BeautifulSoup(raw_html, "lxml").find('li')
    movie_info = crit_scraper.get_movielist_movie_attributes(html_info)
    assert set(movie_info.keys()) == {'crit_id', 'crit_url', 'title', 'year', 'date_added', 'my_ratings'}
    assert movie_info['my_ratings']['tijl'] == {'psi': 55}
    assert movie_info['crit_id'] == 26496
    assert movie_info['crit_url'] == 'https://www.criticker.com/film/Issiz-adam/'
    assert movie_info['title'] == 'Issiz adam'
    assert movie_info['year'] == 2008
    assert 'date_added' in movie_info
    assert arrow.get(movie_info['date_added']).humanize() == 'just now'

    raw_html = read_file('test/fixtures/criticker-rated-movie-in-movie-list.html')
    html_info = BeautifulSoup(raw_html, "lxml").find('li')
    movie_info = crit_scraper.get_movielist_movie_attributes(html_info)
    assert set(movie_info.keys()) == {'crit_id', 'crit_url', 'title', 'year', 'date_added', 'my_ratings'}
    assert movie_info['my_ratings']['tijl'] == {'rating': 61}

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
@patch.object(MySQLDatabase, 'save_movies')
def test_get_movies(mocker):
    create_test_tables()
    db = MySQLDatabase(schema='qmdb_test', env='test')
    crit_scraper = CritickerScraper()
    crit_scraper.get_movies(db, start_popularity=8)
    save_movies_call_args = db.save_movies.call_args_list[0][0]
    assert save_movies_call_args[0] == [1, 1, 1]
    assert crit_scraper.get_movies_of_popularity.call_args_list[0][1] ==\
           {'debug': False, 'min_year': 2013, 'popularity': 10}
    assert crit_scraper.get_movies_of_popularity.call_args_list[1][1] ==\
           {'debug': False, 'min_year': 2016, 'popularity': 9}
    assert crit_scraper.get_movies_of_popularity.call_args_list[2][1] ==\
           {'debug': False, 'min_year': 2018, 'popularity': 8}
    remove_test_tables(db)