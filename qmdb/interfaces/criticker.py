import re
import time

import arrow
import requests
from bs4 import BeautifulSoup
from requests import ConnectionError

from qmdb.config import config
from qmdb.interfaces.interfaces import Scraper


banned_movies = {154: 'Apocalypse Now Redux'}


class CritickerScraper(Scraper):
    def __init__(self, cookies=config.criticker['cookies']):
        self.cookies = cookies

    def refresh_movie(self, movie):
        super().refresh_movie(movie)
        movie_info = self.get_movie_info(movie.crit_url)
        if isinstance(movie_info, dict):
            movie.update_from_dict(movie_info)
            return movie
        else:
            return None

    def get_movie_info(self, crit_url):
        try:
            r = requests.get(crit_url, cookies=self.cookies)
            time.sleep(1)
        except ConnectionError:
            print("Could not connect to Criticker or criticker URL invalid.")
            return None
        soup = BeautifulSoup(r.text, "lxml")
        try:
            poster_url = soup.find('meta', attrs={'itemprop': 'image'}).get('content')
            if poster_url == '':
                poster_url = None
        except AttributeError:
            poster_url = None
        try:
            my_rating = int(soup.find('div', attrs={'class': 'fi_score_div'}).text)
        except AttributeError:
            my_rating = None
        try:
            trailer_url_id = re.search(r'.*youtube.*\/([a-zA-Z0-9]+)$',
                                       soup.find('div', attrs={'id': 'fi_trailer'})
                                           .find('iframe').get('src')).groups()[0]
        except AttributeError:
            trailer_url = None
        else:
            trailer_url = 'https://www.youtube.com/watch?v={}'.format(trailer_url_id)
        try:
            imdb_url = soup.find('p', attrs={'class': 'fi_extrainfo', 'id': 'fi_info_ext'}).find('a').get('href')
        except AttributeError:
            imdbid = None
        else:
            imdbid = int(re.match(r'.+tt(\d{7})\/', imdb_url).groups()[0])
        try:
            crit_rating = float(soup.find('span', attrs={'itemprop': 'ratingValue'}).text)
        except AttributeError:
            crit_rating = None
        try:
            crit_votes = int(soup.find('span', attrs={'itemprop': 'reviewCount'}).text)
        except AttributeError:
            crit_votes = 0

        return {'imdbid': imdbid,
                'criticker_updated': arrow.now(),
                'poster_url': poster_url,
                'my_rating': my_rating,
                'trailer_url': trailer_url,
                'crit_rating': crit_rating,
                'crit_votes': crit_votes}

    @staticmethod
    def get_year_from_movielist_title(title):
        """
        Gets the year of release from the title shown in the Criticker movie list
        :param title: title shown in the Criticker movie list (str)
        :return: the year of release (int)
        """
        match = re.match(r'.*\s+\((\d+)\)', title)
        year = int(match.groups()[0])
        return year

    def get_movielist_movie_attributes(self, movie_html, crit_popularity_page=None):
        """
        Gets some attributes from the Criticker html for a single movie in a movie list
        :param movie_html: a BeautifulSoup object containing the basic info for a single movie
        :return: a dictionary with the url to the movie, the criticker ID, the title and year of release
        """
        a = movie_html.find('a')
        url = a.get('href')
        id = int(movie_html.find('div', attrs={'class': 'fl_titlelist_score'}).get('titleid'))
        title = a.get('title')
        year = self.get_year_from_movielist_title(a.text)
        movie_info = {'crit_id': id,
                      'crit_popularity_page': crit_popularity_page,
                      'crit_url': url,
                      'title': title,
                      'year': year,
                      'date_added': arrow.now()}
        return movie_info

    def get_movie_list_page(self, pagenr=1, min_popularity=1):
        criticker_url = 'https://www.criticker.com/films/?filter=n{}zor&p={}'.format(min_popularity, pagenr)
        try:
            r = requests.get(criticker_url)
            time.sleep(2)
        except ConnectionError:
            print("Could not connect to Criticker.")
            return None
        soup = BeautifulSoup(r.text, "lxml")
        try:
            movie_list = soup.find('ul', attrs={'class': 'fl_titlelist'})\
                         .find_all('li', attrs={'id': re.compile(r'fl_titlelist_title_\d+')})
        except AttributeError:
            print("Couldn't process movies on page {}.".format(pagenr))
            return [], None
        movies = [self.get_movielist_movie_attributes(h, crit_popularity_page=pagenr) for h in movie_list]
        movies = [movie for movie in movies if movie['crit_id'] not in banned_movies.keys()]
        nr_pages_text = str(next(soup.find('p', attrs={'id': 'fl_nav_pagenums_page'}).children))
        nr_pages = int(re.match(r'Page\s+\d+\s+of\s+(\d+)\s*', nr_pages_text).groups()[0])
        return movies, nr_pages

    def get_movies(self, min_popularity=1, debug=False, max_pages=2):
        print("Downloading movies from Criticker with a minimum popularity of {}.".format(min_popularity))
        _, nr_pages = self.get_movie_list_page(min_popularity=min_popularity)
        movies = []
        if debug:
            nr_pages = min([max_pages, nr_pages])
        for pagenr in range(1, nr_pages+1):
            print("   Getting page {} of {}".format(pagenr, nr_pages))
            movies_this_page, _ = self.get_movie_list_page(pagenr=pagenr, min_popularity=min_popularity)
            movies += movies_this_page
        return movies