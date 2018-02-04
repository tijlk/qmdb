import json

import arrow
import requests
from requests import ConnectionError

from qmdb.interfaces.interfaces import Scraper
from qmdb.movie.movie import Movie
from bs4 import BeautifulSoup
import re
import time
from qmdb.config import config


class CritickerScraper(Scraper):
    def __init__(self, cookies=config.criticker['cookies']):
        self.cookies = cookies

    def refresh_movie(self, movie):
        super().refresh_movie(movie)
        print("Refreshing Criticker info for '{} ({})'".format(movie.title, movie.year))
        movie_info = self.get_movie_info(movie.crit_url)
        if isinstance(movie_info, dict):
            movie.update_from_dict(movie_info)
        return movie

    def get_movie_info(self, crit_url):
        try:
            r = requests.get(crit_url, cookies=self.cookies)
            time.sleep(1)
        except ConnectionError:
            print("Could not connect to Criticker.")
            return None
        soup = BeautifulSoup(r.text, "lxml")
        imdb_url = soup.find('p', attrs={'class': 'fi_extrainfo', 'id': 'fi_info_ext'}).find('a').get('href')
        imdbid = int(re.match(r'.+tt(\d{7})\/', imdb_url).groups()[0])
        return {'imdbid': imdbid, 'crit_updated': arrow.now()}

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
            time.sleep(1)
        except ConnectionError:
            print("Could not connect to Criticker.")
            return None
        soup = BeautifulSoup(r.text, "lxml")
        movie_list = soup.find('ul', attrs={'class': 'fl_titlelist'})\
                         .find_all('li', attrs={'id': re.compile(r'fl_titlelist_title_\d+')})
        movies = [self.get_movielist_movie_attributes(h, crit_popularity_page=pagenr) for h in movie_list]
        nr_pages_text = str(next(soup.find('p', attrs={'id': 'fl_nav_pagenums_page'}).children))
        nr_pages = int(re.match(r'Page\s+\d+\s+of\s+(\d+)\s*', nr_pages_text).groups()[0])
        return movies, nr_pages

    def get_movies(self, min_popularity=1, debug=False):
        print("Downloading movies from Criticker with a minimum popularity of {}.".format(min_popularity))
        _, nr_pages = self.get_movie_list_page(min_popularity=min_popularity)
        movies = []
        if debug:
            nr_pages = min([2, nr_pages])
        for pagenr in range(1, nr_pages+1):
            print("   Getting page {} of {}".format(pagenr, nr_pages))
            movies_this_page, _ = self.get_movie_list_page(pagenr=pagenr, min_popularity=min_popularity)
            movies += movies_this_page
        return movies