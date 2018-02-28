import re
import time

import arrow
import requests
from bs4 import BeautifulSoup
from requests import ConnectionError

from qmdb.config import config
from qmdb.interfaces.interfaces import Scraper
from qmdb.movie.movie import Movie


banned_movies = {154: 'Apocalypse Now Redux',
                 1011: "The Exorcist: The Version You've Never Seen"}


class CritickerScraper(Scraper):
    def __init__(self, config=config.criticker, user='tijl'):
        self.user = user
        self.cookies = config[user]

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
            my_psi = int(soup.find('div', attrs={'class': 'fi_psi_div'}).text)
        except (AttributeError, ValueError):
            try:
                extra_infos = [s.find('span') for s in soup.findAll('p', attrs={'class': 'fi_extrainfo'})
                               if s.find('span') is not None]
                extra_infos = [e for e in extra_infos if 'PSI' in str(e.previous)]
                my_psi = int(extra_infos[0].text)
            except (AttributeError, IndexError):
                my_psi = None
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
            imdbid = int(re.match(r'.+tt(\d{7})\/', imdb_url).groups()[0])
        except AttributeError:
            imdbid = None
        try:
            crit_rating = float(soup.find('span', attrs={'itemprop': 'ratingValue'}).text)
        except AttributeError:
            crit_rating = None
        try:
            crit_votes = int(soup.find('span', attrs={'itemprop': 'reviewCount'}).text)
        except AttributeError:
            crit_votes = 0

        movie_info = {'imdbid': imdbid,
                      'criticker_updated': arrow.now(),
                      'poster_url': poster_url,
                      'trailer_url': trailer_url,
                      'crit_rating': crit_rating,
                      'crit_votes': crit_votes}
        if my_rating is not None:
            movie_info['crit_myratings'] = {self.user: my_rating}
        if my_psi is not None:
            movie_info['crit_mypsis'] = {self.user: my_psi}
        return movie_info

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

    def get_movielist_movie_attributes(self, movie_html, popularity=None, pagenr=None, nr_pages=None):
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
                      'crit_url': url,
                      'title': title,
                      'year': year,
                      'date_added': arrow.now()}
        if popularity is not None and pagenr is not None and nr_pages is not None:
            movie_info.update({'crit_popularity': popularity - (pagenr - 1)/(nr_pages - 1)})
        try:
            psi = {self.user: int(movie_html.find('div', attrs={'class': 'pti'}).text)}
        except AttributeError:
            psi = None
        if psi is not None:
            movie_info.update({'crit_mypsis': psi})
        try:
            rating = {self.user: int(movie_html.find('div', attrs={'title': 'Your Ranking'}).text)}
        except AttributeError:
            rating = None
        if rating is not None:
            movie_info.update({'crit_myratings': rating})
        return movie_info

    def get_movie_list_html(self, url):
        try:
            r = requests.get(url, cookies=self.cookies)
            time.sleep(1)
        except ConnectionError:
            print("Could not connect to Criticker.")
            return None
        soup = BeautifulSoup(r.text, "lxml")
        try:
            movie_list = soup.find('ul', attrs={'class': 'fl_titlelist'})\
                         .find_all('li', attrs={'id': re.compile(r'fl_titlelist_title_\d+')})
        except AttributeError:
            print("Couldn't process movies on url {}.".format(url))
            return [], 0
        try:
            nr_pages_text = str(next(soup.find('p', attrs={'id': 'fl_nav_pagenums_page'}).children))
        except AttributeError:
            return [], 0
        nr_pages = int(re.match(r'Page\s+\d+\s+of\s+(\d+)\s*', nr_pages_text).groups()[0])
        return movie_list, nr_pages

    def get_movie_list_page(self, url, pagenr=None, popularity=None):
        movie_list, nr_pages = self.get_movie_list_html(url)
        movies = [self.get_movielist_movie_attributes(h, popularity=popularity, pagenr=pagenr, nr_pages=nr_pages)
                  for h in movie_list]
        movies = [movie for movie in movies if movie['crit_id'] not in banned_movies.keys()]
        return movies, nr_pages

    def get_movie_list_popularity_page(self, pagenr=1, popularity=10, min_year=1):
        criticker_url = 'https://www.criticker.com/films/?filter=n{}zp{}zf{}zor&p={}'\
            .format(popularity, popularity, min_year, pagenr)
        return self.get_movie_list_page(criticker_url, popularity=popularity, pagenr=pagenr)

    def get_movies_of_popularity(self, popularity=1, min_year=1, debug=False, debug_pages=2):
        print("Downloading movies from Criticker with a minimum popularity of {} starting at the year {}."
              .format(popularity, min_year))
        _, nr_pages = self.get_movie_list_popularity_page(popularity=popularity, min_year=min_year)
        movies = []
        if debug:
            nr_pages = min([debug_pages, nr_pages])
        for pagenr in range(1, nr_pages+1):
            print("   Getting page {} of {}".format(pagenr, nr_pages))
            movies_this_page, _ = self.get_movie_list_popularity_page(pagenr=pagenr, popularity=popularity, min_year=min_year)
            movies += movies_this_page
        return movies

    def fibonacci(self, n):
        if n == 1:
            return 1
        elif n == 0:
            return 0
        else:
            return self.fibonacci(n - 1) + self.fibonacci(n - 2)

    def get_movies(self, start_popularity=1, debug=False):
        year = arrow.now().year + 1
        popularity_year_tuples = []
        for i, popularity in enumerate(range(start_popularity, 11)):
            year -= self.fibonacci(i+2)
            popularity_year_tuples.append((popularity, year))

        movies = []
        for popularity, year in reversed(popularity_year_tuples):
            movies += self.get_movies_of_popularity(popularity=popularity, min_year=year, debug=debug)
        return movies

    def get_ratings_page(self, pagenr=1):
        criticker_url = 'https://www.criticker.com/rankings/?p={}'.format(pagenr)
        return self.get_movie_list_page(criticker_url)

    def get_ratings(self):
        print("Downloading ratings from Criticker...")
        movies, nr_pages = self.get_ratings_page()
        for pagenr in range(2, nr_pages+1):
            print("   Getting page {} of {}".format(pagenr, nr_pages))
            new_movies, _ = self.get_ratings_page(pagenr=pagenr)
            movies += new_movies
        return movies

    def get_criticker_movies(self, db, start_popularity=2):
        movies = self.get_movies(start_popularity=start_popularity)
        print("\nSaving movie information to the database\n")
        for movie_info in movies:
            db.set_movie(movie_info)
        db.print()

    def get_criticker_ratings(self, db):
        ratings = self.get_ratings()
        print("\nSaving rating information to the database\n")
        for movie_info in ratings:
            db.set_movie(movie_info)
        db.print()