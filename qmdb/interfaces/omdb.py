import json

import arrow
import requests
from requests import ConnectionError

from qmdb.interfaces.interfaces import Scraper


class OMDBScraper(Scraper):

    def refresh_movie(self, movie):
        super().refresh_movie(movie)
        tomato_url = self.imdbid_to_rturl(movie.imdbid)
        if tomato_url and len(tomato_url) > 0:
            movie.tomato_url = tomato_url
            movie.omdb_updated = arrow.now()
        return movie

    @staticmethod
    def imdbid_to_rturl(imdbid):
        imdbid_str = str(imdbid).zfill(7)
        omdb_url = 'http://www.omdbapi.com/?i=tt' + imdbid_str + '&tomatoes=true'
        try:
            r = requests.get(omdb_url)
            return json.loads(r.text)['tomatoURL']
        except ConnectionError:
            print("Could not connect to OMDB.")
            return None
