import json

import arrow
import requests
from requests import ConnectionError

from qmdb.interfaces.interfaces import Scraper


class OMDBScraper(Scraper):

    def refresh_movie(self, movie):
        super().refresh_movie(movie)
        try:
            tomato_url = self.imdbid_to_rturl(movie.imdbid)
        except InvalidIMDbIdError:
            return None
        if tomato_url and len(tomato_url) > 0:
            movie_info = {'tomato_url': tomato_url, 'omdb_updated': arrow.now()}
            movie.update_from_dict(movie_info)
        return movie

    @staticmethod
    def imdbid_to_rturl(imdbid, apikey='de76d779'):
        if imdbid is None:
            print("No IMDB id known for this movie. So I can't get a Rotten Tomatoes URL for it.")
            raise InvalidIMDbIdError
        imdbid_str = str(imdbid).zfill(7)
        omdb_url = 'http://www.omdbapi.com/?i=tt' + imdbid_str + '&tomatoes=true&apikey={}'.format(apikey)
        try:
            r = requests.get(omdb_url)
            jsonized = json.loads(r.text)
            if jsonized.get('Response') == 'False':
                raise InvalidIMDbIdError
            else:
                return json.loads(r.text).get('tomatoURL')
        except (ConnectionError, json.decoder.JSONDecodeError) as e:
            print("Could not connect to OMDB.")
            return None


class InvalidIMDbIdError(Exception):
    def __init__(self):
        pass
