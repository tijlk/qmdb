import requests
import json
from qmdb.interfaces.interfaces import Scraper
import arrow


class OMDBScraper(Scraper):

    def refresh_movie(self, movie):
        if movie.imdbid:
            tomato_url = self.imdbid_to_rturl(movie.imdbid)
            if len(tomato_url) > 0:
                movie.tomato_url = tomato_url
                movie.omdb_updated = arrow.now()
        return movie

    @staticmethod
    def imdbid_to_rturl(imdbid):
        imdbid_str = str(imdbid).zfill(7)
        omdb_url = 'http://www.omdbapi.com/?i=tt' + imdbid_str + '&tomatoes=true'
        r = requests.get(omdb_url)
        tomato_url = json.loads(r.text)['tomatoURL']
        return tomato_url
