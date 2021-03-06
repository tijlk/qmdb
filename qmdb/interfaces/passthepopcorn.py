import json

import arrow
import requests
from requests import ConnectionError

from qmdb.config import config
from qmdb.interfaces.interfaces import Scraper


class PassThePopcornScraper(Scraper):
    def __init__(self, conf=config.passthepopcorn):
        self.username = conf['username']
        self.password = conf['password']
        self.passkey = conf['passkey']
        self.session = requests.Session()
        self.session_started = False

    def create_session(self):
        self.session.post("https://passthepopcorn.me/ajax.php?action=login",
                          data={"username": self.username, "password": self.password, "passkey": self.passkey,
                                "keeplogged": "0", "login": "Login"}, allow_redirects=False)

    def session_check(self):
        if not self.session_started:
            self.create_session()
            self.session_started = True

    def refresh_movie(self, movie):
        super().refresh_movie(movie)
        movie_info = self.get_movie_info(movie.imdbid)
        if isinstance(movie_info, dict):
            movie_info['crit_id'] = movie.crit_id
            movie_info['ptp_updated'] = arrow.now()
            movie.update_from_dict(movie_info)
            return movie
        else:
            return None

    def get_ptp_request(self, url):
        self.session_check()
        r = self.session.get(url)
        if r.status_code != 200:
            raise Exception("ERROR {}. Something went wrong with the request to PassThePopcorn".format(r.status_code))
        if len(r.history) > 0:
            raise SessionLoggedOutError
        return r

    def get_movie_info(self, imdbid):
        try:
            r = self.get_ptp_request('https://passthepopcorn.me/torrents.php?searchstr=tt{}&json=noredirect'.format(imdbid))
        except SessionLoggedOutError:
            self.create_session()
            try:
                r = self.get_ptp_request('https://passthepopcorn.me/torrents.php?searchstr=tt{}&json=noredirect'.format(imdbid))
            except ConnectionError:
                return None
        except ConnectionError:
            return None
        j = json.loads(r.text)
        try:
            ptp_movie = j['Movies'][0]
        except IndexError:
            return {'ptp_url': None,
                    'ptp_hd_available': False}
        try:
            torrents = ptp_movie['Torrents']
            hd_torrents = [t for t in torrents if t['Quality'] in ('High Definition', 'Ultra High Definition')]
            movie_info = {'ptp_url': 'https://passthepopcorn.me/torrents.php?id={}'.format(ptp_movie['GroupId']),
                          'ptp_hd_available': len(hd_torrents) >= 1}
        except:
            raise Exception
        return movie_info


class SessionLoggedOutError(Exception):
    def __init__(self):
        pass
