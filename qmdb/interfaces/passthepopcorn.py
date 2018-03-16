from qmdb.config import config
import requests
import json
from json import JSONDecodeError
from qmdb.interfaces.interfaces import Scraper
import arrow


class PassThePopcornScraper(Scraper):
    def __init__(self, conf=config.passthepopcorn):
        self.username = conf['username']
        self.password = conf['password']
        self.passkey = conf['passkey']
        self.session = requests.Session()
        self.create_session()

    def create_session(self):
        self.session.post("https://passthepopcorn.me/ajax.php?action=login",
                               data={"username": self.username, "password": self.password, "passkey": self.passkey,
                                     "keeplogged": "0", "login": "Login"}, allow_redirects=False)

    def refresh_movie(self, movie):
        super().refresh_movie(movie)
        movie_info = self.get_movie_info(movie.imdbid)
        movie_info['crit_id'] = movie.crit_id
        if isinstance(movie_info, dict):
            movie_info['ptp_updated']: arrow.now()
            movie.update_from_dict(movie_info)
            return movie
        else:
            return None

    def get_ptp_request(self, url):
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
            r = self.get_ptp_request('https://passthepopcorn.me/torrents.php?searchstr=tt{}&json=noredirect'.format(imdbid))
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
