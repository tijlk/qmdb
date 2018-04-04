import copy
from datetime import timedelta

import arrow
import requests
from bs4 import BeautifulSoup

from qmdb.config import config


class NetflixScraper:
    def __init__(self, db, conf=config.netflix):
        self.db = db
        self.email = conf['email']
        self.password = conf['password']
        self.mashapekey = conf['mashapekey']
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0',
                        'Accept': 'application/json, text/javascript, */*',
                        'Accept-Language': 'en-GB,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Content-Type': 'application/json',
                        'DNT': '1',
                        'Referer': 'https://www.netflix.com'}
        self.session = requests.Session()
        self.authURL = None
        self.unogs_last_suspension = self.db.unogs_suspension
        if self.unogs_last_suspension is not None \
                and arrow.now() - self.unogs_last_suspension <= timedelta(hours=24):
            self.unogs_requests_remaining = 0
        else:
            self.unogs_requests_remaining = None

    def get_authurl(self):
        r = self.session.get("https://www.netflix.com/nl-en/login")
        soup = BeautifulSoup(r.text, "lxml")
        self.authURL = soup.find("input", attrs={'name': 'authURL'}).attrs['value']

    def create_session(self):
        # TODO: Work in Progress
        r1 = self.session.post("https://signup.netflix.com/Login",
                               data={"email": self.email, "password": self.password, "authURL": self.authURL})
        genres = '0,"to":1'
        rmax = '5'
        base = '[["newarrivals",{"from":' + genres + '},{"from":0,"to":' + rmax + '},["title","availability"]],["newarrivals",{"from":' + genres + '},{"from":0,"to":' + rmax + '},"boxarts","_342x192","jpg"]]';
        data = '{"paths":' + base + '}'
        data = '''{"paths":[["videos",1051852,"similars",{"from":0,"to":25},["synopsis","title","summary","queue","trackId","runtime","interactiveBookmark","seasonCount","releaseYear","userRating","userRatingRequestId","numSeasonsLabel","delivery","maturity","availability"]],["videos",1051852,"similars",{"from":0,"to":25},"boxarts",["_260x146","_342x192"],"webp"],["videos",1051852,"similars",{"from":0,"to":25},"current","summary"],["videos",1051852,"similars",["summary","trackId"]],["videos",1051852,"festivals",{"from":0,"to":10},{"from":0,"to":10},["type","winner"]],["videos",1051852,"festivals",{"from":0,"to":10},{"from":0,"to":10},"person",["name","id"]],["videos",1051852,"festivals",{"from":0,"to":10},["length","name","year"]],["videos",1051852,"festivals","length"],["videos",1051852,["creators","directors"],{"from":0,"to":4},["id","name"]],["videos",1051852,"cast",{"from":11,"to":49},["id","name"]],["videos",1051852,"writers",{"from":0,"to":9},["id","name"]],["videos",1051852,["genres","tags"],"summary"],["videos",1051852,["copyright","availabilityEndDateNear"]],["videos",1051852,"trailers",{"from":0,"to":35},["title","summary","trackId","availability"]],["videos",1051852,"trailers",{"from":0,"to":35},"interestingMoment","_260x146","webp"],["videos",1051852,"trailers",{"from":0,"to":35},"current","summary"],["videos",1051852,"seasonList",{"from":0,"to":20},"summary"],["videos",1051852,"seasonList","summary"],["person",[26660,30411,66706,72948,20005059,20005060,20026376],["id","name"]]],"authURL":"1521229565033.qxsdv7hfBi9KUeq4+XxFHyeqamM="}'''
        url = "https://www.netflix.com/api/shakti/0b1df4a2/pathEvaluator?drmSystem=widevine&isWatchlistEnabled=false" \
              "&isShortformEnabled=false&fetchListAnnotations=false&canWatchBranchingPuss=false&withSize=true" \
              "&materialize=true"
        r2 = self.session.post(url, data=data, headers=self.headers)
        rjson = r2.json()
        pass

    def do_unogs_request(self, url):
        if self.unogs_requests_remaining is not None and self.unogs_requests_remaining < 1:
            raise Exception("No more requests remaining for UNOGS!")
        r = requests.get(url, headers={"X-Mashape-Key": self.mashapekey, "Accept": "application/json"})
        self.unogs_requests_remaining = int(r.headers._store['x-ratelimit-requests-remaining'][1])
        if self.unogs_requests_remaining == 0:
            self.db.unogs_suspension = arrow.now()
            self.unogs_last_suspension = self.db.unogs_suspension
            self.db.set_unogs_suspension()
            raise NoUnogsRequestsRemaining
        return r.json()

    def get_genre_ids(self):
        rjson = self.do_unogs_request("https://unogs-unogs-v1.p.mashape.com/api.cgi?t=genres")
        for d in rjson['ITEMS']:
            genre_name = list(d.keys())[0]
            genreids = d[genre_name]
            for genreid in genreids:
                if genreid not in self.db.netflix_genres:
                    self.db.netflix_genres[genreid] = {'genre_names': [], 'movies_updated': None}
                if genre_name not in self.db.netflix_genres[genreid]['genre_names']:
                    self.db.netflix_genres[genreid]['genre_names'].append(genre_name)
        self.db.set_netflix_genres()

    def unogs_movie_info_to_dict(self, d):
        netflix_id = d.get('netflixid')
        if netflix_id is not None:
            netflix_id = int(netflix_id)
        netflix_title = d.get('title')
        netflix_rating = d.get('rating')
        if netflix_rating is not None:
            netflix_rating = float(netflix_rating)
        imdbid = d.get('imdbid')
        if imdbid is not None and imdbid not in ('', 'notfound'):
            imdbid = int(imdbid[2:])
        else:
            return None
        return {'netflix_id': netflix_id,
                'netflix_title': netflix_title,
                'netflix_rating': netflix_rating,
                'imdbid': imdbid,
                'netflix_updated': arrow.now()}

    def get_critid_from_imdbid(self, imdbid):
        return self.db.imdbid_to_critid.get(imdbid)

    def get_movies_for_genre_page(self, genreid, country_code=67, pagenr=1):
        rjson = self.do_unogs_request(("https://unogs-unogs-v1.p.mashape.com/aaapi.cgi?q={{query}}-!1800,2050-!0,5-!0,10-!{}-!Any-"
                                           "!Any-!Any-!Any-!{{downloadable}}&t=ns&cl={}&st=adv&ob=Relevance&p={}&sa=and")
                                          .format(genreid, country_code, pagenr))
        nr_pages = int(rjson['COUNT']) // 100 + 1
        movies = [self.unogs_movie_info_to_dict(d) for d in rjson['ITEMS']]
        movies = [movie for movie in movies if movie is not None]
        movies_with_critid = []
        for movie in movies:
            crit_id = self.get_critid_from_imdbid(movie['imdbid'])
            if isinstance(crit_id, set):
                for id in crit_id:
                    movie_copy = copy.deepcopy(movie)
                    movie_copy['crit_id'] = id
                    movies_with_critid.append(movie_copy)
            elif isinstance(crit_id, int):
                movie['crit_id'] = crit_id
                movies_with_critid.append(movie)
            else:
                pass
        return nr_pages, movies_with_critid

    def get_movies_for_genre(self, genreid, country_code=67):
        movies = []
        nr_pages, movies_new = self.get_movies_for_genre_page(genreid, country_code=country_code, pagenr=1)
        movies += movies_new
        for pagenr in range(1, nr_pages):
            _, movies_new = self.get_movies_for_genre_page(genreid, country_code=country_code, pagenr=pagenr)
            movies += movies_new
        return movies

    def get_movies_for_genres(self, country_code=67):
        genreids_to_update = [{'genreid': g, 'movies_updated': self.db.netflix_genres[g]['movies_updated']
                               if self.db.netflix_genres[g]['movies_updated'] is not None else arrow.get('2000-01-01')}
                              for g in self.db.netflix_genres if self.db.netflix_genres[g]['movies_updated'] is None
                              or (arrow.now() - self.db.netflix_genres[g]['movies_updated']).days >= 7]
        genreids_to_update = sorted(genreids_to_update, key=lambda k: k['movies_updated'])
        genreids_to_update = [g['genreid'] for g in genreids_to_update]
        for genreid in genreids_to_update:
            print("Getting movies for genreid {} ({})".format(genreid, self.db.netflix_genres[genreid]['genre_names']))
            try:
                movies = self.get_movies_for_genre(genreid, country_code=country_code)
            except NoUnogsRequestsRemaining:
                print("No more requests available for Unogs!")
            else:
                self.db.netflix_genres[genreid]['movies_updated'] = arrow.now()
                self.db.save_movies(movies)
                self.db.set_netflix_genres()


class NoUnogsRequestsRemaining(Exception):
    def __init__(self):
        pass
