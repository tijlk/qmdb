import re
import arrow
import requests
from bs4 import BeautifulSoup
from requests import ConnectionError

from qmdb.config import config
from qmdb.interfaces.interfaces import Scraper
from imdb import IMDb, utils
from time import time
import numpy as np
from datetime import date


class IMDBScraper(Scraper):
    def __init__(self):
        self.ia = IMDb()

    def refresh_movie(self, movie, infoset='main'):
        super().refresh_movie(movie)
        imdbid = '%07d' % int(movie.imdbid)
        if isinstance(infoset, str):
            infoset = [infoset]
        for info in infoset:
            get_info = getattr(self, 'process_' + info + '_info')
            movie_info = get_info(imdbid)
            if isinstance(movie_info, dict):
                movie.update_from_dict(movie_info)
        return movie

    @staticmethod
    def person_to_dict(person):
        try:
            person_dict = {'canonical_name': utils.canonicalName(person.data['name']),
                           'name': utils.normalizeName(person.data['name']),
                           'person_id': int(person.personID)}
        except:
            person_dict = None
        return person_dict

    @staticmethod
    def remove_duplicate_dicts(l):
        list_of_tuples = [tuple(d.items()) for d in l]
        list_of_tuples = sorted(set(list_of_tuples), key=list_of_tuples.index)
        return [dict(t) for t in list_of_tuples]

    def process_main_info(self, imdbid):
        try:
            main_info = self.ia.get_movie_main(imdbid)['data']
        except:
            print("ERROR: Could not download main information from IMDb.")
            return None
        info = dict()
        info['imdb_title'] = main_info.get('title')
        info['imdb_year'] = main_info.get('year')
        info['kind'] = main_info.get('kind')
        cast = main_info.get('cast')
        if cast is not None:
            info['cast'] = [self.person_to_dict(person) for person in cast]
            info['cast'] = [e for e in info['cast'] if e is not None]
            info['cast'] = self.remove_duplicate_dicts(info['cast'])
        directors = main_info.get('director')
        if directors is not None:
            info['director'] = [self.person_to_dict(person) for person in directors]
            info['director'] = [e for e in info['director'] if e is not None]
            info['director'] = self.remove_duplicate_dicts(info['director'])
        writers = main_info.get('writer')
        if writers is not None:
            info['writer'] = [self.person_to_dict(person) for person in writers]
            info['writer'] = [e for e in info['writer'] if e is not None]
            info['writer'] = self.remove_duplicate_dicts(info['writer'])
        info['genres'] = None if main_info.get('genres') is None else sorted(list(set(main_info.get('genres'))))
        runtimes = main_info.get('runtimes')
        if runtimes is not None:
            info['runtime'] = int(round(np.median([int(runtime) for runtime in main_info['runtimes']])))
        info['countries'] = None if main_info.get('countries') is None else main_info.get('countries')
        info['imdb_rating'] = main_info.get('rating')
        info['imdb_votes'] = self.parse_imdb_votes(main_info.get('votes'))
        info['plot_storyline'] = main_info.get('plot outline')
        info['languages'] = None if main_info.get('languages') is None else main_info.get('languages')
        info['imdb_main_updated'] = arrow.now()
        return info

    @staticmethod
    def parse_imdb_votes(votes):
        try:
            votes = int(votes)
        except TypeError:
            votes = None
        return votes

    @staticmethod
    def process_release_date(reldate_str):
        results = re.search(r'(.*)::([a-zA-Z0-9 ]*)(([\n]*.*?\((.*?)\))+([\n]*.*?\((.*?)\))+|([\n]*.*?\((.*?)\))+|.*)',
                            reldate_str).groups()
        country = results[0]
        date_str = results[1].strip()
        try:
            date_value = arrow.get(date_str, 'D MMMM YYYY')
        except arrow.parser.ParserError:
            date_value = arrow.get(date_str, 'MMMM YYYY')
        tags = [tag for tag in [results[4], results[6], results[8]] if tag not in (None, '')]
        if len(tags) == 0:
            tags = None
        return {'country': country,
                'date': date_value,
                'tags': tags}

    def get_release_date(self, release_data):
        releases = [self.process_release_date(reldate) for reldate in release_data]
        dutch_release_dates = [release for release in releases if release['country'] == 'Netherlands']
        if len(dutch_release_dates) > 0:
            normal_dutch_release_dates = [release for release in dutch_release_dates if release['tags'] is None]
            limited_dutch_release_dates = [release for release in dutch_release_dates
                                           if release['tags'] is not None and 'limited' in release['tags']]
            if len(normal_dutch_release_dates) > 0:
                dutch_release_date = min([release['date'] for release in normal_dutch_release_dates])
            elif len(limited_dutch_release_dates) > 0:
                dutch_release_date = min([release['date'] for release in limited_dutch_release_dates])
            else:
                dutch_release_date = min([release['date'] for release in dutch_release_dates])
        else:
            dutch_release_date = None
        if len(releases) > 0:
            normal_release_dates = [release for release in releases if release['tags'] is None]
            limited_release_dates = [release for release in releases
                                     if release['tags'] is not None and 'limited' in release['tags']]
            if len(normal_release_dates) > 0:
                original_release_date = min([release['date'] for release in normal_release_dates])
            elif len(limited_release_dates) > 0:
                original_release_date = min([release['date'] for release in limited_release_dates])
            else:
                original_release_date = min([release['date'] for release in releases])
        else:
            original_release_date = None
        return original_release_date, dutch_release_date

    def process_release_info(self, imdbid):
        try:
            release_info = self.ia.get_movie_release_dates(imdbid)['data']
        except:
            print("ERROR: Could not download release information from IMDb.")
            return None
        info = dict()
        original_release_date, dutch_release_date = self.get_release_date(release_info['release dates'])
        info['original_release_date'] = original_release_date
        info['dutch_release_date'] = dutch_release_date
        if 'akas from release info' in release_info:
            titles = [self.process_title(title_str) for title_str in release_info['akas from release info']]
            original_titles = [title for title in titles if title['tag'] == 'original title']
            if len(original_titles) > 0:
                info['original_title'] = sorted([title_dict['title'] for title_dict in original_titles])[0]
            else:
                info['original_title'] = None
        else:
            info['original_title'] = None
        info['imdb_release_updated'] = arrow.now()
        return info

    @staticmethod
    def process_title(title_str):
        results = re.search(r'(.*?)(\s*\((.*?)\))*::(.*)',
                            title_str).groups()
        country = results[0]
        tag = results[2]
        title = results[3]
        return {'country': country,
                'title': title,
                'tag': tag}

    def process_metacritic_info(self, imdbid):
        try:
            metacritic_info = self.ia.get_movie_critic_reviews(imdbid)['data']
        except:
            print("ERROR: Could not download metacritic information from IMDb.")
            return None
        info = dict()
        try:
            info['metacritic_score'] = int(metacritic_info['metascore'])
        except KeyError:
            info['metacritic_score'] = None
        info['imdb_metacritic_updated'] = arrow.now()
        return info

    def process_keywords_info(self, imdbid):
        try:
            keywords_info = self.ia.get_movie_keywords(imdbid)['data']
        except:
            print("ERROR: Could not download keywords information from IMDb.")
            return None
        info = dict()
        info['keywords'] = list(set(keywords_info['keywords']))
        info['imdb_keywords_updated'] = arrow.now()
        return info

    def process_taglines_info(self, imdbid):
        try:
            taglines_info = self.ia.get_movie_taglines(imdbid)['data']
        except:
            print("ERROR: Could not download taglines information from IMDb.")
            return None
        info = dict()
        info['taglines'] = [tagline for tagline in taglines_info['taglines']]
        info['imdb_taglines_updated'] = arrow.now()
        return info

    def process_vote_details_info(self, imdbid):
        try:
            vote_details = self.ia.get_movie_vote_details(imdbid)['data']
        except:
            print("ERROR: Could not download vote_details information from IMDb.")
            return None
        info = dict()
        info['vote_details'] = vote_details['demographics']
        info['imdb_vote_details_updated'] = arrow.now()
        return info

    def process_plot_info(self, imdbid):
        try:
            plot_info = self.ia.get_movie_plot(imdbid)['data']
        except:
            print("ERROR: Could not download plot information from IMDb.")
            return None
        info = dict()
        info['plot_summary'] = min(plot_info.get('plot'), key=len)
        info['imdb_plot_updated'] = arrow.now()
        return info
