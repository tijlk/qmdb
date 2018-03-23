from qmdb.interfaces.criticker import CritickerScraper
from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.imdb import IMDBScraper
from qmdb.interfaces.passthepopcorn import PassThePopcornScraper
from operator import itemgetter
import numpy as np
import arrow
import time
from qmdb.movie.utils import humanized_time
import re
from qmdb.movie.movie import Movie


class Updater(object):
    def __init__(self, **kwargs):
        self.sources = ['criticker', 'omdb', 'imdb_main', 'imdb_release', 'imdb_metacritic', 'imdb_keywords',
                        'imdb_taglines', 'imdb_vote_details', 'imdb_plot', 'ptp']
        self.multipliers = {'base_multiplier': 1,
                            'base_multiplier_criticker': 1,
                            'base_multiplier_omdb': 10,
                            'base_multiplier_imdb_main': 1,
                            'base_multiplier_imdb_release': 10,
                            'base_multiplier_imdb_metacritic': 5,
                            'base_multiplier_imdb_keywords': 10,
                            'base_multiplier_imdb_taglines': 10,
                            'base_multiplier_imdb_vote_details': 2,
                            'base_multiplier_imdb_plot': 10,
                            'base_multiplier_ptp': 1,
                            'firsttime_speedup': 50000}
        self.multipliers.update(kwargs)
        for source in self.sources:
            self.multipliers['multiplier_' + source] = \
                self.multipliers['base_multiplier_' + source] * self.multipliers['base_multiplier']
            self.multipliers['multiplier_' + source + '_firsttime'] = \
                self.multipliers['base_multiplier_' + source] * self.multipliers['base_multiplier'] / \
                self.multipliers['firsttime_speedup']
        self.crit_scraper = CritickerScraper()
        self.omdb_scraper = OMDBScraper()
        self.imdb_scraper = IMDBScraper()
        self.ptp_scraper = PassThePopcornScraper()
        self.years = None
        self.crit_pop = None
        self.earliest_date_added = None
        self.max_connections_per_hour = {'criticker': 400,
                                         'omdb': 40,
                                         'imdb': 800,
                                         'ptp': 400}

    def update_movies(self, db, n=None, weibull_lambda=1.5):
        self.get_movies_stats(db)
        updates = self.get_all_next_updates(db, weibull_lambda=weibull_lambda)
        crit_updates = self.get_source_update_sequence(updates, 'criticker')
        omdb_updates = self.get_source_update_sequence(updates, 'omdb')
        imdb_updates = self.get_source_update_sequence(updates, 'imdb')
        ptp_updates = self.get_source_update_sequence(updates, 'ptp')
        sorted_seq = sorted(crit_updates + omdb_updates + imdb_updates + ptp_updates, key=itemgetter('next_update'))
        if n is not None:
            sources_to_update = sorted_seq[:n]
        else:
            sources_to_update = sorted_seq
        for i, source_to_update in enumerate(sources_to_update):
            time_to_sleep = max(1, (source_to_update['next_update'] - arrow.now()).total_seconds())
            last_updated = getattr(db.movies[source_to_update['crit_id']], source_to_update['source'] + '_updated')
            crit_popularity = db.movies[source_to_update['crit_id']].crit_popularity
            if crit_popularity is None:
                crit_popularity = 5
            print("{}: Updating {} info for '{}' ({}, popularity {:.1f}) {}. Last updated {}.".format(
                arrow.now().format('HH:mm:ss'), source_to_update['source'], db.movies[source_to_update['crit_id']].title,
                db.movies[source_to_update['crit_id']].year,
                crit_popularity,
                arrow.now().shift(seconds=time_to_sleep).humanize(),
                humanized_time(last_updated)))
            time.sleep(time_to_sleep)
            self.update_source(db, source_to_update)
        return

    def get_movies_stats(self, db):
        years_numbers = [db.movies[crit_id].year for crit_id in db.movies]
        years = {'min': np.min(years_numbers),
                 'median': np.median(years_numbers),
                 'max': np.max(years_numbers)}
        years['b_parameter'] = self.b_parameter(years['max'] - years['median'], years['max'] - years['min'])
        years['a_parameter'] = self.a_parameter(years['max'] - years['median'], years['b_parameter'])
        crit_pop_nrs = [db.movies[crit_id].crit_popularity for crit_id in db.movies
                        if db.movies[crit_id].crit_popularity is not None]
        crit_pop = {'min': np.min(crit_pop_nrs),
                    'median': np.median(crit_pop_nrs),
                    'max': np.max(crit_pop_nrs)}
        crit_pop['b_parameter'] = self.b_parameter(crit_pop['max'] - crit_pop['median'],
                                                   crit_pop['max'] - crit_pop['min'])
        crit_pop['a_parameter'] = self.a_parameter(crit_pop['max'] - crit_pop['median'],
                                                   crit_pop['b_parameter'])
        self.years = years
        self.crit_pop = crit_pop
        self.earliest_date_added = np.min([db.movies[crit_id].date_added for crit_id in db.movies])

    @staticmethod
    def b_parameter(median_feature, max_feature, median_period=6, max_period=36):
        return np.log(np.log(max_period) / np.log(median_period)) / np.log(max_feature / median_feature)

    @staticmethod
    def a_parameter(median_feature, b_parameter, median_period=8):
        return np.log(median_period)/np.power(median_feature, b_parameter)

    def get_all_next_updates(self, db, weibull_lambda=1.5):
        seq = []
        for crit_id, movie in db.movies.items():
            updates = self.calculate_next_updates(movie, weibull_lambda=weibull_lambda)
            seq += list(updates)
        now = arrow.now()
        seq = [u for u in seq if u['next_update'] <= now]
        return seq

    def calculate_next_updates(self, movie, weibull_lambda=1.5):
        year_period_score = self.calculate_period_score(self.years['max'] - movie.year, self.years)
        crit_popularity = self.crit_pop['median'] if movie.crit_popularity is None else movie.crit_popularity
        crit_pop_period_score = self.calculate_period_score(self.crit_pop['max'] - crit_popularity, self.crit_pop)
        base_update_period = self.calculate_update_period(year_period_score, crit_pop_period_score)

        update_periods = {}
        for source in self.sources:
            if (source in ('omdb', 'ptp') or source.startswith('imdb')) and movie.imdbid is None:
                break
            update_periods[source] = dict()
            update_periods[source]['source'] = source
            update_periods[source]['crit_id'] = movie.crit_id
            update_periods[source]['update_period'] = base_update_period * self.multipliers['multiplier_' + source]
            update_periods[source]['update_period_firsttime'] = \
                base_update_period * self.multipliers['multiplier_' + source + '_firsttime']
            next_update, period = self.calculate_next_update(getattr(movie, source + '_updated'),
                                                             update_periods[source]['update_period'],
                                                             update_periods[source]['update_period_firsttime'],
                                                             weibull_lambda=weibull_lambda)
            update_periods[source]['next_update'] = next_update
            update_periods[source]['actual_update_period'] = period
        return list(update_periods.values())

    @staticmethod
    def calculate_period_score(feature, stats):
        return np.exp(stats['a_parameter']*np.power(feature, stats['b_parameter']))

    @staticmethod
    def calculate_update_period(year_period_score, crit_pop_period_score, year_power=2,
                                crit_pop_power=1):
        period = np.exp((year_power * np.log(year_period_score) +
                         crit_pop_power * np.log(crit_pop_period_score)) /
                        (year_power + crit_pop_power))
        return period

    def calculate_next_update(self, date_updated, period, firsttime_period, weibull_lambda=1.5, min_period=500):
        weibull = (np.random.weibull(weibull_lambda, 1) / np.power(np.log(2), 1 / weibull_lambda))[0]
        if date_updated is None:
            next_update = self.earliest_date_added.shift(weeks=min(firsttime_period * weibull, min_period))
            return next_update, min(firsttime_period, min_period)
        else:
            next_update = date_updated.shift(weeks=min(period * weibull, min_period))
            return next_update, min(period, min_period)

    def get_source_update_sequence(self, all_updates, source):
        updates = [u for u in all_updates if u['source'].startswith(source)]
        sorted_updates = sorted(updates, key=itemgetter('next_update'))
        if len(sorted_updates) > 0:
            uph = min(len(sorted_updates), self.max_connections_per_hour[source])
            print("Updates needed per hour for {}: {:.0f}".format(source, uph))
            update_intervals = np.random.exponential(3600/uph, len(sorted_updates))
            for i, u in enumerate(sorted_updates):
                if i > 0:
                    sorted_updates[i]['next_update'] = sorted_updates[i-1]['next_update'].shift(seconds=update_intervals[i])
                else:
                    sorted_updates[i]['next_update'] = arrow.now().shift(seconds=update_intervals[i])
        return sorted_updates

    def update_source(self, db, source_to_update):
        movie = db.movies[source_to_update['crit_id']]
        if source_to_update['source'] == 'criticker':
            movie = self.crit_scraper.refresh_movie(movie)
        elif source_to_update['source'] == 'omdb':
            movie = self.omdb_scraper.refresh_movie(movie)
        elif source_to_update['source'].startswith('imdb'):
            infoset = re.search(r'imdb_(.*)', source_to_update['source']).groups()[0]
            movie = self.imdb_scraper.refresh_movie(movie, infoset=infoset)
        elif source_to_update['source'] == 'ptp':
            movie = self.ptp_scraper.refresh_movie(movie)
        if movie is not None:
            db.set_movie(movie)

    def update_movie_completely(self, db, imdbid):
        movie = [db.movies[crit_id] for crit_id in db.movies if db.movies[crit_id].imdbid == imdbid][0]
        movie = self.crit_scraper.refresh_movie(movie)
        movie = self.omdb_scraper.refresh_movie(movie)
        movie = self.imdb_scraper.refresh_movie(movie, infoset='main')
        movie = self.ptp_scraper.refresh_movie(movie)
        if movie is not None:
            db.set_movie(movie)
