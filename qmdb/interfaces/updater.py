from qmdb.interfaces.criticker import CritickerScraper
from qmdb.interfaces.omdb import OMDBScraper
from operator import itemgetter
import numpy as np
import arrow
import time
from qmdb.movie.utils import humanized_time


class Updater(object):
    def __init__(self):
        self.crit_scraper = CritickerScraper()
        self.omdb_scraper = OMDBScraper()
        self.years = None
        self.crit_pop_pages = None
        self.earliest_date_added = None
        self.max_connections_per_hour_omdb = 40
        self.max_connections_per_hour_criticker = 500

    def update_movies(self, db, n=None, multiplier_criticker=1, multiplier_omdb=1, weibull_lambda=1.5):
        self.get_movies_stats(db)
        sorted_seq, uph = self.get_update_sequence(db, multiplier_criticker=multiplier_criticker,
                                                   multiplier_omdb=multiplier_omdb, weibull_lambda=weibull_lambda)
        uph = uph + len(sorted_seq)/24
        print("Updates needed per hour: {}".format(uph))
        if n is not None:
            sources_to_update = sorted_seq[:n]
        else:
            sources_to_update = sorted_seq
        for i, source_to_update in enumerate(sources_to_update):
            time_to_sleep = np.random.exponential(3600/uph, 1)[0]
            if source_to_update['source'] == 'criticker':
                last_updated = db.movies[source_to_update['crit_id']].crit_updated
            else:
                last_updated = db.movies[source_to_update['crit_id']].omdb_updated
            print("Updating {} info for '{}' ({}, page {}) {}. Last updated {}.".format(
                source_to_update['source'], db.movies[source_to_update['crit_id']].title,
                db.movies[source_to_update['crit_id']].year,
                db.movies[source_to_update['crit_id']].crit_popularity_page,
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
        years['b_parameter'] = self.b_parameter(years['max']-years['median'], years['max']-years['min'])
        years['a_parameter'] = self.a_parameter(years['max']-years['median'], years['b_parameter'])
        crit_pop_pages_nrs = [db.movies[crit_id].crit_popularity_page for crit_id in db.movies]
        crit_pop_pages = {'min': np.min(crit_pop_pages_nrs),
                          'median': np.median(crit_pop_pages_nrs),
                          'max': np.max(crit_pop_pages_nrs)}
        crit_pop_pages['b_parameter'] = self.b_parameter(crit_pop_pages['median']-crit_pop_pages['min'],
                                                         crit_pop_pages['max']-crit_pop_pages['min'])
        crit_pop_pages['a_parameter'] = self.a_parameter(crit_pop_pages['median']-crit_pop_pages['min'],
                                                         crit_pop_pages['b_parameter'])
        self.years = years
        self.crit_pop_pages = crit_pop_pages
        self.earliest_date_added = np.min([db.movies[crit_id].date_added for crit_id in db.movies])

    @staticmethod
    def b_parameter(median_feature, max_feature, median_period=6, max_period=36):
        return np.log(np.log(max_period) / np.log(median_period)) / np.log(max_feature / median_feature)

    @staticmethod
    def a_parameter(median_feature, b_parameter, median_period=8):
        return np.log(median_period)/np.power(median_feature, b_parameter)

    def get_all_next_updates(self, db, multiplier_criticker=1, multiplier_omdb=1, weibull_lambda=1.5):
        seq = []
        for crit_id, movie in db.movies.items():
            updates = self.calculate_next_updates(movie, multiplier_criticker=multiplier_criticker,
                                                  multiplier_omdb=multiplier_omdb, weibull_lambda=weibull_lambda)
            seq += list(updates)
        crit_updates_per_hr = np.sum([1 / e['update_period'] for e in seq if e['source'] == 'criticker']) / (24 * 7)
        omdb_updates_per_hr = np.sum([1 / e['update_period'] for e in seq if e['source'] == 'OMDB']) / (24 * 7)
        now = arrow.now()
        seq = [u for u in seq if u['next_update'] <= now]
        return seq, crit_updates_per_hr, omdb_updates_per_hr

    def get_update_sequence(self, db, multiplier_criticker=1, multiplier_omdb=1, weibull_lambda=1.5):
        seq, crit_uph, omdb_uph = self.get_all_next_updates(db, multiplier_criticker=multiplier_criticker,
                                                            multiplier_omdb=multiplier_omdb,
                                                            weibull_lambda=weibull_lambda)
        if crit_uph > self.max_connections_per_hour_criticker or \
            omdb_uph > self.max_connections_per_hour_omdb:
            if crit_uph > self.max_connections_per_hour_criticker:
                multiplier_criticker = crit_uph / self.max_connections_per_hour_criticker * 1.2 * multiplier_criticker
            if omdb_uph > self.max_connections_per_hour_omdb:
                multiplier_omdb = omdb_uph / self.max_connections_per_hour_omdb * 1.2 * multiplier_omdb
            seq, crit_uph, omdb_uph = self.get_all_next_updates(db, multiplier_criticker=multiplier_criticker,
                                                                multiplier_omdb=multiplier_omdb,
                                                                weibull_lambda=weibull_lambda)
        print(crit_uph, omdb_uph)
        sorted_seq = sorted(seq, key=itemgetter('next_update'))
        return sorted_seq, crit_uph+omdb_uph

    def calculate_next_updates(self, movie, multiplier_criticker=1, multiplier_omdb=1, weibull_lambda=1.5):
        year_period_score = self.calculate_period_score(self.years['max'] - movie.year, self.years)
        crit_pop_pages_period_score = self.calculate_period_score(
            movie.crit_popularity_page - self.crit_pop_pages['min'], self.crit_pop_pages)
        update_period = self.calculate_update_period(year_period_score, crit_pop_pages_period_score)

        crit_update_period = update_period * multiplier_criticker
        next_crit_update = self.calculate_next_update(movie.date_added, movie.crit_updated, crit_update_period,
                                                      weibull_lambda=weibull_lambda)
        if movie.imdbid is None:
            multiplier_omdb *= 100
        omdb_update_period = update_period * multiplier_omdb * 5
        next_omdb_update = self.calculate_next_update(movie.date_added, movie.omdb_updated, omdb_update_period,
                                                      weibull_lambda=weibull_lambda)
        return ({'source': 'criticker',
                 'crit_id': movie.crit_id,
                 'next_update': next_crit_update,
                 'update_period': crit_update_period},
                {'source': 'OMDB',
                 'crit_id': movie.crit_id,
                 'next_update': next_omdb_update,
                 'update_period': omdb_update_period})

    @staticmethod
    def calculate_period_score(feature, stats):
        return np.exp(stats['a_parameter']*np.power(feature, stats['b_parameter']))

    @staticmethod
    def calculate_update_period(year_period_score, crit_pop_pages_period_score, year_power=1,
                                crit_pop_pages_power=1/3):
        return np.exp((year_power * np.log(year_period_score) +
                       crit_pop_pages_power * np.log(crit_pop_pages_period_score)) /
                      (year_power + crit_pop_pages_power))

    def calculate_next_update(self, date_added, date_updated, period, weibull_lambda=1.5):
        weibull = (np.random.weibull(weibull_lambda, 1) / np.power(np.log(2), 1 / weibull_lambda))[0]
        waiting_time = weibull * period
        if date_updated is None:
            next_update = self.earliest_date_added.shift(weeks=waiting_time/50000)
        else:
            next_update = date_updated.shift(weeks=waiting_time)
        return next_update

    def update_source(self, db, source_to_update):
        movie = db.movies[source_to_update['crit_id']]
        if source_to_update['source'] == 'criticker':
            movie = self.crit_scraper.refresh_movie(movie)
        elif source_to_update['source'] == 'OMDB':
            movie = self.omdb_scraper.refresh_movie(movie)
        if movie is not None:
            db.set_movie(movie)
