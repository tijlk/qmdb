from qmdb.interfaces.criticker import CritickerScraper
from qmdb.interfaces.omdb import OMDBScraper
from operator import itemgetter
import numpy as np


class Updater(object):
    def __init__(self):
        self.crit_scraper = CritickerScraper()
        self.omdb_scraper = OMDBScraper()

    def update_movies(self, db, n=10):
        sorted_seq = self.get_update_sequence(db)
        sources_to_update = sorted_seq[:n]


    @staticmethod
    def calculate_next_update(movie):
        year_score = 1 / ((2018 - movie.year) * 0.2 + 1)
        crit_popularity_score = 1 / ((movie.crit_popularity_page - 1) * 0.06 + 1)
        total_score = year_score + crit_popularity_score
        # TODO: convert random variable to a certain number of days
        next_crit_update = np.random.exponential(1/total_score, 1)[0] + movie.crit_updated
        # TODO: figure something out for OMDB
        next_omdb_update = np.random.exponential(1/total_score, 1)[0] + movie.omdb_updated
        # TODO: what if the source was never updated before?

    def get_update_sequence(self, db):
        seq = []
        for movie in db.movies:
            crit_prio, omdb_prio = self.calculate_update_priority(movie)
            seq += [crit_prio, omdb_prio]
        sorted_seq = sorted(seq, key=itemgetter('priority'))
        return sorted_seq