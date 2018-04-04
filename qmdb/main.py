from qmdb.database.database import MySQLDatabase
from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.interfaces.updater import Updater
from qmdb.model.predictions import RatingModeler
from qmdb.interfaces.netflix import NetflixScraper
import time


if __name__ == "__main__":
    db = MySQLDatabase(from_scratch=False)
    omdb_scraper = OMDBScraper()
    crit_scraper = CritickerScraper(user='tijl')
    updater = Updater()
    modeler = RatingModeler(db)
    netflix_scraper = NetflixScraper(db)

    netflix_scraper.get_genre_ids()
    netflix_scraper.get_movies_for_genres()

    while True:
        print("\nRefreshing movie information from Criticker, IMDb and OMDB\n")
        time0 = time.time()
        while time.time() - time0 <= 12*3600:
            updater.update_movies(db, n=30, weibull_lambda=3)
        crit_scraper.get_movies(db, start_popularity=2)
        crit_scraper.get_ratings(db)
        modeler.get_predictions()
