from qmdb.database.database import MySQLDatabase, MovieNotInDatabaseError
from qmdb.movie.movie import Movie
from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.interfaces.updater import Updater


if __name__ == "__main__":
    db = MySQLDatabase(from_scratch=False)
    omdb_scraper = OMDBScraper()
    crit_scraper = CritickerScraper(user='tijl')
    updater = Updater()
    #crit_scraper.get_movies(db, start_popularity=2)
    #crit_scraper.get_ratings(db)

    print("\nRefreshing movie information from Criticker, IMDb and OMDB\n")
    while True:
        updater.update_movies(db, n=30, weibull_lambda=5)
    db.print()
