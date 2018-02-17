from qmdb.database.database import MySQLDatabase, MovieNotInDatabaseError
from qmdb.movie.movie import Movie
from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.interfaces.updater import Updater


def get_criticker_movies(db, crit_scraper, min_popularity=6):
    movies = crit_scraper.get_movies(min_popularity=min_popularity)
    print("\nSaving movie information to the database\n")
    for movie in movies:
        db.set_movie(Movie(movie))
    db.print()


if __name__ == "__main__":
    db = MySQLDatabase()
    omdb_scraper = OMDBScraper()
    crit_scraper = CritickerScraper()
    updater = Updater()
    #get_criticker_movies(db, crit_scraper)

    print("\nRefreshing movie information from Criticker, IMDb and OMDB\n")
    while True:
        updater.update_movies(db, n=20, weibull_lambda=5)
    db.print()
