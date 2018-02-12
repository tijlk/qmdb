from qmdb.database.database import SQLiteDatabase, MySQLDatabase, MovieNotInDatabaseError
from qmdb.movie.movie import Movie
from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.interfaces.updater import Updater


def get_criticker_movies(db, crit_scraper, min_popularity=8):
    movies = crit_scraper.get_movies(min_popularity=min_popularity)
    print("\nSaving movie information to the database\n")
    for movie in movies:
        db.set_movie(Movie(movie))
    db.print()


if __name__ == "__main__":
    filename = 'data/movies.sqlite'
    db = MySQLDatabase()
    omdb_scraper = OMDBScraper()
    crit_scraper = CritickerScraper()
    updater = Updater()
    get_criticker_movies(db, crit_scraper)

    print("\nRefreshing movie information from Criticker and OMDB\n")
    while True:
        updater.update_movies(db, n=20, multiplier_omdb=1, multiplier_criticker=1, weibull_lambda=5)
    db.print()
