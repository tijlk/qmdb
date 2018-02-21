from qmdb.database.database import MySQLDatabase, MovieNotInDatabaseError
from qmdb.movie.movie import Movie
from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper
from qmdb.interfaces.updater import Updater


def get_criticker_movies(db, crit_scraper, start_popularity=1):
    movies = crit_scraper.get_movies(start_popularity=start_popularity)
    print("\nSaving movie information to the database\n")
    for movie in movies:
        db.set_movie(Movie(movie))
    db.print()


def get_criticker_ratings(db, crit_scraper):
    ratings = crit_scraper.get_ratings()
    print("\nSaving rating information to the database\n")
    for rating in ratings:
        db.set_movie(Movie(rating))


if __name__ == "__main__":
    db = MySQLDatabase()
    omdb_scraper = OMDBScraper()
    crit_scraper = CritickerScraper(user='tijl')
    updater = Updater()
    get_criticker_movies(db, crit_scraper, start_popularity=2)

    print("\nRefreshing movie information from Criticker, IMDb and OMDB\n")
    while True:
        updater.update_movies(db, n=30, weibull_lambda=5)
    db.print()
