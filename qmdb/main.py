from qmdb.database.database import SQLiteDatabase, MySQLDatabase, MovieNotInDatabaseError
from qmdb.movie.movie import Movie
from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper


if __name__ == "__main__":
    filename = 'data/movies.sqlite'
    db = MySQLDatabase()
    omdb_scraper = OMDBScraper()
    crit_scraper = CritickerScraper()
    movies = crit_scraper.get_movies(min_popularity=10, debug=True)

    print("\nSaving movie information to the database\n")
    for movie in movies:
        db.set_movie(Movie(movie))
    db.print()

    print("\nRefreshing movie information from Criticker and OMDB\n")
    for crit_id in db.movies:
        try:
            movie = db.get_movie(crit_id)
        except MovieNotInDatabaseError:
            print("This is weird.")
            pass
        else:
            movie = crit_scraper.refresh_movie(movie)
            movie = omdb_scraper.refresh_movie(movie)
            db.set_movie(movie)
    db.print()
    db.close()
