from qmdb.database.database import Database, MovieNotInDatabaseError
from qmdb.movie.movie import Movie
from qmdb.interfaces.omdb import OMDBScraper
from qmdb.interfaces.criticker import CritickerScraper


if __name__ == "__main__":
    db = Database('test.sqlite')
    omdb_scraper = OMDBScraper()
    crit_scraper = CritickerScraper()

    movies = crit_scraper.get_movies(min_popularity=10, debug=True)

    for movie in movies:
        db.set_movie(movie)
    db.print()

    crit_ids = [1979, 1077, 2463]
    for crit_id in crit_ids:
        try:
            movie = db.get_movie(crit_id)
        except MovieNotInDatabaseError:
            pass
        else:
            movie = crit_scraper.refresh_movie(movie)
            movie = omdb_scraper.refresh_movie(movie)
            db.set_movie(movie)
    db.print()
    db.close()
