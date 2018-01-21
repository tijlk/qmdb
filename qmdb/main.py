from qmdb.database.database import Database, MovieNotInDatabaseError
from qmdb.movie.movie import Movie
from qmdb.interfaces.omdb import OMDBScraper


if __name__ == "__main__":
    db = Database('test.sqlite')
    omdb_scraper = OMDBScraper()

    imdbids = [133093, 3315342, 133093]
    for imdbid in imdbids:
        movie = Movie(imdbid)
        db.set_movie(movie)
    db.print()

    imdbids = [133093, 3315342, 133093, 133092]
    for imdbid in imdbids:
        try:
            movie = db.get_movie(imdbid)
        except MovieNotInDatabaseError:
            pass
        else:
            movie = omdb_scraper.refresh_movie(movie)
        if movie:
            db.set_movie(movie)
    db.print()
    db.close()
