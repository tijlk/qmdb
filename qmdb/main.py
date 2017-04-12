from qmdb.database.database import Database
from qmdb.movie.movie import Movie
from qmdb.interfaces.omdb import OMDBScraper


if __name__ == "__main__":
    db = Database('test.sqlite')
    omdb_scraper = OMDBScraper()

    imdbids = [133093, 3315342]
    for imdbid in imdbids:
        movie = Movie(imdbid)
        db.add_movie(movie)
    db.print()

    for imdbid in imdbids:
        movie = omdb_scraper.refresh_movie(db.movies[imdbid])
        if movie:
            db.movies[imdbid] = movie
    db.print()
