from qmdb.database.database import Database
from qmdb.movie.movie import Movie


if __name__ == "__main__":
    db = Database('test.sqlite')

    imdbids = [133093, 3315342]
    for imdbid in imdbids:
        movie = Movie(imdbid)
        movie.add_rt_url()
        db.add_movie(movie)

    db.print()
