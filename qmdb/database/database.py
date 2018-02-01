import sqlite3
import os


class Database:
    def __init__(self, filename, from_scratch=False):
        self.filename = filename
        self.movies = {}
        self.conn = None
        self.c = None
        if from_scratch:
            self.initialize(from_scratch=from_scratch)
        else:
            try:
                self.load()
            except FileNotFoundError:
                print("The file containing the database can't be found. " +
                      "Therefore, I'm initalizing the database from scratch")
                self.initialize()

    def initialize(self, from_scratch=False):
        if from_scratch:
            try:
                os.remove(self.filename)
            except OSError:
                pass
        print("Initializing database at {}".format(self.filename))
        self.conn = sqlite3.connect(self.filename)
        self.c = self.conn.cursor()
        self.c.execute("""
            CREATE TABLE movies (
                crit_id integer,
                crit_url text,
                title text,
                year integer,
                imdbid integer,
                tomato_url text,
                omdb_updated integer,
                crit_updated integer
            )
            """)

    def load(self):
        if not os.path.exists(self.filename):
            raise FileNotFoundError
        else:
            self.conn = sqlite3.connect(self.filename)
            self.c = self.conn.cursor()
            try:
                movies_iterator = self.c.execute("select * from movies")
            except sqlite3.OperationalError:
                print("There is something wrong with the database file!")
                raise
            self.movies = {movie[0]: self.load_movie(movie) for movie in movies_iterator}
            print("database at {} loaded".format(self.filename))

    def load_movie(self, movie_info):
        movie = {'crit_id': movie_info[0],
                 'crit_url': movie_info[1]}
        return movie

    def close(self):
        print("database closed at {}".format(self.filename))

    def set_movie(self, movie, overwrite=True):
        if not overwrite:
            if movie.crit_id in self.movies:
                print("Movie CritickerID {} is already in the database".format(movie.crit_id))
        self.movies[movie.crit_id] = movie

    def get_movie(self, crit_id):
        try:
            return self.movies[crit_id]
        except KeyError as e:
            print("CritickerID {} does not exist in the database.".format(e.args[0]))
            raise MovieNotInDatabaseError(crit_id=crit_id)

    def print(self):
        for movie in self.movies:
            self.movies[movie].print()


class MovieNotInDatabaseError(Exception):
    def __init__(self, crit_id=None):
        self.crit_id = crit_id
