class Database:
    def __init__(self, filename):
        self.filename = filename
        self.movies = {}
        try:
            self.load()
        except FileNotFoundError:
            print("The file containing the database can't be found. " +
                  "Therefore, I'm initalizing the database from scratch")
            self.initialize()

    def initialize(self):
        print("Initializing database at {}".format(self.filename))

    def load(self):
        file_exists = False
        if not file_exists:
            raise FileNotFoundError
        else:
            print("database at {} loaded".format(self.filename))
            self.movies = {'1': 'dummy-movie'}

    def close(self):
        print("database closed at {}".format(self.filename))

    def set_movie(self, movie, overwrite=True):
        if not overwrite:
            if movie.imdbid in self.movies:
                print("Movie IMDBid {} is already in the database".format(movie.imdbid))
        self.movies[movie.imdbid] = movie

    def get_movie(self, imdbid):
        try:
            return self.movies[imdbid]
        except KeyError as e:
            print("IMDBid {} does not exist in the database.".format(e.args[0]))
            raise MovieNotInDatabaseError(imdbid=imdbid)

    def print(self):
        for movie in self.movies:
            self.movies[movie].print()


class MovieNotInDatabaseError(Exception):
    def __init__(self, imdbid=None):
        self.imdbid = imdbid
