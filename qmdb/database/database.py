class Database():
    def __init__(self, filename):
        check_if_file_exists = True
        if check_if_file_exists:
            self.load(filename)
        else:
            self.initialize(filename)
        self.movies = {}

    @staticmethod
    def load(filename):
        print("database at {} loaded".format(filename))

    @staticmethod
    def initialize(filename):
        print("Initializing database at {}".format(filename))

    def add_movie(self, movie):
        self.movies[movie.imdbid] = movie

    def print(self):
        for movie in self.movies:
            self.movies[movie].print()
