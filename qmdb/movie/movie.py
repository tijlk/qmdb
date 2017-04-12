from qmdb.movie.utils import humanized_time


class Movie(object):
    def __init__(self, imdbid):
        self.imdbid = imdbid
        self.tomato_url = None
        self.omdb_updated = None

    def print(self):
        print("{} - {} - Updated: {}".format(self.imdbid, self.tomato_url, humanized_time(self.omdb_updated)))
