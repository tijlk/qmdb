from qmdb.interfaces.omdb import imdbid_to_rturl


class Movie(object):
    def __init__(self, imdbid):
        self.imdbid = imdbid
        self.tomato_url = None

    def add_rt_url(self):
        self.tomato_url = imdbid_to_rturl(self.imdbid)

    def print(self):
        print("{} - {}".format(self.imdbid, self.tomato_url))
