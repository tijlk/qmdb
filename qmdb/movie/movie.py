from qmdb.movie.utils import humanized_time


class Movie(object):
    def __init__(self, movie_info):
        self.crit_id = None
        self.crit_url = None
        self.title = None
        self.year = None
        self.imdbid = None
        self.tomato_url = None
        self.omdb_updated = None
        self.crit_updated = None
        self.load_from_dict(movie_info)

    def print(self):
        print("{} - {} - {} - Updated: {}".format(self.crit_id, self.imdbid, self.tomato_url,
                                                  humanized_time(self.omdb_updated)))
    @staticmethod
    def replace_if_not_none(new_value, old_value):
        return new_value if new_value is not None else old_value

    def load_from_dict(self, movie_info):
        if not isinstance(movie_info, dict):
            raise TypeError("A Movie object should be initialized with a dictionary!")
        if self.crit_id is None and ('crit_id' not in movie_info or not isinstance(movie_info['crit_id'], int)):
            raise Exception("There is no valid criticker id listed in the movie info "
                            "and the movie object didn't already have one!")
        self.crit_id = self.replace_if_not_none(movie_info.get('crit_id'), self.crit_id)
        self.crit_url = self.replace_if_not_none(movie_info.get('crit_url'), self.crit_url)
        self.title = self.replace_if_not_none(movie_info.get('title'), self.title)
        self.year = self.replace_if_not_none(movie_info.get('year'), self.year)
        self.imdbid = self.replace_if_not_none(movie_info.get('imdbid'), self.imdbid)
        self.tomato_url = self.replace_if_not_none(movie_info.get('tomato_url'), self.tomato_url)
        self.omdb_updated = self.replace_if_not_none(movie_info.get('omdb_updated'), self.omdb_updated)
        self.crit_updated = self.replace_if_not_none(movie_info.get('crit_updated'), self.crit_updated)