from qmdb.movie.utils import humanized_time
import arrow


class Movie(object):
    def __init__(self, movie_info):
        self.crit_id = None
        self.crit_popularity = None
        self.crit_url = None
        self.title = None
        self.year = None
        self.imdbid = None
        self.tomato_url = None
        self.poster_url = None
        self.trailer_url = None
        self.crit_rating = None
        self.crit_votes = None
        self.imdb_title = None
        self.imdb_year = None
        self.kind = None
        self.cast = None
        self.director = None
        self.writer = None
        self.genres = None
        self.runtime = None
        self.countries = None
        self.imdb_rating = None
        self.imdb_votes = None
        self.plot_summary = None
        self.plot_storyline = None
        self.languages = None
        self.original_release_date = None
        self.dutch_release_date = None
        self.original_title = None
        self.english_title = None
        self.metacritic_score = None
        self.keywords = None
        self.taglines = None
        self.vote_details = None
        self.ptp_url = None
        self.ptp_hd_available = None
        self.netflix_id = None
        self.netflix_rating = None
        self.netflix_title = None
        self.date_added = None
        self.criticker_updated = None
        self.imdb_main_updated = None
        self.imdb_release_updated = None
        self.imdb_metacritic_updated = None
        self.imdb_keywords_updated = None
        self.imdb_taglines_updated = None
        self.imdb_vote_details_updated = None
        self.imdb_plot_updated = None
        self.omdb_updated = None
        self.ptp_updated = None
        self.netflix_updated = None
        self.my_ratings = dict()
        self.update_from_dict(movie_info)

    def print(self):
        print("{} - {} ({}) - Criticker updated {} - OMDB updated {}".format(
            self.crit_id, self.title, self.year, humanized_time(self.criticker_updated), humanized_time(self.omdb_updated)))

    def update_from_dict(self, movie_info):
        if not isinstance(movie_info, dict):
            raise TypeError("A Movie object should be initialized with a dictionary!")
        if self.crit_id is None and ('crit_id' not in movie_info or not isinstance(movie_info['crit_id'], int)):
            raise Exception("There is no valid criticker id listed in the movie info "
                            "and the movie object didn't already have one!")
        self.crit_id = replace_if_not_none(movie_info.get('crit_id'), self.crit_id)
        self.crit_popularity = replace_if_not_none(movie_info.get('crit_popularity'),
                                                             self.crit_popularity)
        self.crit_url = replace_if_not_none(movie_info.get('crit_url'), self.crit_url)
        self.title = replace_if_not_none(movie_info.get('title'), self.title)
        self.year = replace_if_not_none(movie_info.get('year'), self.year)
        self.imdbid = replace_if_not_none(movie_info.get('imdbid'), self.imdbid)
        self.tomato_url = replace_if_not_none(movie_info.get('tomato_url'), self.tomato_url)
        self.poster_url = replace_if_not_none(movie_info.get('poster_url'), self.poster_url)
        self.trailer_url = replace_if_not_none(movie_info.get('trailer_url'), self.trailer_url)
        self.crit_rating = replace_if_not_none(movie_info.get('crit_rating'), self.crit_rating)
        self.crit_votes = replace_if_not_none(movie_info.get('crit_votes'), self.crit_votes)
        self.imdb_title = replace_if_not_none(movie_info.get('imdb_title'), self.imdb_title)
        self.imdb_year = replace_if_not_none(movie_info.get('imdb_year'), self.imdb_year)
        self.kind = replace_if_not_none(movie_info.get('kind'), self.kind)
        self.cast = replace_if_not_none(movie_info.get('cast'), self.cast)
        self.director = replace_if_not_none(movie_info.get('director'), self.director)
        self.writer = replace_if_not_none(movie_info.get('writer'), self.writer)
        self.genres = replace_if_not_none(movie_info.get('genres'), self.genres)
        self.runtime = replace_if_not_none(movie_info.get('runtime'), self.runtime)
        self.countries = replace_if_not_none(movie_info.get('countries'), self.countries)
        self.imdb_rating = replace_if_not_none(movie_info.get('imdb_rating'), self.imdb_rating)
        self.imdb_votes = replace_if_not_none(movie_info.get('imdb_votes'), self.imdb_votes)
        self.plot_summary = replace_if_not_none(movie_info.get('plot_summary'), self.plot_summary)
        self.plot_storyline = replace_if_not_none(movie_info.get('plot_storyline'), self.plot_storyline)
        self.languages = replace_if_not_none(movie_info.get('languages'), self.languages)
        self.original_release_date = replace_if_not_none(
            str_to_arrow(movie_info.get('original_release_date')), self.original_release_date)
        self.dutch_release_date = replace_if_not_none(str_to_arrow(movie_info.get('dutch_release_date')),
                                                           self.dutch_release_date)
        self.original_title = replace_if_not_none(movie_info.get('original_title'), self.original_title)
        self.english_title = replace_if_not_none(movie_info.get('english_title'), self.english_title)
        self.metacritic_score = replace_if_not_none(movie_info.get('metacritic_score'), self.metacritic_score)
        self.keywords = replace_if_not_none(movie_info.get('keywords'), self.keywords)
        self.taglines = replace_if_not_none(movie_info.get('taglines'), self.taglines)
        self.vote_details = replace_if_not_none(movie_info.get('vote_details'), self.vote_details)
        self.ptp_url = replace_if_not_none(movie_info.get('ptp_url'), self.ptp_url)
        self.ptp_hd_available = replace_if_not_none(none_bool(movie_info.get('ptp_hd_available')),
                                                         self.ptp_hd_available)
        if movie_info.get('my_ratings') is not None:
            for user in movie_info.get('my_ratings'):
                if user in self.my_ratings:
                    self.my_ratings[user].update(movie_info.get('my_ratings')[user])
                else:
                    self.my_ratings[user] = movie_info.get('my_ratings')[user]
        self.date_added = replace_if_none(str_to_arrow(movie_info.get('date_added')), self.date_added)
        self.criticker_updated = replace_if_not_none(str_to_arrow(movie_info.get('criticker_updated')),
                                                          self.criticker_updated)
        self.imdb_main_updated = replace_if_not_none(str_to_arrow(movie_info.get('imdb_main_updated')),
                                                          self.imdb_main_updated)
        self.imdb_release_updated = replace_if_not_none(str_to_arrow(movie_info.get('imdb_release_updated')),
                                                             self.imdb_release_updated)
        self.imdb_metacritic_updated = replace_if_not_none(
            str_to_arrow(movie_info.get('imdb_metacritic_updated')), self.imdb_metacritic_updated)
        self.imdb_keywords_updated = replace_if_not_none(
            str_to_arrow(movie_info.get('imdb_keywords_updated')), self.imdb_keywords_updated)
        self.imdb_taglines_updated = replace_if_not_none(
            str_to_arrow(movie_info.get('imdb_taglines_updated')), self.imdb_taglines_updated)
        self.imdb_vote_details_updated = replace_if_not_none(
            str_to_arrow(movie_info.get('imdb_vote_details_updated')), self.imdb_vote_details_updated)
        self.omdb_updated = replace_if_not_none(str_to_arrow(movie_info.get('omdb_updated')),
                                                     self.omdb_updated)
        self.imdb_plot_updated = replace_if_not_none(str_to_arrow(movie_info.get('imdb_plot_updated')),
                                                          self.imdb_plot_updated)
        self.ptp_updated = replace_if_not_none(str_to_arrow(movie_info.get('ptp_updated')),
                                                    self.ptp_updated)
        self.netflix_id = replace_if_not_none(movie_info.get('netflix_id'), self.netflix_id)
        self.netflix_title = replace_if_not_none(movie_info.get('netflix_title'), self.netflix_title)
        self.netflix_rating = replace_if_not_none(movie_info.get('netflix_rating'), self.netflix_rating)
        self.netflix_updated = replace_if_not_none(str_to_arrow(movie_info.get('netflix_updated')),
                                                   self.netflix_updated)

    def get_floating_release_year(self):
        if self.original_release_date is None:
            if self.imdb_year is None:
                return self.year + 0.5
            else:
                return self.imdb_year + 0.5
        else:
            reldate_ts = self.original_release_date.float_timestamp
            year = arrow.get(self.original_release_date).year
            yearstart_ts = arrow.get(str(year) + "-01-01").float_timestamp
            yearend_ts = arrow.get(str(year+1) + "-01-01").float_timestamp
            year_length = yearend_ts - yearstart_ts
            return year + (reldate_ts - yearstart_ts)/year_length


def replace_if_not_none(new_value, old_value):
    return new_value if new_value is not None else old_value


def replace_if_none(new_value, old_value):
    return new_value if old_value is None else old_value


def str_to_arrow(s):
    if isinstance(s, str):
        return arrow.get(s)
    if isinstance(s, arrow.Arrow) or s is None:
        return s
    else:
        raise Exception("the provided entry is of an incompatible data type!")


def none_bool(b):
    if b is None:
        return None
    else:
        return bool(b)