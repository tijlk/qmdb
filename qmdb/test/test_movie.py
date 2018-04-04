import arrow
from qmdb.movie.movie import Movie
import pytest


def test_get_floating_release_year():
    m = Movie({'crit_id': 123,
               'imdb_year': 2017,
               'year': 2016,
               'original_release_date': arrow.get("2018-07-01")})
    assert m.get_floating_release_year() == pytest.approx(2018.5, 0.01)
    m = Movie({'crit_id': 123,
               'imdb_year': 2017,
               'year': 2016,
               'original_release_date': None})
    assert m.get_floating_release_year() == pytest.approx(2017.5, 0.01)
    m = Movie({'crit_id': 123,
               'imdb_year': None,
               'year': 2016,
               'original_release_date': None})
    assert m.get_floating_release_year() == pytest.approx(2016.5, 0.01)
