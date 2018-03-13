import copy
import time
from itertools import groupby

import arrow
import pymysql.cursors
from arrow import Arrow

from qmdb.config import config
from qmdb.movie.movie import Movie


class Database:
    def __init__(self, from_scratch=False):
        self.movies = {}
        self.conn = None
        self.c = None
        self.load_or_initialize(from_scratch=from_scratch)

    def load_or_initialize(self, from_scratch=False):
        if from_scratch:
            self.initialize()
        else:
            self.load()

    def connect(self, from_scratch=False):
        raise NotImplementedError

    def close(self):
        self.conn.commit()
        self.conn.close()

    def initialize(self):
        raise NotImplementedError

    def load(self):
        raise NotImplementedError

    def get_movie(self, crit_id):
        try:
            return self.movies[crit_id]
        except KeyError as e:
            print("CritickerID {} does not exist in the database.".format(e.args[0]))
            raise MovieNotInDatabaseError(crit_id=crit_id)

    def print(self):
        movies = sorted(list(self.movies.values()),
                        key=lambda x: max(arrow.get('1970-01-01') if x.criticker_updated is None else x.criticker_updated,
                                          arrow.get('1970-01-01') if x.omdb_updated is None else x.omdb_updated),
                        reverse=True)
        for movie in movies[:10]:
            self.movies[movie.crit_id].print()
        print("\n")

    @staticmethod
    def make_dict_db_safe(d):
        d = copy.deepcopy(d)
        for k in d:
            if isinstance(d[k], Arrow):
                d[k] = d[k].format()
        return d


class MySQLDatabase(Database):
    def __init__(self, from_scratch=False, schema='qmdb', env='prd'):
        if env == 'prd':
            self.config = config.mysql_prd
        else:
            self.config = config.mysql_tst
        self.schema = schema
        self.columns_movies = {
            'crit_id': 'mediumint unsigned not null',
            'imdbid': 'int unsigned',
            'year': 'smallint unsigned',
            'imdb_year': 'smallint unsigned',
            'title': 'varchar(256) not null',
            'imdb_title': 'varchar(256)',
            'english_title': 'varchar(256)',
            'original_title': 'varchar(256)',
            'crit_rating': 'float',
            'crit_votes': 'mediumint unsigned',
            'imdb_rating': 'float',
            'imdb_votes': 'int unsigned',
            'metacritic_score': 'smallint unsigned',
            'kind': 'varchar(64)',
            'runtime': 'smallint unsigned',
            'plot_summary': 'varchar(4096)',
            'plot_storyline': 'varchar(8192)',
            'original_release_date': 'varchar(32)',
            'dutch_release_date': 'varchar(32)',
            'crit_popularity': 'float',
            'crit_url': 'varchar(256) not null',
            'tomato_url': 'varchar(256)',
            'poster_url': 'varchar(256)',
            'trailer_url': 'varchar(256)',
            'date_added': 'varchar(32) not null',
            'criticker_updated': 'varchar(32)',
            'imdb_main_updated': 'varchar(32)',
            'imdb_release_updated': 'varchar(32)',
            'imdb_metacritic_updated': 'varchar(32)',
            'imdb_keywords_updated': 'varchar(32)',
            'imdb_taglines_updated': 'varchar(32)',
            'imdb_vote_details_updated': 'varchar(32)',
            'imdb_plot_updated': 'varchar(32)',
            'omdb_updated': 'varchar(32)'
        }
        self.columns_persons = {
            'crit_id': 'mediumint unsigned not null',
            'person_id': 'int unsigned not null',
            'rank': 'smallint unsigned not null',
            'name': 'varchar(256)',
            'canonical_name': 'varchar(256)',
            'role': 'varchar(32) not null'
        }
        self.columns_genres = {
            'crit_id': 'mediumint unsigned not null',
            'genre': 'varchar(32) not null'
        }
        self.columns_countries = {
            'crit_id': 'mediumint unsigned not null',
            'country': 'varchar(64) not null',
            'rank': 'smallint unsigned not null'
        }
        self.columns_languages = {
            'crit_id': 'mediumint unsigned not null',
            'language': 'varchar(64) not null',
            'rank': 'smallint unsigned not null'
        }
        self.columns_keywords = {
            'crit_id': 'mediumint unsigned not null',
            'keyword': 'varchar(128) not null'
        }
        self.columns_taglines = {
            'crit_id': 'mediumint unsigned not null',
            'tagline': 'varchar(1024) not null',
            'rank': 'smallint unsigned not null'
        }
        self.columns_vote_details = {
            'crit_id': 'mediumint unsigned not null',
            'demographic': 'varchar(64) not null',
            'votes': 'mediumint not null',
            'rating': 'float'
        }
        self.columns_ratings = {
            'crit_id': 'mediumint unsigned not null',
            'user': 'varchar(64) not null',
            'type': 'varchar(32) not null',
            'score': 'float'
        }
        super().__init__(from_scratch=from_scratch)

    def load_or_initialize(self, from_scratch=False):
        if from_scratch:
            self.initialize()
        else:
            try:
                self.load()
            except pymysql.err.ProgrammingError:
                print("Something went wrong trying to load the tables.")
                raise Exception

    def connect(self, from_scratch=False):
        self.conn = pymysql.connect(host=self.config['host'],
                                    user=self.config['username'],
                                    password=self.config['password'],
                                    db=self.schema,
                                    charset='utf8mb4',
                                    use_unicode=True,
                                    cursorclass=pymysql.cursors.DictCursor)
        self.c = self.conn.cursor()

    def create_table(self, table_name, column_info, primary_keys, indexes):
        if not isinstance(primary_keys, list) or len(primary_keys) == 0 or not isinstance(primary_keys[0], str):
            raise Exception("Something is wrong with the primary keys")
        if not isinstance(indexes, list) or len(indexes) == 0 or not isinstance(indexes[0], str):
            raise Exception("Something is wrong with the indexes")
        self.c.execute("drop table if exists {}".format(table_name))
        print("Creating table {}.{}".format(self.schema, table_name))
        with self.conn:
            sql = """
                CREATE TABLE {} (
                    {},
                    PRIMARY KEY({}), INDEX({})
                ) DEFAULT CHARSET=utf8mb4
                """.format(table_name,
                           ', '.join(["{} {}".format(k, v) for k, v in column_info.items()]),
                           ', '.join(primary_keys),
                           '), INDEX('.join(indexes))
            self.c.execute(sql)

    def initialize(self):
        self.connect()
        self.create_table('movies', self.columns_movies, ['crit_id'], ['crit_id'])
        self.create_table('persons', self.columns_persons, ['crit_id', 'person_id', 'role'], ['person_id', 'role'])
        self.create_table('genres', self.columns_genres, ['crit_id', 'genre'], ['genre'])
        self.create_table('countries', self.columns_countries, ['crit_id', 'country'], ['country'])
        self.create_table('languages', self.columns_languages, ['crit_id', 'language'], ['language'])
        self.create_table('keywords', self.columns_keywords, ['crit_id', 'keyword'], ['keyword'])
        self.create_table('taglines', self.columns_taglines, ['crit_id', 'rank'], ['rank'])
        self.create_table('vote_details', self.columns_vote_details, ['crit_id', 'demographic'], ['demographic'])
        self.create_table('ratings', self.columns_ratings, ['crit_id', 'user', 'type'], ['user', 'type'])
        self.close()

    def load(self, verbose=False):
        self.connect()
        movies = self.load_movies()
        self.load_persons(movies)
        self.load_genres(movies)
        self.load_countries(movies)
        self.load_languages(movies)
        self.load_keywords(movies)
        self.load_taglines(movies)
        self.load_vote_details(movies)
        self.load_ratings(movies)
        self.everything_to_movie(movies)
        self.close()
        if verbose:
            print("database loaded.")

    def load_table(self, tbl):
        try:
            self.c.execute("select * from {}".format(tbl))
        except pymysql.err.ProgrammingError:
            print("The {} table does not exist!".format(tbl))
            raise
        return self.c.fetchall()

    def load_movies(self):
        print("Loading movies...")
        movies = self.load_table('movies')
        return {movie['crit_id']: movie for movie in movies}

    def load_persons(self, movies):
        print("Loading people...")
        persons = sorted(self.load_table('persons'),
                         key=lambda k: (k['crit_id'], k['role'], k['rank']))
        cast = [person for person in persons if person['role'] == 'cast']
        cast_dict = {k: {'cast': [{'canonical_name': e['canonical_name'],
                                   'name': e['name'],
                                   'person_id': e['person_id']}
                                  for e in list(v)]}
                     for k, v in groupby(cast, key=lambda x: x['crit_id'])}
        for k, v in cast_dict.items():
            movies[k].update(v)
        directors = [person for person in persons if person['role'] == 'director']
        directors_dict = {k: {'director': [{'canonical_name': e['canonical_name'],
                                             'name': e['name'],
                                             'person_id': e['person_id']}
                                            for e in list(v)]}
                          for k, v in groupby(directors, key=lambda x: x['crit_id'])}
        for k, v in directors_dict.items():
            movies[k].update(v)
        writers = [person for person in persons if person['role'] == 'writer']
        writers_dict = {k: {'writer': [{'canonical_name': e['canonical_name'],
                                         'name': e['name'],
                                         'person_id': e['person_id']}
                                        for e in list(v)]}
                        for k, v in groupby(writers, key=lambda x: x['crit_id'])}
        for k, v in writers_dict.items():
            movies[k].update(v)

    def load_genres(self, movies):
        print("Loading genres...")
        genres = sorted(self.load_table('genres'), key=lambda k: k['crit_id'])
        genre_dict = {k: {'genres': [e['genre'] for e in list(v)]}
                      for k, v in groupby(genres, key=lambda x: x['crit_id'])}
        for k, v in genre_dict.items():
            movies[k].update(v)

    def load_countries(self, movies):
        print("Loading countries...")
        countries = sorted(self.load_table('countries'), key=lambda k: (k['crit_id'], k['rank']))
        country_dict = {k: {'countries': [e['country'] for e in list(v)]}
                        for k, v in groupby(countries, key=lambda x: x['crit_id'])}
        for k, v in country_dict.items():
            movies[k].update(v)

    def load_languages(self, movies):
        print("Loading languages...")
        languages = sorted(self.load_table('languages'), key=lambda k: (k['crit_id'], k['rank']))
        language_dict = {k: {'languages': [e['language'] for e in list(v)]}
                         for k, v in groupby(languages, key=lambda x: x['crit_id'])}
        for k, v in language_dict.items():
            movies[k].update(v)

    def load_keywords(self, movies):
        print("Loading keywords...")
        keywords = sorted(self.load_table('keywords'), key=lambda k: k['crit_id'])
        keyword_dict = {k: {'keywords': [e['keyword'] for e in list(v)]}
                        for k, v in groupby(keywords, key=lambda x: x['crit_id'])}
        for k, v in keyword_dict.items():
            movies[k].update(v)

    def load_taglines(self, movies):
        print("Loading taglines...")
        taglines = sorted(self.load_table('taglines'), key=lambda k: (k['crit_id'], k['rank']))
        taglines_dict = {k: {'taglines': [e['tagline'] for e in list(v)]}
                         for k, v in groupby(taglines, key=lambda x: x['crit_id'])}
        for k, v in taglines_dict.items():
            movies[k].update(v)

    def load_vote_details(self, movies):
        print("Loading vote details...")
        vote_details = sorted(self.load_table('vote_details'), key=lambda k: k['crit_id'])
        vote_details_dict = {k: {'vote_details': {e['demographic']: {'rating': e['rating'], 'votes': e['votes']}
                                                  for e in list(v)}}
                             for k, v in groupby(vote_details, key=lambda x: x['crit_id'])}
        for k, v in vote_details_dict.items():
            movies[k].update(v)

    def load_ratings(self, movies):
        print("Loading ratings...")
        ratings = sorted(self.load_table('ratings'), key=lambda k: k['crit_id'])
        ratings_dict = {crit_id: {'my_ratings': {user: {rating['type']: rating['score'] for rating in list(user_ratings)}
                                                 for user, user_ratings in groupby(crit_values, key=lambda x: x['user'])}}
                        for crit_id, crit_values in groupby(ratings, key=lambda x: x['crit_id'])}
        for k, v in ratings_dict.items():
            movies[k].update(v)

    def everything_to_movie(self, movies):
        print("Creating Movie objects...")
        for movie_info in movies.values():
            self.movies[movie_info['crit_id']] = Movie(movie_info)

    def set_movie(self, movie):
        if isinstance(movie, dict):
            if movie['crit_id'] in self.movies:
                self.movies[movie['crit_id']].update_from_dict(movie)
                movie = self.movies[movie['crit_id']]
            else:
                movie = Movie(movie)
                self.movies[movie.crit_id] = movie
        elif isinstance(movie, Movie):
            if movie.crit_id not in self.movies:
                self.movies[movie.crit_id] = movie
        else:
            raise Exception("No dict or Movie object was provided.")
        self.update_single_record('movies', self.movie_to_dict_movies(movie))
        self.update_multiple_records('persons', self.movie_to_dict_persons(movie))
        self.update_multiple_records('genres', self.movie_to_dict_genres(movie))
        self.update_multiple_records('countries', self.movie_to_dict_countries(movie))
        self.update_multiple_records('languages', self.movie_to_dict_languages(movie))
        self.update_multiple_records('keywords', self.movie_to_dict_keywords(movie))
        self.update_multiple_records('taglines', self.movie_to_dict_taglines(movie))
        self.update_multiple_records('ratings', self.movie_to_dict_ratings(movie))
        self.update_multiple_records('vote_details', self.movie_to_dict_vote_details(movie))

    def update_single_record(self, tbl, d):
        self.connect()
        d = self.make_dict_db_safe(d)
        sql = "insert into {} (".format(tbl)
        sql += ', '.join([k for k in sorted(d)])
        sql += ') values ('
        sql += ', '.join(['%s' for _ in d])
        sql += ') on duplicate key update '
        sql += ', '.join(['{} = %s'.format(k) for k in sorted(d)])
        values = [d[k] for k in sorted(d)] + [d[k] for k in sorted(d)]
        self.c.execute(sql, values)
        self.close()

    @staticmethod
    def create_insert_multiple_records_sql(tbl, d):
        keys = [key for key in list(d.keys()) if key not in ['crit_id', 'n_rows']]
        sql = "insert into {} (crit_id, {}) values ".format(tbl, ', '.join(keys))
        rows = []
        for i in range(d['n_rows']):
            rows.append("(%s, " + ', '.join(["%s" for _ in keys]) + ")")
        sql += ', '.join(rows)
        values = []
        for i in range(d['n_rows']):
            values += [d['crit_id']] + [d[key][i] for key in keys]
        return sql, values

    def update_multiple_records(self, tbl, d):
        if d['n_rows'] > 0:
            self.connect()
            # Delete existing rows
            sql = "delete from {} where crit_id = %s".format(tbl)
            values = [d['crit_id']]
            self.c.execute(sql, values)
            self.conn.commit()
            # Add new rows
            sql, values = self.create_insert_multiple_records_sql(tbl, d)
            self.c.execute(sql, values)
            self.close()

    def remove_table(self, table_name='movies'):
        self.connect()
        self.c.execute("drop table if exists {}".format(table_name))
        self.close()

    def add_column(self, column_name, column_datatype, table_name='movies', after=None):
        self.connect()
        sql = "alter table {} add column {} {}".format(table_name, column_name, column_datatype)
        if after is not None:
            sql += " after {}".format(after)
        try:
            self.c.execute(sql)
        except pymysql.err.InternalError as e:
            if not e.args[0] == 1060:
                print(e)
                raise Exception
            else:
                print("{} already in table {}".format(e.args[1], table_name))
        finally:
            self.close()

    def add_columns(self, columns, table_name='movies'):
        if isinstance(table_name, str):
            for column in columns:
                self.add_column(column['column_name'], column['column_datatype'],
                                table_name=table_name, after=column.get('after'))
        elif isinstance(table_name, list):
            for table in table_name:
                for column in columns:
                    self.add_column(column['column_name'], column['column_datatype'],
                                    table_name=table, after=column.get('after'))
        else:
            raise Exception

    def movie_to_dict_movies(self, movie):
        d = {k: v for k, v in vars(movie).items()
             if v is not None and k in self.columns_movies.keys()}
        return d

    @staticmethod
    def process_persons(person_dict, persons, role):
        person_ids = [person['person_id'] for person in persons]
        canonical_names = [person['canonical_name'] for person in persons]
        names = [person['name'] for person in persons]
        ranks = [i + 1 for i, person in enumerate(persons)]
        roles = [role for _ in persons]
        person_dict['person_id'] += person_ids
        person_dict['canonical_name'] += canonical_names
        person_dict['name'] += names
        person_dict['rank'] += ranks
        person_dict['role'] += roles

    def movie_to_dict_persons(self, movie):
        person_dict = {'crit_id': movie.crit_id,
                       'n_rows': 0,
                       'person_id': list(),
                       'canonical_name': list(),
                       'name': list(),
                       'rank': list(),
                       'role': list()}
        cast = movie.cast
        if cast is not None:
            self.process_persons(person_dict, cast, 'cast')
        directors = movie.director
        if directors is not None:
            self.process_persons(person_dict, directors, 'director')
        writers = movie.writer
        if writers is not None:
            self.process_persons(person_dict, writers, 'writer')
        person_dict['n_rows'] = len(person_dict['person_id'])
        return person_dict

    @staticmethod
    def movie_to_dict_genres(movie):
        genres_dict = {'crit_id': movie.crit_id,
                       'n_rows': 0,
                       'genre': list()}
        genres = movie.genres
        if genres is not None:
            genres_dict['genre'] = list(genres)
            genres_dict['n_rows'] = len(genres)
        return genres_dict

    @staticmethod
    def movie_to_dict_countries(movie):
        countries_dict = {'crit_id': movie.crit_id,
                          'n_rows': 0,
                          'country': list(),
                          'rank': list()}
        countries = movie.countries
        if countries is not None:
            countries_dict['country'] = list(countries)
            countries_dict['rank'] = list(range(1, len(countries)+1))
            countries_dict['n_rows'] = len(countries)
        return countries_dict

    @staticmethod
    def movie_to_dict_languages(movie):
        languages_dict = {'crit_id': movie.crit_id,
                          'n_rows': 0,
                          'language': list(),
                          'rank': list()}
        languages = movie.languages
        if languages is not None:
            languages_dict['language'] = sorted(list(languages))
            languages_dict['rank'] = list(range(1, len(languages)+1))
            languages_dict['n_rows'] = len(languages)
        return languages_dict

    @staticmethod
    def movie_to_dict_keywords(movie):
        keywords_dict = {'crit_id': movie.crit_id,
                         'n_rows': 0,
                         'keyword': list()}
        keywords = movie.keywords
        if keywords is not None:
            keywords_dict['keyword'] = sorted(list(keywords))
            keywords_dict['n_rows'] = len(keywords)
        return keywords_dict

    @staticmethod
    def movie_to_dict_taglines(movie):
        taglines_dict = {'crit_id': movie.crit_id,
                         'n_rows': 0,
                         'tagline': list(),
                         'rank': list()}
        taglines = movie.taglines
        if taglines is not None:
            taglines_dict['tagline'] = list(taglines)
            taglines_dict['rank'] = list(range(1, len(taglines)+1))
            taglines_dict['n_rows'] = len(taglines)
        return taglines_dict

    @staticmethod
    def movie_to_dict_vote_details(movie):
        vote_details_dict = {'crit_id': movie.crit_id,
                             'n_rows': 0,
                             'demographic': list(),
                             'rating': list(),
                             'votes': list()}
        vote_details = movie.vote_details
        if vote_details is not None:
            vote_details_dict['demographic'] = list(vote_details.keys())
            vote_details_dict['rating'] = [vote_details[demog]['rating'] for demog in vote_details]
            vote_details_dict['votes'] = [vote_details[demog]['votes'] for demog in vote_details]
            vote_details_dict['n_rows'] = len(vote_details.keys())
        return vote_details_dict

    @staticmethod
    def movie_to_dict_ratings(movie):
        ratings_dict = {'crit_id': movie.crit_id,
                        'n_rows': 0,
                        'user': list(),
                        'type': list(),
                        'score': list()}
        ratings = movie.my_ratings
        if ratings is not None:
            records = []
            for user in ratings:
                for rating_type in ratings[user]:
                    records.append({'user': user, 'type': rating_type, 'score': ratings[user][rating_type]})
            ratings_dict['user'] = [r['user'] for r in records]
            ratings_dict['type'] = [r['type'] for r in records]
            ratings_dict['score'] = [r['score'] for r in records]
            ratings_dict['n_rows'] = len(records)
        return ratings_dict

    def save_movies(self, movies):
        print("\nSaving movie information to the database...\n")
        time0 = time.time()
        for i, movie_info in enumerate(movies):
            self.set_movie(movie_info)
        time_taken = time.time() - time0
        print("...took {:.1f} minuters".format(time_taken/60))
        self.print()


class MovieNotInDatabaseError(Exception):
    def __init__(self, crit_id=None):
        self.crit_id = crit_id
