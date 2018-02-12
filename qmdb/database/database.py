import sqlite3
import os
from qmdb.movie.movie import Movie
from arrow import Arrow
import arrow
import copy
from qmdb.config import config
import pymysql.cursors


class Database:
    def __init__(self, from_scratch=False, movies_table='movies'):
        self.movies = {}
        self.movies_table = movies_table
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

    def set_movie(self, movie, overwrite=True):
        crit_id = movie.crit_id
        if not overwrite:
            if crit_id in self.movies:
                print("Movie CritickerID {} is already in the database".format(crit_id))
        else:
            if crit_id in self.movies:
                self.movies[crit_id].update_from_dict(movie.to_dict())
            else:
                self.movies[crit_id] = movie
            self.update_record(self.movies_table, self.movies[crit_id].to_dict(), 'crit_id')

    def get_movie(self, crit_id):
        try:
            return self.movies[crit_id]
        except KeyError as e:
            print("CritickerID {} does not exist in the database.".format(e.args[0]))
            raise MovieNotInDatabaseError(crit_id=crit_id)

    def print(self):
        movies = sorted(list(self.movies.values()),
                        key=lambda x: max(arrow.get('1970-01-01') if x.crit_updated is None else x.crit_updated,
                                          arrow.get('1970-01-01') if x.omdb_updated is None else x.omdb_updated),
                        reverse=True)
        for movie in movies[:10]:
            self.movies[movie.crit_id].print()

    @staticmethod
    def make_dict_db_safe(d):
        d = copy.deepcopy(d)
        for k in d:
            if isinstance(d[k], Arrow):
                d[k] = d[k].format()
        return d

    def update_record(self, tbl, d, key):
        raise NotImplementedError


class SQLiteDatabase(Database):
    def __init__(self, filename, from_scratch=False):
        self.filename = filename
        self.columns_movies = {
            'crit_id': 'integer primary key',
            'crit_popularity_page': 'integer',
            'crit_url': 'text',
            'title': 'text',
            'year': 'integer',
            'imdbid': 'integer',
            'tomato_url': 'text',
            'date_added': 'text',
            'omdb_updated': 'text',
            'crit_updated': 'text'
        }
        super().__init__(from_scratch=from_scratch)

    def load_or_initialize(self, from_scratch=False):
        if from_scratch:
            self.initialize()
        else:
            try:
                self.load()
            except FileNotFoundError:
                print("The file containing the database can't be found. " +
                      "Therefore, I'm initalizing the database from scratch")
                self.initialize()

    def connect(self, from_scratch=False):
        if not from_scratch and not os.path.exists(self.filename):
            raise FileNotFoundError
        else:
            self.conn = sqlite3.connect(self.filename)
            self.c = self.conn.cursor()

    def initialize(self):
        try:
            os.remove(self.filename)
        except OSError:
            pass
        print("Initializing database at {}".format(self.filename))
        self.connect(from_scratch=True)
        with self.conn:
            sql = """
                CREATE TABLE {} (
                    {}
                )
                """.format(self.movies_table, ', '.join(["{} {}".format(k, v) for k, v in self.columns_movies.items()]))
            self.c.execute(sql)
        self.close()

    def load(self):
        self.connect()
        try:
            movies_iterator = self.c.execute("select * from {}".format(self.movies_table))
        except sqlite3.OperationalError:
            print("There is something wrong with the database file!")
            raise
        self.movies = {movie[0]: self.load_movie(movie) for movie in movies_iterator}
        self.close()
        print("database loaded.")

    def load_movie(self, movie_info_tuple):
        movie_info_dict = {k: movie_info_tuple[i] for i, k in enumerate(self.columns_movies.keys())}
        return Movie(movie_info_dict)

    def update_record(self, tbl, d, key):
        self.connect()
        d = self.make_dict_db_safe(d)
        sql = "update {} set ".format(tbl)
        sql += ', '.join([" {} = ? ".format(k) for k in sorted(d)])
        sql += 'where {} = ?'.format(key)
        values = tuple([d[k] for k in sorted(d)] + [d[key]])
        self.c.execute(sql, values)
        sql = "insert into {} ({}) values ({})".format(
            tbl, ', '.join([k for k in sorted(d)]), ', '.join(['?' for _ in d])
        )
        values = tuple([d[k] for k in sorted(d)])
        try:
            self.c.execute(sql, values)
        except sqlite3.IntegrityError:
            pass
        self.close()


class MySQLDatabase(Database):
    def __init__(self, from_scratch=False, schema='qmdb', movies_table='movies'):
        self.schema = schema
        self.columns_movies = {
            'crit_id': 'mediumint unsigned primary key',
            'crit_popularity_page': 'smallint unsigned not null',
            'crit_url': 'varchar(256) not null',
            'title': 'varchar(256) not null',
            'year': 'smallint unsigned',
            'imdbid': 'mediumint unsigned',
            'tomato_url': 'varchar(256)',
            'date_added': 'varchar(32) not null',
            'omdb_updated': 'varchar(32)',
            'crit_updated': 'varchar(32)'
        }
        super().__init__(from_scratch=from_scratch, movies_table=movies_table)

    def load_or_initialize(self, from_scratch=False):
        if from_scratch:
            self.initialize()
        else:
            try:
                self.load()
            except pymysql.err.ProgrammingError:
                print("The movies table does not exist yet. " +
                      "Therefore, I'm initalizing the table from scratch")
                self.initialize()

    def connect(self, from_scratch=False):
        self.conn = pymysql.connect(host=config.mysql['host'],
                                    user=config.mysql['username'],
                                    password=config.mysql['password'],
                                    db=self.schema,
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)
        self.c = self.conn.cursor()

    def initialize(self):
        self.connect()
        self.c.execute("drop table if exists {}".format(self.movies_table))
        print("Initializing database")
        with self.conn:
            sql = """
                CREATE TABLE {} (
                    {}
                )
                """.format(self.movies_table, ', '.join(["{} {}".format(k, v) for k, v in self.columns_movies.items()]))
            self.c.execute(sql)
        self.close()

    def load(self, verbose=False):
        self.connect()
        try:
            self.c.execute("select * from {}".format(self.movies_table))
            movies = self.c.fetchall()
        except pymysql.err.ProgrammingError:
            print("The movies table does not exist!")
            raise
        self.movies = {movie['crit_id']: Movie(movie) for movie in movies}
        self.close()
        if verbose:
            print("database loaded.")

    def update_record(self, tbl, d, key):
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

    def remove_table(self, table_name='movies'):
        self.connect()
        self.c.execute("drop table if exists {}".format(table_name))
        self.close()


class MovieNotInDatabaseError(Exception):
    def __init__(self, crit_id=None):
        self.crit_id = crit_id
