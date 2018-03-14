import sys
from qmdb.database.database import MySQLDatabase
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import cross_val_score, GridSearchCV, train_test_split
from sklearn.metrics import mean_squared_error, roc_auc_score, r2_score
from sklearn.pipeline import Pipeline
import pandas as pd
import numpy as np
from sklearn.preprocessing import Imputer
import arrow
from scipy.stats import weibull_min
from qmdb.movie.movie import Movie


def ifnull(var, val):
    if isinstance(var, arrow.Arrow):
        return var
    if var is None or np.isnan(var):
        return val
    return var


def movie_kind_to_dict(kind):
    if isinstance(kind, str):
        return {'kind_' + kind.replace(" ", "_").lower(): 1}
    else:
        return {}


def movie_genres_to_dict(genres):
    if isinstance(genres, list):
        return {'genre_' + genre.replace(" ", "_").lower(): 1 for genre in genres}
    else:
        return {}


def my_weibull(x, c=0.6, a=0.8, b=3):
    return np.real(1 - weibull_min.cdf(x ** a / b, c))


class RatingModeler:
    def __init__(self, db):
        self.db = db
        self.movies = None
        self.movies_train = None
        self.movies_test = None
        self.dict_vectorizer_kind = None
        self.dict_vectorizer_genre = None
        self.cols_to_use = None

    def load_movies(self):
        movies = pd.DataFrame([self.db.movie_to_dict_movies(self.db.movies[crit_id]) for crit_id in self.db.movies])
        movies['release_date'] = movies.apply(lambda r: ifnull(r['dutch_release_date'],
                                                               ifnull(r['original_release_date'],
                                                                      arrow.get(str(ifnull(r['imdb_year'],
                                                                                           ifnull(r['year'],
                                                                                                  1900))) + '-01-01'))),
                                              axis=1)
        movies['years_since_release'] = movies['release_date'].apply(lambda r: (arrow.now() - r).days / 365.25)
        cols = ['crit_id', 'imdbid', 'title', 'year', 'crit_rating', 'crit_votes', 'imdb_rating',
                'imdb_votes', 'kind', 'metacritic_score', 'runtime', 'years_since_release']
        movies = movies[cols].set_index('crit_id')
        print("{} movies in this dataframe.".format(len(movies)))
        return movies

    @staticmethod
    def get_crit_id_and_score(movie, type='rating'):
        crit_id = movie.crit_id
        if 'tijl' in movie.my_ratings:
            rating = movie.my_ratings['tijl'].get(type)
        else:
            rating = None
        return {'crit_id': crit_id, type: rating}

    def load_ratings(self):
        ratings = pd.DataFrame([self.get_crit_id_and_score(self.db.movies[crit_id], type='rating')
                                for crit_id in self.db.movies]).set_index('crit_id')
        print("{} movies in this dataframe.".format(len(ratings)))
        return ratings

    def load_psis(self):
        psis = pd.DataFrame([self.get_crit_id_and_score(self.db.movies[crit_id], type='psi')
                                for crit_id in self.db.movies]).set_index('crit_id')
        print("{} movies in this dataframe.".format(len(psis)))
        return psis

    def load_genres(self):
        genres = pd.DataFrame([{'crit_id': self.db.movies[crit_id].crit_id, 'genres': self.db.movies[crit_id].genres}
                               for crit_id in self.db.movies]).set_index('crit_id')
        print("{} movies in this dataframe.".format(len(genres)))
        return genres

    def load_data(self):
        print("Loading data to be used for training...")
        movies_raw = self.load_movies()
        ratings = self.load_ratings()
        psis = self.load_psis()
        genres = self.load_genres()

        movies = pd.concat([movies_raw, ratings, psis, genres], axis=1, join='outer')
        movies['rated'] = movies['rating'].apply(lambda x: 1 if x > 0 else 0)
        print("{} movies in this dataframe.".format(len(movies)))
        movies = movies[movies['imdbid'] > 0]
        print("{} movies in this dataframe.".format(len(movies)))
        self.movies = movies

    def split_train_test(self):
        self.movies_train, self.movies_test = train_test_split(self.movies, test_size=0.33, random_state=42)
        print("{} movies in the training set.".format(len(self.movies_train)))
        print("{} movies in the test set.".format(len(self.movies_test)))

    def do_one_hot_encoding(self):
        self.dict_vectorizer_kind = DictVectorizer(sparse=False)
        D_train = [movie_kind_to_dict(movie['kind']) for i, movie in self.movies_train.iterrows()]
        D_test = [movie_kind_to_dict(movie['kind']) for i, movie in self.movies_test.iterrows()]
        X_train = self.dict_vectorizer_kind.fit_transform(D_train)
        X_test = self.dict_vectorizer_kind.transform(D_test)
        ohe_kind_train = pd.DataFrame(X_train, columns=self.dict_vectorizer_kind.feature_names_,
                                      index=self.movies_train.index)
        ohe_kind_test = pd.DataFrame(X_test, columns=self.dict_vectorizer_kind.feature_names_,
                                     index=self.movies_test.index)

        self.dict_vectorizer_genre = DictVectorizer(sparse=False)
        D_train = [movie_genres_to_dict(movie['genres']) for i, movie in self.movies_train.iterrows()]
        D_test = [movie_genres_to_dict(movie['genres']) for i, movie in self.movies_test.iterrows()]
        X_train = self.dict_vectorizer_genre.fit_transform(D_train)
        X_test = self.dict_vectorizer_genre.transform(D_test)
        ohe_genre_train = pd.DataFrame(X_train, columns=self.dict_vectorizer_genre.feature_names_,
                                       index=self.movies_train.index)
        ohe_genre_test = pd.DataFrame(X_test, columns=self.dict_vectorizer_genre.feature_names_,
                                      index=self.movies_test.index)

        self.movies_train = pd.concat([self.movies_train, ohe_kind_train, ohe_genre_train], axis=1, join='outer')
        self.movies_test = pd.concat([self.movies_test, ohe_kind_test, ohe_genre_test], axis=1, join='outer')
        self.movies = self.movies_train.append(self.movies_test)

    def train_rating_model(self):
        rated_movies_train = self.movies_train[self.movies_train['rated'] == 1]
        rated_movies_test = self.movies_test[self.movies_test['rated'] == 1]
        self.cols_to_use = [col for col in rated_movies_train.columns
                       if col not in ['imdbid', 'crit_id', 'title', 'kind', 'rating', 'rated', 'genres', 'year']]
        X_train = np.array(rated_movies_train[self.cols_to_use])
        X_test = np.array(rated_movies_test[self.cols_to_use])
        y_train = np.array(rated_movies_train['rating'])
        y_test = np.array(rated_movies_test['rating'])

        pipe = Pipeline([("imputer", Imputer(missing_values="NaN",
                                             strategy="mean",
                                             axis=0)),
                         ("forest", RandomForestRegressor())])

        param_grid = [{'imputer': [Imputer(missing_values="NaN",
                                           strategy="mean",
                                           axis=0)]},
                      {'forest': [RandomForestRegressor()],
                       'forest__max_features': ['sqrt'], # ['auto', 'sqrt']
                       'forest__max_depth': [20], # [5, 10, 20]
                       'forest__n_estimators': [200]}]  # [100, 200, 500]

        grid = GridSearchCV(pipe, cv=10, n_jobs=-1, param_grid=param_grid,
                            scoring='neg_mean_squared_error', return_train_score=True, verbose=1)
        grid.fit(X_train, y_train)
        print("Cross-validation RMSE: {:.3f}".format(np.sqrt(-grid.best_score_)))
        estimator = grid.best_estimator_
        y_pred = estimator.predict(X_test)
        test_rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        test_r2 = np.sqrt(r2_score(y_test, y_pred))
        print("            Test RMSE: {:.3f}".format(test_rmse))
        print("            Test R^2: {:.3f}".format(test_r2))
        print("\nBest model:")
        print("  max_features: {}".format(grid.best_params_['forest__max_features']))
        print("  max_depth: {}".format(grid.best_params_['forest__max_depth']))
        print("  n_estimators: {}\n".format(grid.best_params_['forest__n_estimators']))

        X_train_test = np.concatenate((X_train, X_test))
        y_train_test = np.concatenate((y_train, y_test))
        estimator.fit(X_train_test, y_train_test)
        feature_importances = pd.DataFrame(dict(zip(self.cols_to_use, estimator.named_steps['forest'].feature_importances_)),
                                           index=['feature_importance']) \
            .T.sort_values(by='feature_importance', ascending=False)
        X = np.array(self.movies[self.cols_to_use])
        rating_preds = pd.DataFrame([{'title': tuple[0], 'year': tuple[1], 'pred_rating': tuple[2]}
                                     for tuple in
                                     zip(list(self.movies['title']), list(self.movies['year']), list(estimator.predict(X)))])
        rating_preds = rating_preds.set_index(pd.Index(list(self.movies_train.index) + list(self.movies_test.index)))
        rating_preds = rating_preds.sort_values(by='pred_rating', ascending=False)
        return rating_preds

    def train_seen_model(self):
        self.cols_to_use = [col for col in self.movies_train.columns
                       if col not in ['imdbid', 'crit_id', 'title', 'kind', 'rating', 'rated', 'genres', 'year']]
        X_train = np.array(self.movies_train[self.cols_to_use])
        X_test = np.array(self.movies_test[self.cols_to_use])
        y_train = np.array(self.movies_train['rated'])
        y_test = np.array(self.movies_test['rated'])

        pipe = Pipeline([("imputer", Imputer(missing_values="NaN",
                                             strategy="mean",
                                             axis=0)),
                         ("forest", RandomForestClassifier())])

        param_grid = [{'imputer': [Imputer(missing_values="NaN",
                                           strategy="mean",
                                           axis=0)]},
                      {'forest': [RandomForestClassifier()],
                       'forest__max_features': ['sqrt'], # ['auto', 'sqrt']
                       'forest__max_depth': [10], # [5, 10, 20]
                       'forest__n_estimators': [500]}] # [100, 200, 500]

        grid = GridSearchCV(pipe, cv=10, n_jobs=-1, param_grid=param_grid, scoring='roc_auc',
                            return_train_score=True, verbose=1)
        grid.fit(X_train, y_train)
        print("Cross-validation AUC: {:.3f}".format(grid.best_score_))
        estimator = grid.best_estimator_
        y_pred = estimator.predict_proba(X_test)[:, 1]
        test_auc = roc_auc_score(y_test, y_pred)
        print("            Test AUC: {:.3f}".format(test_auc))
        print("\nBest model:")
        print("  max_features: {}".format(grid.best_params_['forest__max_features']))
        print("  max_depth: {}".format(grid.best_params_['forest__max_depth']))
        print("  n_estimators: {}".format(grid.best_params_['forest__n_estimators']))

        X_train_test = np.concatenate((X_train, X_test))
        y_train_test = np.concatenate((y_train, y_test))
        estimator.fit(X_train_test, y_train_test)
        feature_importances = pd.DataFrame(dict(zip(self.cols_to_use, estimator.named_steps['forest'].feature_importances_)),
                                           index=['feature_importance']) \
            .T.sort_values(by='feature_importance', ascending=False)

        X = np.array(self.movies[self.cols_to_use])
        seen_preds = pd.DataFrame([{'title': tuple[0], 'year': tuple[1], 'seen_probability': tuple[2]}
                                   for tuple in zip(list(self.movies['title']), list(self.movies['year']),
                                                    list(estimator.predict_proba(X)[:, 1]))])
        seen_preds = seen_preds.set_index(pd.Index(list(self.movies_train.index) + list(self.movies_test.index)))
        seen_preds = seen_preds.sort_values(by='seen_probability', ascending=False)
        return seen_preds

    def get_predictions(self):
        self.load_data()
        self.split_train_test()
        self.do_one_hot_encoding()
        seen_preds = self.train_seen_model()
        rating_preds = self.train_rating_model()
        scored_movies = pd.concat([self.movies, seen_preds[['seen_probability']], rating_preds[['pred_rating']]],
                                  axis=1, join='outer')
        scored_movies['pred_seeit'] = scored_movies \
            .apply(lambda row: np.power(row['seen_probability'] * my_weibull(row['years_since_release']) ** 3, 1 / 4),
                   axis=1)
        scored_movies['random'] = np.random.rand(len(scored_movies))
        scored_movies['show'] = scored_movies.apply(lambda row: 1 if row['pred_seeit'] >= row['random'] else 0, axis=1)
        scored_movies['score'] = scored_movies.apply(
            lambda row: np.power(row['pred_seeit'] * (row['pred_rating'] / 100) ** 3, 1 / 4) * 100, axis=1)
        movie_dicts = [{'crit_id': int(i),
                        'my_ratings': {'tijl': {'seen_probability': float(row['seen_probability']),
                                                'pred_rating': float(row['pred_rating']),
                                                'pred_seeit': float(row['pred_seeit']),
                                                'pred_score': float(row['score'])}}} for i, row in scored_movies.iterrows()]
        self.db.save_movies(movie_dicts)