SELECT m.crit_id,
       m.imdbid,
       m.year,
       m.imdb_year,
       m.title,
       m.imdb_title,
       m.original_title,
       m.crit_rating,
       m.crit_votes,
       m.imdb_rating,
       m.imdb_votes,
       m.metacritic_score,
       m.kind,
       m.runtime,
       m.plot_summary,
       m.plot_storyline,
       m.original_release_date,
       m.dutch_release_date,
       m.crit_popularity,
       m.crit_url,
       m.tomato_url,
       m.poster_url,
       m.trailer_url,
       m.ptp_url,
       m.ptp_hd_available,
       m.netflix_id,
       m.netflix_title,
       m.netflix_rating,
       m.date_added,
       m.criticker_updated,
       m.imdb_main_updated,
       m.imdb_release_updated,
       m.imdb_metacritic_updated,
       m.imdb_keywords_updated,
       m.imdb_taglines_updated,
       m.imdb_vote_details_updated,
       m.imdb_plot_updated,
       m.omdb_updated,
       m.ptp_updated,
       m.netflix_updated,
       g.genres,
       l.languages,
       c.countries,
       a.actors,
       d.directors,
       w.writers,
       r.rating_tijl,
       r.psi_tijl,
       r.pred_rating_tijl,
       r.pred_seeit_tijl,
       r.pred_score_tijl,
       r.seen_probability_tijl
  from qmdb.movies as m
       left join (select crit_id,
                         group_concat(genre separator ' / ') as genres
                    from qmdb.genres
                   group by crit_id) as g
              on m.crit_id = g.crit_id
       left join (select crit_id,
                         group_concat(language order by rank separator ' / ') as languages
                    from qmdb.languages
                   group by crit_id) as l
              on m.crit_id = l.crit_id
       left join (select crit_id,
                         group_concat(country order by rank separator ' / ') as countries
                    from qmdb.countries
                   group by crit_id) as c
              on m.crit_id = c.crit_id
       left join (select crit_id,
                         group_concat(name order by rank separator ' / ') as actors
                    from qmdb.persons
                   where role = 'cast'
                     and rank <= 5
                   group by crit_id) as a
              on m.crit_id = a.crit_id
       left join (select crit_id,
                         group_concat(name order by rank separator ' / ') as directors
                    from qmdb.persons
                   where role = 'director'
                     and rank <= 3
                   group by crit_id) as d
              on m.crit_id = d.crit_id
       left join (select crit_id,
                         group_concat(name order by rank separator ' / ') as writers
                    from qmdb.persons
                   where role = 'writer'
                     and rank <= 3
                   group by crit_id) as w
              on m.crit_id = w.crit_id
       left join (select crit_id,
                         avg(case when user = 'tijl' and type = 'rating' then score else null end) as rating_tijl,
                         avg(case when user = 'tijl' and type = 'psi' then score else null end) as psi_tijl,
                         avg(case when user = 'tijl' and type = 'pred_rating' then score else null end) as pred_rating_tijl,
                         avg(case when user = 'tijl' and type = 'pred_seeit' then score else null end) as pred_seeit_tijl,
                         avg(case when user = 'tijl' and type = 'seen_probability' then score else null end) as seen_probability_tijl,
                         avg(case when user = 'tijl' and type = 'pred_score' then score else null end) as pred_score_tijl
                    from qmdb.ratings
                   group by crit_id) as r
              on m.crit_id = r.crit_id