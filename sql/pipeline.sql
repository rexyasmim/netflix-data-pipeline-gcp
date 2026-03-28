CREATE OR REPLACE EXTERNAL TABLE `yasmim-pipeline-netflix.netflix_raw.raw_belief_data`
(
  userId STRING,
  movieId STRING,
  isSeen STRING,
  watchDate STRING,
  userElicitRating STRING,
  userPredictRating STRING,
  userCertainty STRING,
  tstamp STRING,
  month_idx STRING,
  source STRING,
  systemPredictRating STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://yasmim-bucket-netflix/bronze/belief_data.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

CREATE OR REPLACE EXTERNAL TABLE `yasmim-pipeline-netflix.netflix_raw.raw_user_rating_history`
(
  userId STRING,
  movieId STRING,
  rating STRING,
  timestamp STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://yasmim-bucket-netflix/bronze/user_rating_history.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

CREATE OR REPLACE EXTERNAL TABLE `yasmim-pipeline-netflix.netflix_raw.raw_ratings_for_additional_users`
(
  userId STRING,
  movieId STRING,
  rating STRING,
  timestamp STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://yasmim-bucket-netflix/bronze/ratings_for_additional_users.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

CREATE OR REPLACE EXTERNAL TABLE `yasmim-pipeline-netflix.netflix_raw.raw_movie_elicitation_set`
(
  movieId STRING,
  tstamp STRING,
  month_idx STRING,
  source STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://yasmim-bucket-netflix/bronze/movie_elicitation_set.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

CREATE OR REPLACE EXTERNAL TABLE `yasmim-pipeline-netflix.netflix_raw.raw_user_recommendation_history`
(
  userId STRING,
  movieId STRING,
  tstamp STRING,
  PredictRating STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://yasmim-bucket-netflix/bronze/user_recommendation_history.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);





CREATE OR REPLACE TABLE `yasmim-pipeline-netflix.netflix_analytical.dimension_movies` AS
SELECT
  SAFE_CAST(movieId AS INT64) AS movieId,
  CAST(title AS STRING) AS title,
  CAST(genres AS STRING) AS genres,
  SAFE_CAST(REGEXP_EXTRACT(CAST(title AS STRING), r'\((\d{4})\)\s*$') AS INT64) AS release_year
  FROM `yasmim-pipeline-netflix.netflix_raw.raw_movies`;



CREATE OR REPLACE TABLE `yasmim-pipeline-netflix.netflix_analytical.fact_ratings` AS
WITH all_ratings AS (
  SELECT
  SAFE_CAST(NULLIF(userId,'') AS INT64) AS user_id,
  SAFE_CAST(NULLIF(movieId,'') AS INT64) AS movie_id,
  SAFE_CAST(NULLIF(NULLIF(rating,'NA'),'') AS FLOAT64) AS rating,
  COALESCE(
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp),
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp)
  ) AS rating_ts,
  'user_rating_history' as src
  FROM `yasmim-pipeline-netflix.netflix_raw.raw_user_rating_history`

  UNION ALL

  SELECT
  SAFE_CAST(NULLIF(userId,'') AS INT64) AS user_id,
  SAFE_CAST(NULLIF(movieId,'') AS INT64) AS movie_id,
  SAFE_CAST(NULLIF(NULLIF(rating,'NA'),'') AS FLOAT64) AS rating,
  COALESCE(
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp),
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp)
  ) AS rating_ts,
  'rating_for_additional_users' as src
  FROM `yasmim-pipeline-netflix.netflix_raw.raw_ratings_for_additional_users`
)
SELECT 
  user_id,
  movie_id,
  rating,
  rating_ts,
  src
  from all_ratings
  where user_id IS NOT NULL 
  AND movie_id IS NOT NULL 
  AND rating IS NOT NULL 
  AND rating_ts IS NOT NULL 
  AND src IS NOT NULL;



CREATE OR REPLACE VIEW `yasmim-pipeline-netflix.netflix_analytical.vw_movies_kpis` AS
SELECT 
  r.movie_id,
  m.title,
  m.genres,
  m.release_year,
  COUNT(*) AS total_ratings,
  AVG(r.rating) AS avg_rating,
  STDDEV(r.rating) AS std_rating,
  MIN(r.rating_ts) AS first_rating_ts,
  MAX(r.rating_ts) AS last_rating_ts
FROM `yasmim-pipeline-netflix.netflix_analytical.fact_ratings` r
LEFT JOIN `yasmim-pipeline-netflix.netflix_analytical.dimension_movies` m on m.movieId = r.movie_id
GROUP BY 1,2,3,4;

CREATE OR REPLACE VIEW `yasmim-pipeline-netflix.netflix_analytical.vw_top_movies` AS
SELECT 
  movie_id,
  title,
  genres,
  release_year,
  total_ratings,
  ROUND(avg_rating,2) AS avg_rating
  FROM `yasmim-pipeline-netflix.netflix_analytical.vw_movies_kpis`
  WHERE total_ratings >= 20
  AND avg_rating BETWEEN 0 AND 5 
  ORDER BY 6 DESC, 5 DESC
  LIMIT 10;

CREATE OR REPLACE VIEW `yasmim-pipeline-netflix.netflix_analytical.vw_genre_performance` AS
WITH exploded AS (
  SELECT 
    r.rating,
    genre
  FROM `yasmim-pipeline-netflix.netflix_analytical.fact_ratings` r
  JOIN `yasmim-pipeline-netflix.netflix_analytical.dimension_movies` m 
  ON m.movieId = r.movie_id
  CROSS JOIN UNNEST(SPLIT(COALESCE(m.genres,''), '|')) AS genre
)
SELECT 
  genre,
  COUNT(*) AS total_ratings,
  AVG(rating) AS avg_rating,
  STDDEV(rating) AS std_rating,
FROM exploded
WHERE genre IS NOT NULL
AND genre != ''
AND genre != '(no genres listed)'
GROUP BY 1
ORDER BY 2 DESC, 3 DESC;

CREATE OR REPLACE VIEW `yasmim-pipeline-netflix.netflix_analytical.vw_ratings_heatmap` AS
SELECT 
  EXTRACT(YEAR FROM rating_ts) as year,
  EXTRACT(MONTH FROM rating_ts) as month,
  FORMAT_TIMESTAMP('%b', rating_ts) as month_name,
  COUNT(*) AS total_ratings
FROM `yasmim-pipeline-netflix.netflix_analytical.fact_ratings`
group by 1,2,3
order by 1,2;



CREATE OR REPLACE VIEW `yasmim-pipeline-netflix.netflix_analytical.vw_scatter_popularity_vs_quality` AS
SELECT 
  movie_id,
  title,
  genres,
  release_year,
  total_ratings,
  avg_rating
FROM `yasmim-pipeline-netflix.netflix_analytical.vw_movies_kpis`
WHERE total_ratings >= 50
