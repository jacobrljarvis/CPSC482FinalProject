-- bigquery_ml.sql
-- Analytics views and ML models. Run in BigQuery console after create_tables.sql.

-- ==================== VIEWS ====================

CREATE OR REPLACE VIEW nba_analytics.v_player_season_averages AS
SELECT player_name, season, team, COUNT(*) AS games_played,
  ROUND(AVG(minutes), 1) AS mpg, ROUND(AVG(points), 1) AS ppg,
  ROUND(AVG(rebounds), 1) AS rpg, ROUND(AVG(assists), 1) AS apg,
  ROUND(AVG(steals), 1) AS spg, ROUND(AVG(blocks), 1) AS bpg,
  ROUND(AVG(turnovers), 1) AS tpg, ROUND(AVG(fg_pct), 3) AS fg_pct,
  ROUND(AVG(fg3_pct), 3) AS fg3_pct, ROUND(AVG(ft_pct), 3) AS ft_pct,
  ROUND(AVG(true_shooting_pct), 3) AS ts_pct, ROUND(AVG(pts_per36), 1) AS pts_per36,
  ROUND(AVG(plus_minus), 1) AS avg_plus_minus,
  SUM(points) AS total_points, SUM(rebounds) AS total_rebounds, SUM(assists) AS total_assists
FROM nba_analytics.player_box_scores
GROUP BY player_name, season, team;

CREATE OR REPLACE VIEW nba_analytics.v_player_careers AS
SELECT player_name, MIN(season) AS first_season, MAX(season) AS last_season,
  COUNT(DISTINCT season) AS seasons_played, COUNT(*) AS career_games,
  SUM(points) AS career_points, SUM(rebounds) AS career_rebounds, SUM(assists) AS career_assists,
  ROUND(AVG(points), 1) AS career_ppg, ROUND(AVG(rebounds), 1) AS career_rpg,
  ROUND(AVG(assists), 1) AS career_apg, ROUND(AVG(true_shooting_pct), 3) AS career_ts_pct
FROM nba_analytics.player_box_scores
GROUP BY player_name HAVING COUNT(*) >= 50;

CREATE OR REPLACE VIEW nba_analytics.v_league_trends AS
SELECT season, COUNT(DISTINCT player_name) AS total_players,
  COUNT(*) AS total_game_entries,
  ROUND(AVG(points), 1) AS league_avg_ppg, ROUND(AVG(rebounds), 1) AS league_avg_rpg,
  ROUND(AVG(assists), 1) AS league_avg_apg, ROUND(AVG(fg_pct), 3) AS league_avg_fg_pct,
  ROUND(AVG(fg3_pct), 3) AS league_avg_fg3_pct, ROUND(AVG(fg3_attempted), 1) AS league_avg_fg3a,
  ROUND(AVG(true_shooting_pct), 3) AS league_avg_ts_pct
FROM nba_analytics.player_box_scores
GROUP BY season ORDER BY season;

CREATE OR REPLACE VIEW nba_analytics.v_player_stats_with_salary AS
SELECT psa.player_name, psa.season, psa.team, psa.games_played, psa.mpg,
  psa.ppg, psa.rpg, psa.apg, psa.ts_pct, psa.avg_plus_minus,
  sal.salary, sal.salary_inflation_adj,
  CASE WHEN psa.total_points > 0 THEN ROUND(sal.salary / psa.total_points, 0) ELSE NULL END AS cost_per_point,
  CASE WHEN sal.salary > 0 THEN ROUND(psa.ppg / (sal.salary / 1000000), 2) ELSE NULL END AS ppg_per_million
FROM nba_analytics.v_player_season_averages psa
JOIN nba_analytics.player_salaries sal
  ON psa.player_name = sal.player_name AND psa.season = sal.season;

-- ==================== ML TRAINING VIEW ====================

CREATE OR REPLACE VIEW nba_analytics.v_ml_salary_training AS
SELECT ss.player_name, ss.season, ss.position, ss.age, ss.games_played,
  ss.minutes_played AS mpg, ss.points AS ppg, ss.total_rebounds AS rpg,
  ss.assists AS apg, ss.steals AS spg, ss.blocks AS bpg,
  ss.fg_pct, ss.fg3_pct, ss.ft_pct, ss.efg_pct,
  sal.salary_inflation_adj AS salary
FROM nba_analytics.player_season_stats ss
JOIN nba_analytics.player_salaries sal
  ON ss.player_name = sal.player_name AND ss.season = sal.season
WHERE ss.games_played >= 20 AND ss.minutes_played >= 10 AND sal.salary_inflation_adj > 0;

-- ==================== ML MODELS ====================

-- Linear Regression
CREATE OR REPLACE MODEL nba_analytics.salary_predictor_linear
OPTIONS(model_type = 'LINEAR_REG', input_label_cols = ['salary'],
  data_split_method = 'AUTO_SPLIT', max_iterations = 20) AS
SELECT age, games_played, mpg, ppg, rpg, apg, spg, bpg, fg_pct, fg3_pct, efg_pct, salary
FROM nba_analytics.v_ml_salary_training
WHERE fg_pct IS NOT NULL AND fg3_pct IS NOT NULL AND efg_pct IS NOT NULL;

-- XGBoost
CREATE OR REPLACE MODEL nba_analytics.salary_predictor_xgboost
OPTIONS(model_type = 'BOOSTED_TREE_REGRESSOR', input_label_cols = ['salary'],
  data_split_method = 'AUTO_SPLIT', num_parallel_tree = 5, max_tree_depth = 6, subsample = 0.8) AS
SELECT age, games_played, mpg, ppg, rpg, apg, spg, bpg, fg_pct, fg3_pct, efg_pct, salary
FROM nba_analytics.v_ml_salary_training
WHERE fg_pct IS NOT NULL AND fg3_pct IS NOT NULL AND efg_pct IS NOT NULL;

-- ==================== EVALUATION ====================

SELECT 'Linear Regression' AS model, * FROM ML.EVALUATE(MODEL nba_analytics.salary_predictor_linear)
UNION ALL
SELECT 'XGBoost', * FROM ML.EVALUATE(MODEL nba_analytics.salary_predictor_xgboost);

-- Feature weights
SELECT * FROM ML.WEIGHTS(MODEL nba_analytics.salary_predictor_linear) ORDER BY ABS(weight) DESC;

-- ==================== PREDICTIONS ====================

CREATE OR REPLACE TABLE nba_analytics.salary_predictions AS
SELECT player_name, season, ppg, rpg, apg, age,
  salary AS actual_salary, predicted_salary,
  ROUND(salary - predicted_salary, 0) AS overpay_amount,
  ROUND((salary - predicted_salary) / NULLIF(predicted_salary, 0) * 100, 1) AS overpay_pct
FROM ML.PREDICT(MODEL nba_analytics.salary_predictor_xgboost,
  (SELECT * FROM nba_analytics.v_ml_salary_training
   WHERE fg_pct IS NOT NULL AND fg3_pct IS NOT NULL AND efg_pct IS NOT NULL))
ORDER BY overpay_pct DESC;

-- Most overpaid (2015+)
SELECT * FROM nba_analytics.salary_predictions WHERE season >= 2015 ORDER BY overpay_amount DESC LIMIT 20;

-- Best value (2015+)
SELECT * FROM nba_analytics.salary_predictions WHERE season >= 2015 ORDER BY overpay_amount ASC LIMIT 20;
