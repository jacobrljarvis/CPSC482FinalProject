-- create_tables.sql
-- Creates BigQuery dataset and loads processed Parquet from GCS.
-- Replace YOUR_BUCKET with your actual bucket name.

CREATE SCHEMA IF NOT EXISTS nba_analytics
OPTIONS(location = 'US', description = 'NBA Player Performance Analytics');

-- Load from Parquet via external tables
CREATE OR REPLACE EXTERNAL TABLE nba_analytics.player_box_scores_ext
OPTIONS (format = 'PARQUET', uris = ['gs://YOUR_BUCKET/processed/player_box_scores/*.parquet']);
CREATE OR REPLACE TABLE nba_analytics.player_box_scores AS
SELECT * FROM nba_analytics.player_box_scores_ext;

CREATE OR REPLACE EXTERNAL TABLE nba_analytics.player_season_stats_ext
OPTIONS (format = 'PARQUET', uris = ['gs://YOUR_BUCKET/processed/player_season_stats/*.parquet']);
CREATE OR REPLACE TABLE nba_analytics.player_season_stats AS
SELECT * FROM nba_analytics.player_season_stats_ext;

CREATE OR REPLACE EXTERNAL TABLE nba_analytics.player_salaries_ext
OPTIONS (format = 'PARQUET', uris = ['gs://YOUR_BUCKET/processed/player_salaries/*.parquet']);
CREATE OR REPLACE TABLE nba_analytics.player_salaries AS
SELECT * FROM nba_analytics.player_salaries_ext;

CREATE OR REPLACE EXTERNAL TABLE nba_analytics.team_payroll_ext
OPTIONS (format = 'PARQUET', uris = ['gs://YOUR_BUCKET/processed/team_payroll/*.parquet']);
CREATE OR REPLACE TABLE nba_analytics.team_payroll AS
SELECT * FROM nba_analytics.team_payroll_ext;

-- Cleanup external tables
DROP TABLE IF EXISTS nba_analytics.player_box_scores_ext;
DROP TABLE IF EXISTS nba_analytics.player_season_stats_ext;
DROP TABLE IF EXISTS nba_analytics.player_salaries_ext;
DROP TABLE IF EXISTS nba_analytics.team_payroll_ext;

-- Verify
SELECT 'player_box_scores' AS table_name, COUNT(*) AS row_count FROM nba_analytics.player_box_scores
UNION ALL SELECT 'player_season_stats', COUNT(*) FROM nba_analytics.player_season_stats
UNION ALL SELECT 'player_salaries', COUNT(*) FROM nba_analytics.player_salaries
UNION ALL SELECT 'team_payroll', COUNT(*) FROM nba_analytics.team_payroll;
