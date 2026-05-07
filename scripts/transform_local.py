"""
transform_local.py
Local pandas version of the PySpark transformations.
Use to test and verify before running on Dataproc.

Usage:
    pip install pandas pyarrow
    python scripts/transform_local.py
"""

import os
import pandas as pd
import numpy as np

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
os.makedirs(OUT_DIR, exist_ok=True)


def transform_box_scores():
    print("1. Transforming box scores (this may take a minute)...")
    df = pd.read_csv(
        os.path.join(RAW_DIR, "NBA Player Box Score Stats(1950 - 2022).csv"),
        index_col=0, low_memory=False,
    )
    df = df.rename(columns={
        "Season": "season", "Game_ID": "game_id", "PLAYER_NAME": "player_name",
        "Team": "team", "GAME_DATE": "game_date", "MATCHUP": "matchup",
        "WL": "win_loss", "MIN": "minutes", "FGM": "fg_made", "FGA": "fg_attempted",
        "FG_PCT": "fg_pct", "FG3M": "fg3_made", "FG3A": "fg3_attempted",
        "FG3_PCT": "fg3_pct", "FTM": "ft_made", "FTA": "ft_attempted",
        "FT_PCT": "ft_pct", "OREB": "off_rebounds", "DREB": "def_rebounds",
        "REB": "rebounds", "AST": "assists", "STL": "steals", "BLK": "blocks",
        "TOV": "turnovers", "PF": "personal_fouls", "PTS": "points",
        "PLUS_MINUS": "plus_minus",
    })
    df = df.drop(columns=["VIDEO_AVAILABLE"], errors="ignore")
    df["game_date"] = pd.to_datetime(df["game_date"], format="mixed", errors="coerce")
    df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce")
    df = df[df["minutes"].notna() & (df["minutes"] > 0)].copy()
    df["pts_per36"] = (df["points"] / df["minutes"] * 36).round(1)
    df["reb_per36"] = (df["rebounds"] / df["minutes"] * 36).round(1)
    df["ast_per36"] = (df["assists"] / df["minutes"] * 36).round(1)
    denom = 2 * (df["fg_attempted"] + 0.44 * df["ft_attempted"])
    df["true_shooting_pct"] = np.where(denom > 0, (df["points"] / denom).round(3), np.nan)
    df["is_home"] = df["matchup"].str.contains("vs.", na=False)
    df = df.drop_duplicates(subset=["player_name", "game_id"])
    print(f"   {len(df):,} rows after cleaning")
    df.to_parquet(os.path.join(OUT_DIR, "player_box_scores.parquet"), index=False)
    return df


def transform_season_stats():
    print("\n2. Transforming season stats...")
    df = pd.read_csv(os.path.join(RAW_DIR, "NBA Player Stats(1950 - 2022).csv"), index_col=0)
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    df = df.rename(columns={
        "Season": "season", "Player": "player_name", "Pos": "position",
        "Age": "age", "Tm": "team", "G": "games_played", "GS": "games_started",
        "MP": "minutes_played", "FG": "fg_made", "FGA": "fg_attempted",
        "FG%": "fg_pct", "3P": "fg3_made", "3PA": "fg3_attempted",
        "3P%": "fg3_pct", "2P": "fg2_made", "2PA": "fg2_attempted",
        "2P%": "fg2_pct", "eFG%": "efg_pct", "FT": "ft_made",
        "FTA": "ft_attempted", "FT%": "ft_pct", "ORB": "off_rebounds",
        "DRB": "def_rebounds", "TRB": "total_rebounds", "AST": "assists",
        "STL": "steals", "BLK": "blocks", "TOV": "turnovers",
        "PF": "personal_fouls", "PTS": "points",
    })
    df = df[df["team"] != "TOT"].copy()
    df = df.drop_duplicates(subset=["player_name", "season", "team"])
    print(f"   {len(df):,} rows after cleaning")
    df.to_parquet(os.path.join(OUT_DIR, "player_season_stats.parquet"), index=False)
    return df


def transform_salaries():
    print("\n3. Transforming salaries...")
    df = pd.read_csv(os.path.join(RAW_DIR, "NBA Salaries(1990-2023).csv"), index_col=0)
    df = df.rename(columns={
        "playerName": "player_name", "seasonStartYear": "season",
        "salary": "salary", "inflationAdjSalary": "salary_inflation_adj",
    })
    for col in ["salary", "salary_inflation_adj"]:
        df[col] = df[col].astype(str).str.replace(r"[$,]", "", regex=True)
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df = df.dropna(subset=["salary"])
    df = df.drop_duplicates(subset=["player_name", "season"])
    print(f"   {len(df):,} rows after cleaning")
    df.to_parquet(os.path.join(OUT_DIR, "player_salaries.parquet"), index=False)
    return df


def transform_payroll():
    print("\n4. Transforming team payroll...")
    df = pd.read_csv(os.path.join(RAW_DIR, "NBA Payroll(1990-2023).csv"), index_col=0)
    df = df.rename(columns={
        "team": "team_name", "seasonStartYear": "season",
        "payroll": "payroll", "inflationAdjPayroll": "payroll_inflation_adj",
    })
    for col in ["payroll", "payroll_inflation_adj"]:
        df[col] = df[col].astype(str).str.replace(r"[$,]", "", regex=True)
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df = df.dropna(subset=["payroll"])
    print(f"   {len(df):,} rows after cleaning")
    df.to_parquet(os.path.join(OUT_DIR, "team_payroll.parquet"), index=False)
    return df


if __name__ == "__main__":
    print("=" * 55)
    print("NBA Data Transformation (Local / Pandas)")
    print("=" * 55)
    box = transform_box_scores()
    stats = transform_season_stats()
    sal = transform_salaries()
    pay = transform_payroll()
    print(f"\nAll transformations complete!")
    print(f"  player_box_scores:   {len(box):>10,} rows")
    print(f"  player_season_stats: {len(stats):>10,} rows")
    print(f"  player_salaries:     {len(sal):>10,} rows")
    print(f"  team_payroll:        {len(pay):>10,} rows")
