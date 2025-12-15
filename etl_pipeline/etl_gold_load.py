from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit

catalog = "nba"
silver_schema = "silver"
gold_schema = "gold"
game_table_name = "games"
boxscore_table_name = "game_boxscores"
officials_table_name = "game_officials"
teamstats_table_name = "game_team_stats"
players_table_name = "game_players"
player_stats_table_name = "game_player_stats"

@dp.table(name=f"{catalog}.{gold_schema}.{boxscore_table_name}")
def games():
  df = spark.read.table(f"{catalog}.{silver_schema}.{boxscore_table_name}")
  home_df = df.select(
    "game_id", "date", "regulation_time",
    col("home_team_id").alias("team_id"),
    lit("home").alias("team_type"),
    col("home_team_result").alias("team_result"),
    col("away_team_id").alias("opponent_id"),
    col("away_team_result").alias("opponent_result")
  )
  away_df = df.select(
      "game_id", "date", "regulation_time",
      col("away_team_id").alias("team_id"),
      lit("away").alias("team_type"),
      col("away_team_result").alias("team_result"),
      col("home_team_id").alias("opponent_id"),
      col("home_team_result").alias("opponent_result")
  )
  return home_df.unionByName(away_df)
  


