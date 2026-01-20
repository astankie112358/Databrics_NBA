from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit
from pyspark.sql import functions as F
from utilities.schemas import Schema

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

@dp.table(name=f"{catalog}.{gold_schema}.{officials_table_name}")
def officials():
  df = spark.read.table(f"{catalog}.{silver_schema}.{officials_table_name}")
  return df

@dp.table(name=f"{catalog}.{gold_schema}.{teamstats_table_name}",)
def pivoted_teamstats():
    column_types=Schema.team_stats_gold()
    df = spark.read.table(f"{catalog}.{silver_schema}.{teamstats_table_name}")
    agg_exprs = [
        F.first(
            F.when(F.col("stat_type") == stat, F.col("stat_value")), 
            ignorenulls=True
        ).alias(stat)
        for stat in column_types
    ]
    df = df.groupBy("game_id","team_id","against_team_id","home","loaded_date").agg(*agg_exprs)
    df = df.select("game_id","team_id","against_team_id","home","loaded_date",
      *[F.col(c_name).cast(c_type) for c_name,c_type in column_types.items()]
    )
    return df
  
@dp.table(name=f"{catalog}.{gold_schema}.{players_table_name}")
def players():
  df = spark.read.table(f"{catalog}.{silver_schema}.{players_table_name}")
  return df

@dp.table(name=f"{catalog}.{gold_schema}.player_dictionary")
def players():
  df = spark.read.table(f"{catalog}.{silver_schema}.player_dictionary")
  return df

@dp.table(name=f"{catalog}.{gold_schema}.{player_stats_table_name}")
def player_stats():
  column_types=Schema.player_stats_gold()
  df=spark.read.table(f"{catalog}.{silver_schema}.{player_stats_table_name}")
  agg_exprs = [
        F.first(
            F.when(F.col("stat_type") == stat, F.col("stat_value")), 
            ignorenulls=True
        ).alias(stat)
        for stat in column_types
    ]
  df = df.groupBy("game_id","team_id","player_id").agg(*agg_exprs)
  df = df.select("game_id","team_id","player_id",
    *[F.col(c_name).cast(c_type) for c_name,c_type in column_types.items()]
  )
  return df

@dp.table(name=f"{catalog}.{gold_schema}.{game_table_name}")
def games():
  df = spark.read.table(f"{catalog}.{silver_schema}.{game_table_name}")
  return df


