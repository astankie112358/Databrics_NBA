from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, expr, collect_set, concat_ws, filter, max, last
from utilities.rules import Rules

catalog = "nba"
bronze_schema = "bronze"
silver_schema = "silver"
game_table_name = "games"

# Games table
game_rules = Rules.game_rules()
@dp.view(name = "games_clean")
@dp.expect_all_or_drop(game_rules)
def games_clean():
    return (
        spark.readStream.table(f"{catalog}.{bronze_schema}.{game_table_name}")
    )
    
@dp.table(name = f"{catalog}.{silver_schema}.err_{game_table_name}")
def games_quarantine():
    df = spark.readStream.table(f"{catalog}.{bronze_schema}.{game_table_name}")
    failed_df = Rules().filter_failed(df, game_rules)
    return failed_df

dp.create_streaming_table(
  name=f"{catalog}.{silver_schema}.{game_table_name}"
)

dp.create_auto_cdc_flow(
  target=f"{catalog}.{silver_schema}.{game_table_name}",
  source="games_clean",
  keys=["game_id"],
  sequence_by=col("loaded_date"),
  except_column_list=["ingest_timestamp", "_rescued_data"]
)

# Game_boxscores table
boxscore_rules = Rules.boxscore_rules()
@dp.view(name = "game_boxscores_clean")
@dp.expect_all_or_drop(boxscore_rules)
def game_boscore_clean():
    return (
        spark.readStream.table(f"{catalog}.{bronze_schema}.game_boxscores")
    )

dp.create_streaming_table(
  name=f"{catalog}.{silver_schema}.game_boxscores"
)

dp.create_auto_cdc_flow(
  target=f"{catalog}.{silver_schema}.game_boxscores",
  source="game_boxscores_clean",
  keys=["game_id"],
  sequence_by=col("loaded_date"),
  except_column_list=["ingest_timestamp", "_rescued_data"]
)

@dp.table(name = f"{catalog}.{silver_schema}.err_game_boxscores")
def game_boxscores_quarantine():
    df = spark.readStream.table(f"{catalog}.{bronze_schema}.game_boxscores")
    failed_df = Rules().filter_failed(df, boxscore_rules)
    return failed_df

# Game_officials table
game_official_rules = Rules.game_official_rules()
@dp.view(name = "game_officials_clean")
@dp.expect_all_or_drop(game_official_rules)
def game_officials_clean():
    return (
        spark.readStream.table(f"{catalog}.{bronze_schema}.game_officials")
    )
dp.create_streaming_table(
  name=f"{catalog}.{silver_schema}.game_officials"
)

dp.create_auto_cdc_flow(
  target=f"{catalog}.{silver_schema}.game_officials",
  source="game_officials_clean",
  keys=["game_id", "official_number"],
  sequence_by=col("loaded_date"),
  except_column_list=["ingest_timestamp", "_rescued_data"]
)

@dp.table(name = f"{catalog}.{silver_schema}.err_game_officials")
def game_officials_quarantine():
    df = spark.readStream.table(f"{catalog}.{bronze_schema}.game_officials")
    failed_df = Rules().filter_failed(df, game_official_rules)
    return failed_df

# Game_players table
player_rules = Rules.player_rules()
@dp.view(name = "game_players_clean")
@dp.expect_all_or_drop(player_rules)
def game_players_clean():
    return (
        spark.readStream.table(f"{catalog}.{bronze_schema}.game_players")
    )
dp.create_streaming_table(
  name=f"{catalog}.{silver_schema}.game_players"
)

dp.create_auto_cdc_flow(
  target=f"{catalog}.{silver_schema}.game_players",
  source="game_players_clean",
  keys=["game_id", "team_id", "player_id"],
  sequence_by=col("loaded_date"),
  except_column_list=["ingest_timestamp", "_rescued_data"]
)

@dp.table(name = f"{catalog}.{silver_schema}.err_game_players")
def game_players_quarantine():
    df = spark.readStream.table(f"{catalog}.{bronze_schema}.game_players")
    failed_df = Rules().filter_failed(df, player_rules)
    return failed_df

# Player_Stat table
player_stat_rules = Rules.player_stat_rules()
@dp.table(name = "game_player_stats_clean")
@dp.expect_all_or_drop(player_stat_rules)
def game_player_stats_clean():
    return (
        spark.readStream.table(f"{catalog}.{bronze_schema}.game_player_stats")
    )

dp.create_streaming_table(
  name=f"{catalog}.{silver_schema}.game_player_stats"
)

dp.create_auto_cdc_flow(
  target=f"{catalog}.{silver_schema}.game_player_stats",
  source="game_player_stats_clean",
  keys=["game_id", "team_id", "player_id"],
  sequence_by=col("loaded_date"),
  except_column_list=["ingest_timestamp", "_rescued_data"]
)

# Team_stat Table
team_stat_rules = Rules.team_stat_rules()
@dp.view(name = "game_team_stats_clean")
@dp.expect_all_or_drop(team_stat_rules)
def game_team_stats_clean():
    return (
        spark.readStream.table(f"{catalog}.{bronze_schema}.game_team_stats")
    )

dp.create_streaming_table(
  name=f"{catalog}.{silver_schema}.game_team_stats"
)

dp.create_auto_cdc_flow(
  target=f"{catalog}.{silver_schema}.game_team_stats",
  source="game_team_stats_clean",
  keys=["game_id", "team_id", "home", "against_team_id"],
  sequence_by=col("loaded_date"),
  except_column_list=["ingest_timestamp", "_rescued_data"]
)

#Player dictionary

@dp.table(name = f"{catalog}.{silver_schema}.player_dictionary")
def player_dictionary_clean():
    return (
        spark.read.table(f"{catalog}.{bronze_schema}.game_players")
        .groupBy("player_id")
        .agg(
            last("first_name").alias("first_name"),
            last("family_name").alias("family_name"),
            last("name_i").alias("name_i"),
            last("status").alias("status"),
            concat_ws(",", filter(collect_set("position"), lambda x: x!='None')).alias("positions"),
            concat_ws(",", collect_set("jersey_num")).alias("jersey_numbers"),
        )
    )
