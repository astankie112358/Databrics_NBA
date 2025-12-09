from pyspark import pipelines as dp
from utilities.schemas import Schema
from pyspark.sql.functions import current_timestamp

catalog = "nba"
bronze_schema = "bronze"
source_schema = "source"
game_table_name = "games"
boxscore_table_name = "game_boxscores"
officials_table_name = "game_officials"
teamstats_table_name = "game_team_stats"
players_table_name = "game_players"
player_stats_table_name = "game_player_stats"

game_volume_name = "games"
boxscore_volume_name = "game_boxscores"
officials_volume_name = "game_officials"
teamstats_volume_name = "game_team_stats"
players_volume_name = "game_players"
player_stats_volume_name = "game_player_stats"

@dp.table(name=f"{catalog}.{bronze_schema}.{game_table_name}")
def load_games():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "date")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.games())
        .load(f"/Volumes/{catalog}/{source_schema}/{game_volume_name}")
        .withColumn("loaded_date", current_timestamp())
   )

@dp.table(name=f"{catalog}.{bronze_schema}.{boxscore_table_name}")
def load_game_boxscores():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "date_day")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.boxscores())
        .load(f"/Volumes/{catalog}/{source_schema}/{boxscore_volume_name}")
        .withColumn("loaded_date", current_timestamp())
   )

@dp.table(name=f"{catalog}.{bronze_schema}.{officials_table_name}")
def load_game_officials():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "game_id")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.officials())
        .load(f"/Volumes/{catalog}/{source_schema}/{officials_volume_name}")
        .withColumn("loaded_date", current_timestamp())
   )

@dp.table(name=f"{catalog}.{bronze_schema}.{teamstats_table_name}")
def load_game_team_stats():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "game_id")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.team_stats())
        .load(f"/Volumes/{catalog}/{source_schema}/{teamstats_volume_name}")
        .withColumn("loaded_date", current_timestamp())
   )
    
@dp.table(name=f"{catalog}.{bronze_schema}.{players_table_name}")
def load_players():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", ("game_id,team_id"))
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.players())
        .load(f"/Volumes/{catalog}/{source_schema}/{players_volume_name}")
        .withColumn("loaded_date", current_timestamp())
    )

@dp.table(name=f"{catalog}.{bronze_schema}.{player_stats_table_name}")
def load_game_player_stats():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", ("game_id,team_id,player_id"))
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.player_stats())
        .load(f"/Volumes/{catalog}/{source_schema}/{player_stats_volume_name}")
        .withColumn("loaded_date", current_timestamp())
    )