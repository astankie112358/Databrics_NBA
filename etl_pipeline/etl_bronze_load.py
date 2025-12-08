from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType
from utilities.schemas import Schema

catalog = "nba"
bronze_schema = "bronze"
source_schema = "source"
game_volume_name = "games"
boxscore_volume_name = "game_boxscores"
officials_volume_name = "game_officials"
teamstats_volume_name = "game_team_stats"
players_volume_name = "game_players"
player_stats_volume_name = "game_player_stats"

@dp.table(name=f"{catalog}.{bronze_schema}.games")
def load_games():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "date")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.games())
        .load(f"/Volumes/{catalog}/{source_schema}/{game_volume_name}")
   )

@dp.table(name=f"{catalog}.{bronze_schema}.game_boxscores")
def load_game_boxscores():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "date_day")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.boxscores())
        .load(f"/Volumes/{catalog}/{source_schema}/{boxscore_volume_name}")
   )

@dp.table(name=f"{catalog}.{bronze_schema}.game_officials")
def load_game_officials():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "game_id")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.officials())
        .load(f"/Volumes/{catalog}/{source_schema}/{officials_volume_name}")
   )

@dp.table(name=f"{catalog}.{bronze_schema}.game_team_stats")
def load_game_team_stats():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "game_id")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.team_stats())
        .load(f"/Volumes/{catalog}/{source_schema}/{teamstats_volume_name}")
   )
    
@dp.table(name=f"{catalog}.{bronze_schema}.players")
def load_players():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", ("game_id,team_id"))
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.players())
        .load(f"/Volumes/{catalog}/{source_schema}/{players_volume_name}")
    )

@dp.table(name=f"{catalog}.{bronze_schema}.game_player_stats")
def load_game_player_stats():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", ("game_id,team_id,player_id"))
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(Schema.player_stats())
        .load(f"/Volumes/{catalog}/{source_schema}/{player_stats_volume_name}")
    )