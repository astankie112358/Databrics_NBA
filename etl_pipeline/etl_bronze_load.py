from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, LongType

catalog = "nba"
game_volume_name = "games"
boxscore_volume_name = "game_boxscores"
officials_volume_name = "game_officials"
teamstats_volume_name = "game_team_stats"
bronze_schema = "bronze"
source_schema = "source"
table_name = "games"

game_schema = StructType([
    StructField("game_id", StringType(), True),
    StructField("away_team", StringType(), True),
    StructField("home_team", StringType(), True),
    StructField("date", StringType(), True)
])

@dp.table(name=f"{catalog}.{bronze_schema}.{table_name}")
def load_games():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "date")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(game_schema)
        .load(f"/Volumes/{catalog}/{source_schema}/{game_volume_name}")
   )
    
volume_name = "game_boxscores"
table_name = "game_boxscores"

game_boxscore_schema = StructType([
    StructField("away_team_id", StringType(), True),
    StructField("away_team_result", StringType(), True),
    StructField("date", StringType(), True),
    StructField("game_id", StringType(), True),
    StructField("home_team_id", StringType(), True),
    StructField("home_team_result", StringType(), True),
    StructField("regulation_time", StringType(), True),
    StructField("date_day", StringType(), True)
])

@dp.table(name=f"{catalog}.{bronze_schema}.{table_name}")
def load_game_boxscores():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "date_day")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(game_boxscore_schema)
        .load(f"/Volumes/{catalog}/{source_schema}/{boxscore_volume_name}")
   )
    
game_officials_schema = StructType([
    StructField("familyName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("game_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("official_num", StringType(), True),
    StructField("personId", StringType(), True)
])
 
volume_name = "game_officials"
table_name = "game_officials"
@dp.table(name=f"{catalog}.{bronze_schema}.{table_name}")
def load_game_officials():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "game_id")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(game_officials_schema)
        .load(f"/Volumes/{catalog}/{source_schema}/{officials_volume_name}")
   )

game_team_stats_schema = StructType([
    StructField("team_id", StringType(), True),
    StructField("against_team_id", StringType(), True),
    StructField("stat_type", StringType(), True),
    StructField("stat_value", StringType(), True),
    StructField("home", StringType(), True),
    StructField("game_id", StringType(), True)
])

volume_name = "game_team_stats"
table_name = "game_team_stats"
@dp.table(name=f"{catalog}.{bronze_schema}.{table_name}")
def load_game_team_stats():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "game_id")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(game_team_stats_schema)
        .load(f"/Volumes/{catalog}/{source_schema}/{teamstats_volume_name}")
   )