from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

catalog = "nba"
volume_name = "games"
bronze_schema = "bronze"
source_schema = "source"
table_name = "games"

checkpoint_base = "/tmp/lakeflow_checkpoints"

# game_schema = StructType([
#     StructField("game_id", IntegerType(), True),
#     StructField("away_team", StringType(), True),
#     StructField("home_team", StringType(), True),
#     StructField("date", StringType(), True)
# ])

# game_boxscore_schema = StructType([
#     StructField("away_team_id", StringType(), True),
#     StructField("away_team_result", StringType(), True),
#     StructField("date", StringType(), True),
#     StructField("game_id", StringType(), True),
#     StructField("home_team_id", StringType(), True),
#     StructField("home_team_result", StringType(), True),
#     StructField("regulation_time", StringType(), True),
#     StructField("date_day", StringType(), True)
# ])

# game_officials_schema = StructType([
#     StructField("familyName", StringType(), True),
#     StructField("firstName", StringType(), True),
#     StructField("game_id", StringType(), True),
#     StructField("name", StringType(), True),
#     StructField("official_num", StringType(), True),
#     StructField("personId", StringType(), True)
# ])

# game_team_stats_schema = StructType([
#     StructField("team_id", StringType(), True),
#     StructField("against_team_id", StringType(), True),
#     StructField("Stat_Type", StringType(), True),
#     StructField("Stat_Value", StringType(), True),
#     StructField("Home", StringType(), True),
#     StructField("game_id", StringType(), True)
# ])

# games_df = (
#     spark.read
#     .format("parquet")
#     .schema(game_schema)
#     .load(f"/Volumes/{catalog}/{source_schema}/games")
# )
# display(games_df)

# game_boxscores_df = (
#     spark.read
#     .format("parquet")
#     .schema(game_boxscore_schema)
#     .load(f"/Volumes/{catalog}/{source_schema}/game_boxscores")
# )
# display(game_boxscores_df)

game_officials_df = (
    spark.read
    .format("parquet")
#    .schema(game_officials_schema)
    .load(f"/Volumes/{catalog}/{source_schema}/game_team_stats")
)
display(game_officials_df)

# game_team_stats_df = (
#     spark.read
#     .format("parquet")
#     .schema(game_team_stats_schema)
#     .load(f"/Volumes/{catalog}/{source_schema}/game_team_stats")
# )
# display(game_team_stats_df)