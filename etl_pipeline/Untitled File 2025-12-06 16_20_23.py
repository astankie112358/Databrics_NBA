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

def load_players():
    return (
        spark.read
        .format("parquet")
        .option("partitionColumns", ["game_id", "team_id"])
        .option("schemaEvolutionMode", "rescue")
        .schema(Schema.players())
        .load(f"/Volumes/{catalog}/{source_schema}/{players_volume_name}")
    )
display(load_players())