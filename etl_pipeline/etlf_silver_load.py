from pyspark import pipelines as dp
from pyspark.sql.functions import col

catalog = "nba"
bronze_schema = "bronze"
silver_schema = "silver"
game_table_name = "games"

rules = {
    "valid_game_id": "game_id IS NOT NULL"
}

@dp.view
@dp.expect_all(rules)
def validated_games():
    return spark.readStream.table(
        f"{catalog}.{bronze_schema}.{game_table_name}"
    )

dp.create_streaming_table(
    name=f"{catalog}.{silver_schema}.{game_table_name}",
)

dp.create_auto_cdc_flow(
    name="silver_games_cdc",
    target=f"{catalog}.{silver_schema}.{game_table_name}",
    source="validated_games",
    keys=["game_id"],
    sequence_by=col("loaded_date")
)