from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, expr
from utilities.rules import Rules

catalog = "nba"
bronze_schema = "bronze"
silver_schema = "silver"
game_table_name = "games"

rules = Rules().game_rules()

@dp.view(name = "games_clean")
@dp.expect_all_or_drop(rules)
def games_clean():
    return (
        spark.readStream.table(f"{catalog}.{bronze_schema}.{game_table_name}")
    )
    
@dp.table(name = f"{catalog}.{silver_schema}.err_{game_table_name}")
def games_quarantine():
    df = spark.readStream.table(f"{catalog}.{bronze_schema}.{game_table_name}")
    failed_df = Rules().filter_failed(df, rules)
    return failed_df

dp.create_streaming_table(
  name=f"{catalog}.{silver_schema}.{game_table_name}"
)

dp.create_auto_cdc_flow(
  target=f"{catalog}.{silver_schema}.{game_table_name}",
  source="games_clean",
  keys=["game_id"],
  sequence_by=col("ingest_timestamp"),
)
