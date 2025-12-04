from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import StructType, StructField, StringType

catalog="nba"
volume_name="games"
bronze_schema = "bronze"
source_schema = "source"
table_name="games"

game_schema = StructType([
    StructField("game_id", StringType(), True),
    StructField("away_team", StringType(), True),
    StructField("home_team", StringType(), True),
    StructField("date", StringType(), True)
])

@dlt.table(name=f"{catalog}.{bronze_schema}.{table_name}")
def load_games():
   df = (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("header", "true")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(game_schema)
        .load(f"/Volumes/{catalog}/{source_schema}/{volume_name}")
    )
   return df