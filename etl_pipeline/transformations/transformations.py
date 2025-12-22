from pyspark.sql import functions as F

def fix_minutes_stat(df, type, value):
    return df.withColumn(type,
        F.when(
        (F.col(type).contains("minutes") | F.col(type).contains("timeLeading")),
        F.coalesce(F.regexp_extract(F.col(value), r"PT(\d+)M", 1).cast("float"), F.lit(0)) +
        F.coalesce(F.regexp_extract(F.col(value), r"(\d+\.?\d*)S", 1).cast("float"), F.lit(0)) / 60
        ).otherwise(F.col(value).cast("float")))
