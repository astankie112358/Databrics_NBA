from pyspark.sql.types import StructType, StructField, StringType, TimestampType

class Schema:
    @staticmethod
    def games():
        return StructType([
            StructField('game_id', StringType(), True),
            StructField('away_team', StringType(), True),
            StructField('home_team', StringType(), True),
            StructField('date', StringType(), True),
            StructField('ingest_timestamp', TimestampType(), True)
        ])
    @staticmethod
    def boxscores():
        return StructType([
            StructField("away_team_id", StringType(), True),
            StructField("away_team_result", StringType(), True),
            StructField("date", StringType(), True),
            StructField("game_id", StringType(), True),
            StructField("home_team_id", StringType(), True),
            StructField("home_team_result", StringType(), True),
            StructField("regulation_time", StringType(), True),
            StructField("date_day", StringType(), True),
            StructField("ingest_timestamp", TimestampType(), True)
        ])
    @staticmethod
    def team_stats():
        return StructType([
            StructField("team_id", StringType(), True),
            StructField("against_team_id", StringType(), True),
            StructField("stat_type", StringType(), True),
            StructField("stat_value", StringType(), True),
            StructField("home", StringType(), True),
            StructField("game_id", StringType(), True),
            StructField("ingest_timestamp", TimestampType(), True)
        ])
    @staticmethod
    def officials():
        return StructType([
            StructField("family_name", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("game_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("official_number", StringType(), True),
            StructField("person_id", StringType(), True),
            StructField("ingest_timestamp", TimestampType(), True)
        ])
    @staticmethod
    def players():
        return StructType([
            StructField("game_id", StringType(), True),
            StructField("team_id", StringType(), True),
            StructField("played_at_home", StringType(), True),
            StructField("against_team_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("order", StringType(), True),
            StructField("player_id", StringType(), True),
            StructField("jersey_num", StringType(), True),
            StructField("position", StringType(), True),
            StructField("starter", StringType(), True),
            StructField("oncourt", StringType(), True),
            StructField("played", StringType(), True),
            StructField("name", StringType(), True),
            StructField("name_i", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("family_name", StringType(), True),
            StructField("ingest_timestamp", TimestampType(), True)
        ])
    @staticmethod
    def player_stats():
        return StructType([
            StructField("game_id", StringType(), True),
            StructField("team_id", StringType(), True),
            StructField("player_id", StringType(), True),
            StructField("stat_type", StringType(), True),
            StructField("stat_value", StringType(), True),
            StructField("ingest_timestamp", TimestampType(), True)
        ])