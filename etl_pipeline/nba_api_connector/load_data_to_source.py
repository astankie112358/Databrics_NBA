from find_games import get_games_by_date

catalog="nba"
source_schema = "source"
date="2025-11-28"
volume_name = "games"

API_KEY = dbutils.secrets.get(
    scope="nba_secrets",
    key="balldontlie_api_key"
)
games=get_games_by_date(date, API_KEY)
games=spark.createDataFrame(games, "game_id: string, away_team: string, home_team: string, date: string")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{source_schema}.{volume_name}")
volume_path = f"/Volumes/{catalog}/{source_schema}/{volume_name}/games.parquet"
games.write.mode("overwrite").format("parquet").partitionBy("date").save(volume_path)

