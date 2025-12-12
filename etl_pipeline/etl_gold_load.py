from pyspark import pipelines as dp

catalog = "nba"
silver_schema = "silver"
gold_schema = "gold"
game_table_name = "games"
boxscore_table_name = "game_boxscores"
officials_table_name = "game_officials"
teamstats_table_name = "game_team_stats"
players_table_name = "game_players"
player_stats_table_name = "game_player_stats"

@dp.table(name=f"{catalog}.{gold_schema}.{boxscore_table_name}")
def games():
  return spark.read.table(f"{catalog}.{silver_schema}.{boxscore_table_name}")\
          .melt(
            id_vars=[
  


