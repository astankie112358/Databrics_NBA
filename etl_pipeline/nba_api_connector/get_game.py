import pyspark.pandas as pd
# from nba_api.live.nba.endpoints import boxscore,scoreboard
# #from nba_api.stats.endpoints.hustlestatsboxscore import HustleStatsBoxScore

# class Game:
#   def __init__(self,game_id):
#     self.game_id = game_id
#     self.game_data = boxscore.BoxScore(game_id).get_dict()['game']

#   def game_info(self):
#     game_info={
#         'home_team_id': self.game_data['homeTeam']['teamId'],
#         'away_team_id': self.game_data['awayTeam']['teamId'],
#         'game_id': self.game_id,
#         'date': self.game_data['gameTimeUTC'],
#         'home_team_result': self.game_data['homeTeam']['score'],
#         'away_team_result': self.game_data['awayTeam']['score'],
#         'regulation_time': self.game_data['regulationPeriods']}
#     return game_info

# game=Game(game_id='0022500318')
# print(game.game_info())
