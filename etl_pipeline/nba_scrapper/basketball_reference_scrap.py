from basketball_reference_scraper.box_scores import get_box_scores
from basketball_reference_scraper.seasons import get_schedule
from teams_map import TeamAbb
# class BasketballReferenceAPI:
#     def __init__(self):
#         self.team_abb = Teams.TeamAbb

#     def get_game(self, date, team1_abb, team2_abb):
#         game_id = team1_abb + team2_abb + date.strftime('%Y%m%d')
#         box_score = pd.DataFrame()
#         for type in ['BASIC', 'ADVANCED']:
#             new_box_score = get_box_scores(date.strftime('%Y-%m-%d'), team1_abb, team2_abb,
#                                            period='GAME', stat_type=type)
#             new_box_score = pd.concat([new_box_score[team1_abb], new_box_score[team2_abb]], keys=[team1_abb, team2_abb])
#             box_score = pd.concat([box_score, new_box_score], ignore_index=True)
#         box_score['GAME_ID'] = game_id
#         print(game_id)
#         return box_score, game_id

#     def get_season_box_scores(self, season):
#         schedule = pd.DataFrame.from_dict(get_schedule(season, False))
#         box_score = pd.DataFrame()
#         for index, game in schedule.iterrows():
#             print('Game: ' + str(index + 1) + ' out of ' + str(len(schedule) + 1))
#             home_team_abb = Teams.TeamAbb[game['HOME'].upper()]
#             visitor_team_abb = Teams.TeamAbb[game['VISITOR'].upper()]
#             new_box_score = self.get_game(game['DATE'], home_team_abb, visitor_team_abb)[0]
#             pd.concat([box_score, new_box_score])
#         return box_score

#     def get_season_box_scores_separtate_file(self, season, path):
#         schedule = pd.DataFrame.from_dict(get_schedule(season, False))
#         schedule['HOME'] = schedule.apply(lambda row: Teams.TeamAbb[row['HOME'].upper()], axis=1)
#         schedule['VISITOR'] = schedule.apply(lambda row: Teams.TeamAbb[row['VISITOR'].upper()], axis=1)
#         schedule['GAME_ID'] = schedule['HOME'] + schedule['VISITOR'] + schedule['DATE'].dt.strftime('%Y%m%d')
#         schedule['Season'] = str(season) + str(season + 1)[-2:]
#         files = os.listdir(path)
#         files = [file.replace('.csv', '') for file in files]
#         schedule.drop(schedule[schedule['GAME_ID'].isin(files)].index, inplace=True)
#         box_score = pd.DataFrame()
#         first_game_flag = 0
#         for index, game in schedule.iterrows():
#             if first_game_flag == 0:
#                 first_game_flag = index
#             print('Game: ' + str(index - first_game_flag + 1) + ' out of ' + str(len(schedule) + 1))
#             new_box_score = self.get_game(game['DATE'], game['HOME'], game['VISITOR'])[0]
#             box_score = pd.concat([box_score, new_box_score])
#             box_score.to_csv(path + game['GAME_ID'] + '.csv')
#         return path

