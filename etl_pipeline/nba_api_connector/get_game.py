from nba_api.live.nba.endpoints import boxscore,scoreboard

#from nba_api.stats.endpoints.hustlestatsboxscore import HustleStatsBoxScore
class Game:
  def __init__(self,game_id):
    self.game_id = game_id
    self.game_data = boxscore.BoxScore(game_id).get_dict()['game']
    self.home_team_id=self.game_data['homeTeam']['teamId']
    self.away_team_id=self.game_data['awayTeam']['teamId']

  def game_info(self):
    game_info={
        'home_team_id': self.game_data['homeTeam']['teamId'],
        'away_team_id': self.game_data['awayTeam']['teamId'],
        'game_id': self.game_id,
        'date': self.game_data['gameTimeUTC'],
        'home_team_result': self.game_data['homeTeam']['score'],
        'away_team_result': self.game_data['awayTeam']['score'],
        'regulation_time': self.game_data['regulationPeriods']}
    return game_info
  def get_officials(self):
    officials = [
        {
            'official_number': f'official_{i+1}',
            'person_id': official['personId'],
            'game_id': self.game_id,
            'name': official['name'],
            'first_name': official['firstName'],
            'family_name': official['familyName']
        }
        for i, official in enumerate(self.game_data['officials'])
    ]
    return officials
  
  def get_team_stats(self):
    return self.game_data['homeTeam']['statistics'], self.game_data['awayTeam']['statistics']
  def get_players(self):
    return self.game_data['homeTeam']['players'], self.game_data['awayTeam']['players']