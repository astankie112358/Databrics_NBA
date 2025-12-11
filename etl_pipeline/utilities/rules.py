from pyspark.sql import functions as F
class Rules:
    @staticmethod
    def game_rules():
        return {
            "valid_game_id": "game_id is NOT NULL",
            "valid_away_team": "away_team is NOT NULL",
            "valid_home_team": "home_team is NOT NULL",
            "valid_date": "date is NOT NULL",
        }
    @staticmethod
    def boxscore_rules():
        return {
            "valid_home_team": "home_team_id is NOT NULL",
            "valid_away_team": "away_team_id is NOT NULL",
            "valid_game_id": "game_id is NOT NULL",
            "valid_date": "date is NOT NULL",
            "valid_regulation_time": "regulation_time is NOT NULL"          
        }
    @staticmethod
    def game_official_rules():
        return {
            "valid_first_name": "first_name is NOT NULL",
            "valid_family_name": "family_name is NOT NULL",
            "valid_person_id": "person_id is NOT NULL",
            "valid_game_id": "game_id is NOT NULL",
            "valid_official_number": "official_number IN ('official_1', 'official_2', 'official_3')"
        }   
    @staticmethod
    def player_stat_rules():
        return {
            "valid_game_id": "game_id is NOT NULL",
            "valid_team_id": "team_id is NOT NULL",
            "valid_player_id": "player_id is NOT NULL",
        }
    @staticmethod
    def player_rules():
        return {
            "valid_player_id": "player_id is NOT NULL",
            "valid_first_name": "first_name is NOT NULL",
            "valid_family_name": "family_name is NOT NULL",
            "valid_team_id": "team_id is NOT NULL",
            "valid_against_team_id": "against_team_id is NOT NULL"
        }
    @staticmethod
    def team_stat_rules():
        return{
            "valid_team_id": "team_id is NOT NULL",
            "valid_game_id": "game_id is NOT NULL",
            "valid_against_team_id": "against_team_id is NOT NULL",
        }

    @staticmethod
    def filter_failed(df, rules):
        rule_checks = []
        for rule_name, rule_sql in rules.items():
            condition = F.when(F.expr(f"NOT ({rule_sql})"), F.lit(rule_name)).otherwise(F.lit(""))
            rule_checks.append(condition)
        return df.withColumn("failed_rules", F.array(*rule_checks)) \
                 .withColumn("failed_rules", F.array_remove(F.col("failed_rules"), "")) \
                 .filter(F.size(F.col("failed_rules")) > 0)