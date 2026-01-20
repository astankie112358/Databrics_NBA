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
    @staticmethod
    def team_stats_gold():
        return {
            "assists": "int",
            "assistsTurnoverRatio": "float",
            "benchPoints": "int",
            "biggestLead": "int",
            "biggestLeadScore": "string",
            "biggestScoringRun": "int",
            "biggestScoringRunScore": "string",
            "blocks": "int",
            "blocksReceived": "int",
            "fastBreakPointsAttempted": "int",
            "fastBreakPointsMade": "int",
            "fastBreakPointsPercentage": "float",
            "fieldGoalsAttempted": "int",
            "fieldGoalsEffectiveAdjusted": "float",
            "fieldGoalsMade": "int",
            "fieldGoalsPercentage": "float",
            "foulsDrawn": "int",
            "foulsOffensive": "int",
            "foulsPersonal": "int",
            "foulsTeam": "int",
            "foulsTeamTechnical": "int",
            "foulsTechnical": "int",
            "freeThrowsAttempted": "int",
            "freeThrowsMade": "int",
            "freeThrowsPercentage": "float",
            "leadChanges": "int",
            "minutes": "string",
            "minutesCalculated": "string",
            "points": "float",
            "pointsAgainst": "float",
            "pointsFastBreak": "float",
            "pointsFromTurnovers": "float",
            "pointsInThePaint": "float",
            "pointsInThePaintAttempted": "float",
            "pointsInThePaintMade": "float",
            "pointsInThePaintPercentage": "float",
            "pointsSecondChance": "float",
            "reboundsDefensive": "float",
            "reboundsOffensive": "float",
            "reboundsPersonal": "float",
            "reboundsTeam": "float",
            "reboundsTeamDefensive": "float",
            "reboundsTeamOffensive": "float",
            "reboundsTotal": "float",
            "secondChancePointsAttempted": "float",
            "secondChancePointsMade": "float",
            "secondChancePointsPercentage": "float",
            "steals": "float",
            "teamFieldGoalAttempts": "float",
            "threePointersAttempted": "float",
            "threePointersMade": "float",
            "threePointersPercentage": "float",
            "timeLeading": "string",
            "timesTied": "float",
            "trueShootingAttempts": "float",
            "trueShootingPercentage": "float",
            "turnovers": "float",
            "turnoversTeam": "float",
            "turnoversTotal": "float",
            "twoPointersAttempted": "float",
            "twoPointersMade": "float",
            "twoPointersPercentage": "float"
        }
    @staticmethod
    def player_stats_gold():
        return {
            "assists": "int",
            "blocks": "int",
            "blocksReceived": "int",
            "fieldGoalsAttempted": "int",
            "fieldGoalsMade": "int",
            "fieldGoalsPercentage": "float",
            "foulsDrawn": "int",
            "foulsOffensive": "int",
            "foulsPersonal": "int",
            "foulsTechnical": "int",
            "freeThrowsAttempted": "int",
            "freeThrowsMade": "int",
            "freeThrowsPercentage": "float",
            "minus": "float",
            "minutes": "string",
            "minutesCalculated": "string",
            "plus": "float",
            "plusMinusPoints": "float",
            "points": "int",
            "pointsFastBreak": "int",
            "pointsInThePaint": "int",
            "pointsSecondChance": "int",
            "reboundsDefensive": "int",
            "reboundsOffensive": "int",
            "reboundsTotal": "int",
            "steals": "int",
            "threePointersAttempted": "int",
            "threePointersMade": "int",
            "threePointersPercentage": "float",
            "turnovers": "int",
            "twoPointersAttempted": "int",
            "twoPointersMade": "int",
            "twoPointersPercentage": "float"
        }
