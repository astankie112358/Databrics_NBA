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
    def filter_failed(df, rules):
        rule_checks = []
        for rule_name, rule_sql in rules.items():
            condition = F.when(F.expr(f"NOT ({rule_sql})"), F.lit(rule_name)).otherwise(F.lit(""))
            rule_checks.append(condition)
        return df.withColumn("failed_rules", F.array(*rule_checks)) \
                 .withColumn("failed_rules", F.array_remove(F.col("failed_rules"), "")) \
                 .filter(F.size(F.col("failed_rules")) > 0)