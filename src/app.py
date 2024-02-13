"""
An example stored procedure. __main__ provides an entrypoint for local development
and testing.
"""

from snowflake.snowpark.session import Session
from snowflake.snowpark.dataframe import col, DataFrame
from snowflake.snowpark.functions import udf
import functions

def refresh_arsenal(snowpark_session: Session):
    """
    Compiles fixture data for Arsenal into a table containing goals scored, goals conceded and matches played
    """
    df = snowpark_session.table("fixtures")
    arsenal_home_fixtures = df.select(
        "FIXTURE_ID",
        "HOME_TEAM_ID",
        "AWAY_TEAM_ID",
        col("HOME_TEAM_GOALS").alias("GOALS_FOR"),
        col("AWAY_TEAM_GOALS").alias("GOALS_AGAINST")
    ).filter((col("HOME_TEAM_ID")== 1))

    arsenal_home_totals = arsenal_home_fixtures.agg(
        {"FIXTURE_ID": "count", 
         "GOALS_FOR": "sum", 
         "GOALS_AGAINST": "sum"}
    )

    arsenal_away_fixtures = df.select(
        "FIXTURE_ID",
        "HOME_TEAM_ID",
        "AWAY_TEAM_ID",
        col("HOME_TEAM_GOALS").alias("GOALS_AGAINST"),
        col("AWAY_TEAM_GOALS").alias("GOALS_FOR")
    ).filter((col("AWAY_TEAM_ID")== 1))

    arsenal_away_totals = arsenal_away_fixtures.agg(
        {"FIXTURE_ID": "count", 
         "GOALS_FOR": "sum", 
         "GOALS_AGAINST": "sum"}
    )

    arsenal_totals = arsenal_home_totals.union(arsenal_away_totals).agg(
        {"COUNT(FIXTURE_ID)": "sum", 
         "SUM(GOALS_FOR)": "sum", 
         "SUM(GOALS_AGAINST)": "sum"}
    )

    arsenal_totals = arsenal_totals.select(
        col("SUM(COUNT(FIXTURE_ID))").alias("GAMES_PLAYED"),
        col("SUM(SUM(GOALS_FOR))").alias("GOALS_SCORED"),
         col("SUM(SUM(GOALS_AGAINST))").alias("GOALS_CONCEDED")
    )


    return arsenal_totals





if __name__ == "__main__":
    # This entrypoint is used for local development (`$ python src/procs/app.py`)

    from util.local import get_env_var_config

    print("Creating session...")
    session = Session.builder.configs(get_env_var_config()).create()
    session.add_import(functions.__file__, 'functions')

    print("Running stored procedure...")
    result = refresh_arsenal(session)

    print("Stored procedure complete:")
    result.show()
