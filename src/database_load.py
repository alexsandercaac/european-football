"""
    DVC stage that loads all the tables from the database and saves them as
    csv files.
"""

from src.utils.data.load import load_table
from src.utils.dvc.params import get_params
import sqlite3


def main():
    """
    Load all the tables from the database and save them as csv files.
    """
    params = get_params()

    sql_database = params["sql_database"]
    connection = sqlite3.connect(sql_database)

    df_players = load_table("Player", connection)
    df_players.to_csv("data/raw/players.csv")

    df_country = load_table("Country", connection)
    df_country.to_csv("data/raw/country.csv")

    df_league = load_table("League", connection)
    df_league.to_csv("data/raw/league.csv")

    df_match = load_table("Match", connection)
    df_match.to_csv("data/raw/df_match.csv")

    df_player_attributes = load_table("Player_Attributes", connection)
    df_player_attributes.to_csv("data/raw/players_attributes.csv")

    df_team = load_table("Team", connection)
    df_team.to_csv("data/raw/teams.csv")

    df_team_attributes = load_table("Team_Attributes", connection)
    df_team_attributes.to_csv("data/raw/teams_attributes.csv")


if __name__ == "__main__":
    main()
