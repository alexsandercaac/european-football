import bs4 as bs
import requests
import pandas as pd
import time
from datetime import datetime
import json
import random
import psycopg2
import psycopg2.extras
import numpy as np
from concurrent.futures import ThreadPoolExecutor


CONCURRENT_THREADS = 4
DATE_FORMAT = "%d-%m-%Y %H_%M_%S"
CONN_STRING = "host={} port={} dbname={} user={} password={}".format(
    'localhost', '5432', 'postgres', 'postgres', 'album2022')


def insert_into_versions_table(versions: list):
    """Insert a batch of rows fetched from players versions information.
    Using batches to avoid database overload of open connections.

    Args:
        players_info (list): Set of informations to be stored in database.
    """

    conn = psycopg2.connect(CONN_STRING)
    cursor = conn.cursor()

    try:
        batch_query = """
        INSERT INTO sofifa.versions
        ("date", potential, rating, value, wage, version_id,
        fifa_edition, player_id)
        VALUES %s
        on conflict(player_id, version_id) do nothing"""

        psycopg2.extras.execute_values(
            cursor, batch_query, versions, template=None, page_size=100
        )

        conn.commit()
    except Exception as error:
        print(error)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def get_versions_by_changelog(player_id):
    session = requests.Session()

    with open("scrapper/headers.json", "r") as file:
        headers = json.load(file)

    print(f"Getting versions for player {player_id}")
    time.sleep(0.1)
    url_changelog = f"https://sofifa.com/player/{player_id}/changeLog"
    response = session.get(url_changelog, headers=headers)
    if response.status_code == 429:
        time.sleep(3.5)
        print("Too many requests")
    html_changelog = bs.BeautifulSoup(response.text,'html.parser')

    # Getting all versions that there was a change in player
    links = html_changelog.find_all("a", {"rel": "nofollow"})
    change_log_urls = []
    for link in links:
        href = link["href"]
        if "changeLog" not in href and "player" in href\
            and "na=" not in href:
                change_log_urls.append(link["href"])

    change_log_versions = [version.split("/")[-2]
                           for version in change_log_urls]

    # Filtering versions in changelog from all versions
    time.sleep(0.1)
    url_versions = f"https://sofifa.com/api/player/history?id={player_id}"
    versions_obtained = session.get(
        url_versions, headers=headers).json()["data"]

    # If there is no changelog, we only have one entry for the player. So we
    # store the versions_obtained from api.
    if len(change_log_versions) > 0:
        versions_changelog = [version for version in versions_obtained
                              if version[5] in change_log_versions]
    else:
        # Obtaining only existing versions
        versions_changelog = [version for version in versions_obtained if
                              version[5] != ""]
    # print(versions_changelog)

    versions = []
    # If versions_changelog is empty, we have to manually insert the data.
    # ! When inserting data for players, we have to check if the fields aren't
    # ! None.
    if len(versions_changelog) > 0:
        for version in versions_changelog:
            # ! If the version is empty, we don't have data for that player
            # ! at that version.
            if version[5] != "":
                versions.append({
                    "date": version[0],
                    "potential": version[1],
                    "rating": version[2],
                    "value": version[3],
                    "wage": version[4],
                    "version_id": version[5],
                    "fifa_edition": version[6],
                    "player_id": player_id
                })
    else:
        for change_log_version in change_log_versions:
            versions.append({
                "date": None,
                "potential": None,
                "rating": None,
                "value": None,
                "wage": None,
                "version_id": change_log_version,
                "fifa_edition": None,
                "player_id": player_id
            })
    # The player for some reason doesn't exists in sofifa. We'll insert null
    # data for him in database.
    if response.status_code == 404:
        print("The player does not exist in sofifa!")
        versions = [{
            "date": None,
            "potential": None,
            "rating": None,
            "value": None,
            "wage": None,
            "version_id": "None",
            "fifa_edition": None,
            "player_id": player_id
        }]

    insert_into_versions_table(pd.DataFrame(versions).to_numpy())


def get_ids():
    """Get all the ids that are stored in database. We use this to choose which
    rows we'll fetch from players_versions dataframe.

    Returns:
        ids (list): List of ids.
    """
    conn = psycopg2.connect(CONN_STRING)
    cursor = conn.cursor()
    try:
        query = f"""SELECT DISTINCT(player_id) from sofifa.versions"""
        cursor.execute(query)
        conn.commit()
        ids = cursor.fetchall()
    except Exception as error:
        print(error)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    return [id_[0] for id_ in ids]


def load_players(ids: list) -> pd.DataFrame:
    """Load players from players_versions DataFrame whose ids were not fetched
    yet. Since we're paralellizing the process, we can't assure the order of
    rows that were saved. We use the index from the players_versions df as
    a identifier.

    Args:
        ids (list): Ids that were already fetched.

    Returns:
        pd.DataFrame: DataFrame with rows to fetch from players versions.
    """
    players = pd.read_csv("data/interim/player.csv",
                          index_col=0)

    if ids is not None:
        players = players.loc[~players["player_fifa_api_id"].isin(ids)]

    return list(players["player_fifa_api_id"])


if __name__ == "__main__":

    versions_len = 1
    while versions_len != 0:
        try:
            player_ids = get_ids()
            player_ids = [int(player_id) for player_id in player_ids]
            print(f"Loading players dataframe")
            players_fifa_api_id = load_players(player_ids)

            versions_len = len(players_fifa_api_id)
            print(f"Players remaining: {versions_len}")
            # # Creating many subsets to ease the processing and parellizing.
            # splits = np.array_split(players_fifa_api_id, 4_000)
            print("Starting ThreadPoolExecutor")
            with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS)\
            as executor:
                executor.map(get_versions_by_changelog, players_fifa_api_id)

        except Exception as error_players:
            print("Error while obtaining players information")
            print(error_players)
