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


# def main():
#     players = pd.read_csv("data/interim/player.csv", index_col=0)
#     players_fifa_api_id = list(players["player_fifa_api_id"])
#     players_fifa_api_id.sort()

#     with open("scrapper/data/last_id.txt", "r") as file:
#         last_id = int(file.read())

#     players_fifa_api_id = [e for e in players_fifa_api_id if e>=last_id]

#     session = requests.Session()

#     with open("scrapper/headers.json", "r") as file:
#         headers = json.load(file)

#     versions = []

#     # Player's Versions
#     try:
#         for player_id in players_fifa_api_id:
#             print(f"Getting versions for player {player_id}")
#             # time.sleep(random.randint(4, 7))
#             url_versions = f"https://sofifa.com/api/player/history?id={player_id}"
#             versions_obtained = session.get(url_versions, headers=headers).json()["data"]

#             # time.sleep(random.randint(1, 3))

#             for version in versions_obtained:
#                 # ! If the version is empty, we don't have data for that player
#                 # ! at that version.
#                 if version[5] != "":
#                     versions.append({
#                         "date": version[0],
#                         "potential": version[1],
#                         "rating": version[2],
#                         "value": version[3],
#                         "wage": version[4],
#                         "version_id": version[5],
#                         "fifa_edition": version[6],
#                         "player_id": player_id
#                     })
#         versions_df = pd.DataFrame(versions)
#         now = datetime.now()
#         dt_string = now.strftime(DATE_FORMAT)
#         versions_df.to_csv(f"scrapper/data/players_versions/versions_df_{dt_string}.csv")

#     except Exception as e:
#         print("Error:", e)
#         versions_df = pd.DataFrame(versions)
#         now = datetime.now()
#         dt_string = now.strftime(DATE_FORMAT)
#         versions_df.to_csv(f"scrapper/data/players_versions/versions_df_{dt_string}.csv")

#         with open("scrapper/data/last_id.txt", "w") as file:
#             file.write(str(player_id))
#         versions = []


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
            and "na" not in href:
                change_log_urls.append(link["href"])

    change_log_versions = [version.split("/")[-2]
                           for version in change_log_urls]

    # Filtering versions in changelog from all versions
    time.sleep(0.1)
    url_versions = f"https://sofifa.com/api/player/history?id={player_id}"
    versions_obtained = session.get(
        url_versions, headers=headers).json()["data"]

    versions_changelog = [version for version in versions_obtained
                          if version[5] in change_log_versions]

    versions = []
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
    try:
        player_ids = get_ids()
        player_ids = [int(player_id) for player_id in player_ids]
        print(f"Loading players dataframe")
        players_fifa_api_id = load_players(player_ids)

        if len(players_fifa_api_id) != 0:
            # # Creating many subsets to ease the processing and parellizing.
            # splits = np.array_split(players_fifa_api_id, 4_000)
            print("Starting ThreadPoolExecutor")
            with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS)\
            as executor:
                executor.map(get_versions_by_changelog, players_fifa_api_id)
        else:
            print("There is no data to gather!")
    except Exception as error_players:
       print("Error while obtaining players information")
       print(error_players)
