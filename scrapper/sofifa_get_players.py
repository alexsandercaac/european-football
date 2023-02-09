import bs4 as bs
import requests
import pandas as pd
import time
from datetime import datetime
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import psycopg2.extras
from typing import Tuple


DATE_FORMAT = "%d-%m-%Y %H_%M_%S"
LAST_ID_FILE = "data/last_id_player.txt"
CONCURRENT_THREADS = 6
CONN_STRING = "host={} port={} dbname={} user={} password={}".format(
    'localhost', '5432', 'postgres', 'postgres', 'album2022')


def get_player_position(html: bs.BeautifulSoup) -> str:
    """Getting the player's best position: <li> with "ellipsis" class
    and with "Best Position" text inside.

    Args:
        html (bs.BeautifulSoup): Page content

    Returns:
        position (str): Player's position.
    """

    lis = html.find_all("li", "ellipsis")
    for li in lis:
        if "Best Position" in str(li):
            span = li.find("span")
            position = span.text.strip()

    return position


def get_player_team(html: bs.BeautifulSoup):
    """Get the player's team. Teams are in the divs with "block-quarter" class.
    First we try to find the team by the term "Contract". If found, the player
    is at a real club. Otherwise, he's only in his national team and then we'll
    put it as his team for the sake of having a image.

    Args:
        html (bs.BeautifulSoup): Page content

    Returns:
        team (str): Player's team
        team_img_data_src (str): Team's logo
        team_id (str): Team's id to later link.
    """
    divs = html.find_all("div", "block-quarter")
    team = ""
    for div in divs:
        if "Contract" in str(div):
            team_img = div.find("img", {"data-type": "team"})
            team_img_src = team_img["data-src"]
            team_img_data_src = team_img_src.replace("60", "180")

            team_id = team_img_data_src.split("teams/")[1].split("/")[0]
            team_link = div.find("a")
            team = team_link.text.strip()

    # If a team wasnt found, we'll use the players national team.
    if team == "":
        for div in divs:
            if "Position" in str(div) and "Kit Number" in str(div):
                team_img = div.find("img", {"data-type": "team"})
                team_img_src = team_img["data-src"]
                team_img_data_src = team_img_src.replace("60", "180")

                team_id = team_img_data_src.split("teams/")[1].split("/")[0]
                team_link = div.find("a")
                team = team_link.text.strip()

    return team, team_img_data_src, team_id


def get_player_picture(html: bs.BeautifulSoup) -> str:
    """Get player's picture of the version selected

    Args:
        html (bs.BeautifulSoup): Page content.

    Returns:
        player_img_src: Player's image source.
    """
    # Player picture is in the div with "bp3-card player" class.
    player_div = html.find("div", "bp3-card player")
    player_img = player_div.find("img")
    player_img_data_srcset = player_img["data-srcset"]

    player_img_srcs = player_img_data_srcset.split(",")
    player_img_src = player_img_srcs[1].replace("3x", "").strip()

    return player_img_src


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
    players = pd.read_csv("data/concatenated/players_versions.csv",
                          index_col=0)
    players = players.reset_index()
    players.drop(columns=["index"], inplace=True)
    players.drop_duplicates(inplace=True)

    if ids is not None:
        players = players.loc[~players.index.isin(ids)]

    return players


def get_player(players: np.array):
    """Fetch player version data from webpage and store it into the database.
    Each players np.array is a subset from the players_versions dataframe.
    If we can fetch all the information needed, we try to save it into the
    database. If there's any error (wrong fetch, disconnect, etc) we rollback
    and don't commit the operation. In case we receive a 429 response_code, we
    wait 5s to try again.

    Args:
        players (np.array): Set of players versions to fetch and store.
    """
    time.sleep(0.1)

    headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
               AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 \
               Safari/537.36"}

    try:
        players_infos = []
        for player in players:
            time.sleep(0.4)
            # For each version, we extract all information available.
            date = player[0]
            potential = player[1]
            rating = player[2]
            value = player[3]
            wage = player[4]
            version_id = player[5]
            fifa_edition = player[6]
            player_id = player[7]
            # print(f"""Player id {player_id} version {version_id}""")

            session = requests.Session()
            url = f'https://sofifa.com/player/{player_id}/{version_id}'
            response = session.get(url, headers=headers)
            # If the response code is 429, it means that we're sending too
            # many requests and we have to wait.
            if response.status_code == 429:
                time.sleep(3.5)
                print("Too many requests")
            html = bs.BeautifulSoup(response.text,'html.parser')

            # Element in page with basic player info
            player_schema_info = html.find(
                "script", {"type":"application/ld+json"})

            player_schema_info = eval(player_schema_info.text.\
                                        replace("\r", "").replace("\n", "").\
                                        replace("\t", "").strip())

            position = player_schema_info["jobTitle"]
            country = player_schema_info["nationality"]
            image = player_schema_info["image"]
            team, team_img, team_id = get_player_team(html)

            player_info = {
                "player_id": player_id, "version_id": version_id,
                "team_id": team_id, "team": team, "date": date,
                "country": country, "potential": potential,
                "rating": rating, "value": value, "wage": wage,
                "fifa_edition": fifa_edition, "position": position,
                "player_img": image, "team_img": team_img,
                "index_id": player[-1]
            }
            players_infos.append(player_info)

        insert_into_players_table(pd.DataFrame(players_infos).to_numpy())

    except Exception as e:
        print("Error:", e)


def insert_into_players_table(players_info: list):
    """Insert a batch of rows fetched from players versions information.
    Using batches to avoid database overload of open connections.

    Args:
        players_info (list): Set of informations to be stored in database.
    """

    conn = psycopg2.connect(CONN_STRING)
    cursor = conn.cursor()

    try:
        batch_query = """
        INSERT INTO sofifa.player
        (player_id, version_id, team_id, team, "date", country, potential,
        rating, value, wage, fifa_edition, "position", player_img, team_img,
        index_id)
        VALUES %s"""

        psycopg2.extras.execute_values(
            cursor, batch_query, players_info, template=None, page_size=100
        )

        conn.commit()
    except Exception as error:
        print(error)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def get_ids():
    """Get all the ids that are stored in database. We use this to choose which
    rows we'll fetch from players_versions dataframe.

    Returns:
        ids (list): List of ids.
    """
    conn = psycopg2.connect(CONN_STRING)
    cursor = conn.cursor()
    try:
        query = f"""SELECT (index_id) from sofifa.player"""
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


if __name__ == "__main__":
    try:
        ids = get_ids()
        print(f"Loading players dataframe")
        players = load_players(ids)
        players_np = np.array(players)
        players_np_index = np.array(players.index).reshape(-1, 1)
        players_np = np.concatenate((players_np, players_np_index), axis=1)

        # Creating many subsets to ease the processing and parellizing.
        splits = np.array_split(players_np, 750_000)
        print("Starting ThreadPoolExecutor")
        with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS)\
        as executor:
            executor.map(get_player, splits)
    except Exception as error_players:
       print("Error while obtaining players information")
       print(error_players)
