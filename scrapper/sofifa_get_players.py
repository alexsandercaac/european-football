import bs4 as bs
import requests
import pandas as pd
import time
from datetime import datetime
import json
import random
from tqdm import tqdm
import numpy as np
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import logging
from threading import current_thread
from threading import get_ident
from threading import get_native_id
import psycopg2
import psycopg2.extras


DATE_FORMAT = "%d-%m-%Y %H_%M_%S"
LAST_ID_FILE = "scrapper/data/last_id_player.txt"
CONCURRENT_THREADS = 4
CONN_STRING = "host={} port={} dbname={} user={} password={}".format(
    'localhost', '5432', 'postgres', 'postgres', 'album2022')


def get_player_position(html):
    # Getting the player's best position: <li> with "ellipsis" class
    # and with "Best Position" text inside.
    lis = html.find_all("li", "ellipsis")
    for li in lis:
        if "Best Position" in str(li):
            span = li.find("span")
            position = span.text.strip()

    return position


def get_player_team(html):
    # Teams are in the divs with "block-quarter" class.
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


def get_player_picture(html):
    # Player picture is in the div with "bp3-card player" class.
    player_div = html.find("div", "bp3-card player")
    player_img = player_div.find("img")
    player_img_data_srcset = player_img["data-srcset"]

    player_img_srcs = player_img_data_srcset.split(",")
    player_img_src = player_img_srcs[1].replace("3x", "").strip()

    return player_img_src


def load_players(ids):
    players = pd.read_csv("scrapper/data/concatenated/players_versions.csv",
                          index_col=0)
    players = players.reset_index()
    players.drop(columns=["index"], inplace=True)
    players.drop_duplicates(inplace=True)

    if ids is not None:
        players = players.loc[~players.index.isin(ids)]

    return players


def main(players):

    with open("scrapper/headers.json", "r") as file:
        headers = json.load(file)
    with open(LAST_ID_FILE, "r") as file:
        last_id = eval(file.read())

    players_infos = []
    try:
        for i, version in players.iterrows():
            if i >= last_id:
                # For each version, we extract all information available.
                date = version["date"]
                potential = version["potential"]
                rating = version["rating"]
                value = version["value"]
                wage = version["wage"]
                version_id = version["version_id"]
                fifa_edition = version["fifa_edition"]
                player_id = version["player_id"]
                print(f"""Player id {player_id} version {version_id}""")

                session = requests.Session()
                url = f'https://sofifa.com/player/{player_id}/{version_id}'
                response = session.get(url, headers=headers)
                html = bs.BeautifulSoup(response.text,'html.parser')

                # Element in page with basic player info
                player_schema_info = html.find(
                    "script", {"type":"application/ld+json"})

                player_schema_info = eval(player_schema_info.text.\
                                          replace("\r", "").replace("\n", "").\
                                            replace("\t", "").strip())

                # position = get_player_position(html)
                position = player_schema_info["jobTitle"]
                country = player_schema_info["nationality"]
                image = player_schema_info["image"]
                team, team_img, team_id = get_player_team(html)
                # player_img = get_player_picture(html)

                players_infos.append({
                    "player_id": player_id, "version_id": version_id,
                    "team_id": team_id, "team": team, "date": date,
                    "country": country, "potential": potential,
                    "rating": rating, "value": value, "wage": wage,
                    "fifa_edition": fifa_edition, "position": position,
                    "player_img": image, "team_img": team_img
                })

            if i % 500 == 0 and i != 0:
                now = datetime.now()
                dt_string = now.strftime(DATE_FORMAT)
                players_infos_df = pd.DataFrame(players_infos)
                players_infos_df.to_csv(
                    f"scrapper/data/players/players_df_{dt_string}.csv")

                with open(LAST_ID_FILE, "w") as file:
                    file.write(str(i))
                players_infos = []

    except Exception as e:
        print("Error:", e)
        now = datetime.now()
        dt_string = now.strftime(DATE_FORMAT)
        players_infos_df = pd.DataFrame(players_infos)
        players_infos_df.to_csv(
            f"scrapper/data/players/players_df_{dt_string}.csv")

        with open(LAST_ID_FILE, "w") as file:
            file.write(str(i))
        players_infos = []


def retry(fun, max_tries=100):
    for i in range(max_tries):
        time.sleep(5)
        print(f"Try number {i}")
        try:
            fun()
        except Exception as e:
            print("Error", e)


def get_player(players):
    time.sleep(0.4)
    with open("scrapper/headers.json", "r") as file:
        headers = json.load(file)

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
            print(f"""Player id {player_id} version {version_id}""")

            session = requests.Session()
            url = f'https://sofifa.com/player/{player_id}/{version_id}'
            response = session.get(url, headers=headers)
            html = bs.BeautifulSoup(response.text,'html.parser')

            # Element in page with basic player info
            player_schema_info = html.find(
                "script", {"type":"application/ld+json"})

            player_schema_info = eval(player_schema_info.text.\
                                        replace("\r", "").replace("\n", "").\
                                        replace("\t", "").strip())

            # position = get_player_position(html)
            position = player_schema_info["jobTitle"]
            country = player_schema_info["nationality"]
            image = player_schema_info["image"]
            team, team_img, team_id = get_player_team(html)
            # player_img = get_player_picture(html)

            player_info = {
                "player_id": player_id, "version_id": version_id,
                "team_id": team_id, "team": team, "date": date,
                "country": country, "potential": potential,
                "rating": rating, "value": value, "wage": wage,
                "fifa_edition": fifa_edition, "position": position,
                "player_img": image, "team_img": team_img, "index_id": player[-1]
            }
            players_infos.append(player_info)

        insert_into_players_table(np.array(players_infos))

    except Exception as e:
        print("Error:", e)


def insert_into_players_table(players_info):
    """Insert individually the base_url that will be parsed"""

    conn = psycopg2.connect(CONN_STRING)
    cursor = conn.cursor()

    try:
        # query = f"""
        #     INSERT INTO sofifa.player
        #     (player_id, version_id, team_id, team, "date", country, potential,
        #     rating, value, wage, fifa_edition, "position", player_img, team_img,
        #     index_id)
        #     VALUES('{player_info["player_id"]}', '{player_info["version_id"]}',
        #            '{player_info["team_id"]}', '{player_info["team"]}',
        #            '{player_info["date"]}', '{player_info["country"]}',
        #            '{player_info["potential"]}',
        #            '{player_info["rating"]}', '{player_info["value"]}',
        #            '{player_info["wage"]}', '{player_info["fifa_edition"]}',
        #            '{player_info["position"]}', '{player_info["player_img"]}',
        #            '{player_info["team_img"]}', '{player_info["index_id"]}')
        #     on conflict (player_id, version_id) do nothing;
        # """
        # print(query)
        batch_query = """
        INSERT INTO sofifa.player
        (player_id, version_id, team_id, team, "date", country, potential,
        rating, value, wage, fifa_edition, "position", player_img, team_img,
        index_id)
        VALUES %s"""

        psycopg2.extras.execute_values(
            cursor, batch_query, players_info, template=None, page_size=100
        )

        # cursor.execute(query)

        conn.commit()
    except Exception as error:
        print(error)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def get_ids():

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
    # retry(main)

    try:
        ids = get_ids()
        print(f"Loading players dataframe")
        players = load_players(ids)
        players_np = np.array(players)
        players_np_index = np.array(players.index).reshape(-1, 1)
        players_np = np.concatenate((players_np, players_np_index), axis=1)

        splits = np.array_split(players_np, 1_000_000)
        print("Starting ThreadPoolExecutor")
        with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS)\
        as executor:
            # futures = []
            # for player in players:
            #     futures.append(executor.submit(main, players = players))
            # executor.map(get_player, players_np)
            executor.map(get_player, splits)
    except Exception as erro_parse_content:
       print("ERRO NO PARSE CONTENT")
       print(erro_parse_content)
