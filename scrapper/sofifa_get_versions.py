import bs4 as bs
import requests
import pandas as pd
import time
from datetime import datetime
import json
import random


DATE_FORMAT = "%d-%m-%Y %H_%M_%S"

def main():
    players = pd.read_csv("data/interim/player.csv", index_col=0)
    players_fifa_api_id = list(players["player_fifa_api_id"])
    players_fifa_api_id.sort()

    with open("scrapper/data/last_id.txt", "r") as file:
        last_id = int(file.read())

    players_fifa_api_id = [e for e in players_fifa_api_id if e>=last_id]

    session = requests.Session()

    with open("scrapper/headers.json", "r") as file:
        headers = json.load(file)

    versions = []

    # Player's Versions
    try:
        for player_id in players_fifa_api_id:
            print(f"Getting versions for player {player_id}")
            # time.sleep(random.randint(4, 7))
            url_versions = f"https://sofifa.com/api/player/history?id={player_id}"
            versions_obtained = session.get(url_versions, headers=headers).json()["data"]

            # time.sleep(random.randint(1, 3))

            for version in versions_obtained:
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
        versions_df = pd.DataFrame(versions)
        now = datetime.now()
        dt_string = now.strftime(DATE_FORMAT)
        versions_df.to_csv(f"scrapper/data/players_versions/versions_df_{dt_string}.csv")

    except Exception as e:
        print("Error:", e)
        versions_df = pd.DataFrame(versions)
        now = datetime.now()
        dt_string = now.strftime(DATE_FORMAT)
        versions_df.to_csv(f"scrapper/data/players_versions/versions_df_{dt_string}.csv")

        with open("scrapper/data/last_id.txt", "w") as file:
            file.write(str(player_id))
        versions = []


def retry(fun, max_tries=50):
    for i in range(max_tries):
        time.sleep(5)
        print(f"Try number {i}")
        try:
            fun()
        except Exception:
            continue

if __name__ == "__main__":
    retry(main)
