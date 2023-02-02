import bs4 as bs
import requests
import pandas as pd
import time
from datetime import datetime
import json
import random

DATE_FORMAT = "%d-%m-%Y %H_%M_%S"

session = requests.Session()
headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"}

players_infos = []

with open("players_versions.json", "r") as file:
    versions = json.load(file)

try:
    for version in versions:
        # For each version, we extract all information available.
        date = version[0]
        potential = version[1]
        rating = version[2]
        value = version[3]
        wage = version[4]
        version_id = version[5]
        fifa_edition = version[6]
        player_id = version[7]

        url = f'https://sofifa.com/player/{player_id}/{version}'
        response = session.get(url, headers=headers)
        html = bs.BeautifulSoup(response.text,'html.parser')

        # Getting the player's best position: <li> with "ellipsis" class and with
        # "Best Position" text inside.
        lis = html.find_all("li", "ellipsis")
        for li in lis:
            if "Best Position" in str(li):
                span = li.find("span")
                position = span.text

        # Teams are in the divs with "block-quarter" class.
        divs = html.find_all("div", "block-quarter")
        for div in divs:
            if "Contract" in str(div):
                team_img = div.find("img", {"data-type": "team"})
                team_img_src = team_img["data-src"]
                team_img_data_src = team_img_src.replace("60", "180")

                team_link = div.find("a")
                team = team_link.text

        # Player picture is in the div with "bp3-card player" class.
        player_div = html.find("div", "bp3-card player")
        player_img = player_div.find("img")
        player_img_data_srcset = player_img["data-srcset"]

        player_img_srcs = player_img_data_srcset.split(",")
        player_img_src = player_img_srcs[1].replace("3x", "").strip()

    players_infos.append({
        "player_id": player_id,
        "date": date,
        "potential": potential,
        "rating": rating,
        "value": value,
        "wage": wage,
        "version_id": version_id,
        "fifa_edition": fifa_edition,
        "position": position,
        "team_img": team_img,
        "team": team,
        "player_img": player_img_src
    })

except Exception as e:
    now = datetime.now()
    dt_string = now.strftime(DATE_FORMAT)
    players_infos_df = pd.DataFrame(players_infos, index=[0])
    players_infos_df.to_csv()

finally:
    now = datetime.now()
    dt_string = now.strftime(DATE_FORMAT)
    players_infos_df = pd.DataFrame(players_infos, index=[0])
    players_infos_df.to_csv()
