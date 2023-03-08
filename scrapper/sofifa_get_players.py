import bs4 as bs
import requests
import pandas as pd
import time
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import psycopg2.extras


DATE_FORMAT = "%d-%m-%Y %H_%M_%S"
LAST_ID_FILE = "data/last_id_player.txt"
TIME_BETWEEN_PLAYERS = 0.2
TIME_RECONNECT = 4
LIMIT_QUERY = 5000
CONCURRENT_THREADS = 16
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
    team_img_data_src = None
    team_id = None
    for div in divs:
        if "Contract" in str(div):
            team_img = div.find("img", {"data-type": "team"})
            team_img_src = team_img["data-src"]
            team_img_data_src = team_img_src.replace("60", "180")

            team_id = team_img_data_src.split("teams/")[1].split("/")[0]
            team_link = div.find("a")
            team = team_link.text.strip()

    # If a team wasnt found, we'll use the players national team.
    # TODO: criar nova coluna "is_on_national_team" para indicar se ele estava
    # TODO: na seleção. Usar essa busca abaixo como informação. Acrescentar
    # TODO: country_id para ter a informação do logo da seleção.

    # TODO: Criar lookup de times, de seleções e de ligas (id, nome)
    # TODO: Fazer busca nas urls:
    # TODO: https://sofifa.com/teams?type=club e
    # TODO: https://sofifa.com/teams?type=national
    # TODO: Pegar country flag também. Usar country_id como o da seleção
    # TODO: e não da bandeira.
    # TODO: Criar urls de times e de players no código, não salvar no banco.
    # TODO: https://cdn.sofifa.net/teams/100087/360.png exemplo em que 100087
    # TODO: é o id do time. 360 é a resolução da imagem.
    # TODO: Pro player, basta dividir seu id em dois e usar barras. Ex:
    # TODO: 158023 vira 158/023
    # TODO: É apenas uma foto por versão do jogo, então ao final acrescentar
    # TODO: /22_180.png para o FIFA22, por exemplo.

    # TODO: fazer um team_versions. Pegar versions (todas) e fazer um drop
    # TODO: duplicates, mantendo a mais antiga.
    # TODO: Pegar national_teams pro versions também.
    # TODO: Os kits estão presentes em https://cdn.sofifa.net/kits/1/16_0.png
    # TODO: onde por exemplo 1 é o id do time. O ano do jogo vem seguido da
    # TODO: versão do uniforme. 0 é o principal, 1 o segundo, 2 o de goleiro
    # TODO: e qualquer outra versão vem depois (3, 4...)
    # TODO: Pegar também o id da liga

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


def convert_to_zeros(s):
    if s[-1] == 'M':
        return int(float(s[:-1]) * 1000000)
    elif s[-1] == 'K':
        return int(float(s[:-1]) * 1000)
    else:
        return int(float(s))


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

    headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
               AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 \
               Safari/537.36"}
    # TODO: tirar o for e deixar por linha da tabela version
    try:
        players_infos = []
        for player in players:
            time.sleep(TIME_BETWEEN_PLAYERS)
            # print(player)
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

            if version_id == "None":
                player_info = {
                    "player_id": player_id, "version_id": version_id,
                    "team_id": None, "team": None, "date": None,
                    "country": None, "potential": None,
                    "rating": None, "value": None, "wage": None,
                    "fifa_edition": None, "position": None,
                    "player_img": None, "team_img": None,
                    "preferred_foot": None, "weak_foot": None,
                    "skill_moves": None, "intl_reputation": None,
                    "work_rate_atk": None, "work_rate_def": None
                }
                players_infos.append(player_info)

            else:
                session = requests.Session()
                url = f'https://sofifa.com/player/{player_id}/{version_id}'
                response = session.get(url, headers=headers)
                # If the response code is 429, it means that we're sending too
                # many requests and we have to wait.
                retries_count = 0
                while response.status_code == 429:
                    time.sleep(TIME_RECONNECT + retries_count)
                    print("Too many requests")
                    session = requests.Session()
                    response = session.get(url, headers=headers)
                    retries_count += 1
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

                # If date is None, we didn't gather this information
                # when scrapping versions
                if date is None:
                    fifa_date_spans = html.find_all("span", "bp3-button-text")
                    date_spans = [span for span in fifa_date_spans
                                if "FIFA" not in span.text]
                    date = date_spans[0].text
                    fifa_spans = [span for span in fifa_date_spans
                                if "FIFA" in span.text]
                    fifa_edition = fifa_spans[0].text

                if potential is None:
                    divs = html.find_all("div", "block-quarter")
                    for div in divs:
                        if "Potential" in str(div):
                            potential = int(''.join(
                                [c for c in div.text if c.isdigit()]))

                if rating is None:
                    divs = html.find_all("div", "block-quarter")
                    for div in divs:
                        if "Overall Rating" in str(div):
                            rating = int(''.join(
                                [c for c in div.text if c.isdigit()]))

                if value is None:
                    divs = html.find_all("div", "block-quarter")
                    for div in divs:
                        if "Value" in str(div):
                            print(div.text)
                            value = div.text.replace(
                                "Value", "").replace("€", "")
                            value = convert_to_zeros(value)

                if wage is None:
                    divs = html.find_all("div", "block-quarter")
                    for div in divs:
                        if "Wage" in str(div):
                            print(div.text)
                            wage = div.text.replace("Wage", "").replace("€", "")
                            wage = convert_to_zeros(wage)

                lis = html.find_all("li", "ellipsis")
                for li in lis:
                    if "Preferred Foot" in li.text:
                        preferred_foot = li.text.replace("Preferred Foot", "")
                    if "Weak Foot" in li.text:
                        weak_foot = li.text.replace("Weak Foot", "").strip()
                    if "Skill Moves" in li.text:
                        skill_moves = li.text.replace("Skill Moves", "").strip()
                    if "International Reputation" in li.text:
                        intl_reputation = li.text.replace(
                            "International Reputation", "").strip()
                    if "Work Rate" in li.text:
                        work_rate = li.text.replace(
                            "Work Rate", "").strip()
                        if "N/A" in work_rate:
                            work_rate_atk = None
                            work_rate_def = None
                        else:
                            work_rate_atk, work_rate_def = work_rate.split("/")
                            work_rate_atk = work_rate_atk.strip()
                            work_rate_def = work_rate_def.strip()

                player_info = {
                    "player_id": player_id, "version_id": version_id,
                    "team_id": team_id, "team": team, "date": date,
                    "country": country, "potential": potential,
                    "rating": rating, "value": value, "wage": wage,
                    "fifa_edition": fifa_edition, "position": position,
                    "player_img": image, "team_img": team_img,
                    "preferred_foot": preferred_foot, "weak_foot": weak_foot,
                    "skill_moves": skill_moves,
                    "intl_reputation": intl_reputation,
                    "work_rate_atk": work_rate_atk,
                    "work_rate_def": work_rate_def
                }
                players_infos.append(player_info)

        # print(players_infos)
        insert_into_players_table(pd.DataFrame(players_infos).to_numpy())

    except Exception as e:
        print("Error:", e, player_id, version_id)


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
        preferred_foot, weak_foot, skill_moves, intl_reputation, work_rate_atk,
        work_rate_def)

        VALUES %s
        on conflict(player_id, version_id) do nothing"""

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


def get_versions():
    """Get all the ids that are stored in database. We use this to choose which
    rows we'll fetch from players_versions dataframe.

    Returns:
        ids (list): List of ids.
    """
    print("Querying...")
    conn = psycopg2.connect(CONN_STRING)
    cursor = conn.cursor()
    try:
        query = f"""
        select v.* from sofifa.versions v left outer join sofifa.player p
        on v.player_id = p.player_id and v.version_id = p.version_id
        where p.player_id is null
        limit {LIMIT_QUERY};"""
        cursor.execute(query)
        conn.commit()
        versions = cursor.fetchall()
        return versions
    except Exception as error:
        print(error)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    try:
        versions_len = 1
        while versions_len != 0:
            print(f"Querying versions")
            versions = get_versions()

            versions_len = len(versions)
            # Creating many subsets to ease the processing and parellizing.
            splits = np.array_split(versions, LIMIT_QUERY)
            print("Starting ThreadPoolExecutor")
            # TODO: colocar por version e nao splits.
            with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS)\
            as executor:
                executor.map(get_player, splits)
    except Exception as error_players:
       print("Error while obtaining players information")
       print(error_players)
