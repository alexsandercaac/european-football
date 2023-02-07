import pandas as pd
import os
from tqdm import tqdm


def main():
    print("Joining players dfs into one final df.")
    players_files = [file for file in os.listdir("scrapper/data/players")
                      if "players_df" in file]
    players_df = pd.DataFrame()
    for player in tqdm(players_files):
        player_df = pd.read_csv(f"scrapper/data/players/{player}", index_col=0)
        players_df = pd.concat([players_df, player_df])

    players_df.drop_duplicates(inplace=True)
    players_df.to_csv("scrapper/data/concatenated/players_full_info.csv")


if __name__ == "__main__":
    main()
