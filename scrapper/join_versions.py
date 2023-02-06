import pandas as pd
import os
from tqdm import tqdm


def main():
    print("Joining versions dfs into one final df.")
    versions_files = [file for file in os.listdir("scrapper/data")
                      if "versions_df" in file]
    versions_df = pd.DataFrame()
    for version in tqdm(versions_files):
        version_df = pd.read_csv(f"scrapper/data/{version}", index_col=0)
        versions_df = pd.concat([versions_df, version_df])

    versions_df.drop_duplicates(inplace=True)
    versions_df.to_csv("scrapper/data/concatenated/players_versions.csv")


if __name__ == "__main__":
    main()
