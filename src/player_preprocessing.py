"""
    DVC stage that performs minimal processing on the `Player` table
"""
import pandas as pd


def main():
    """
    Load all the tables from the database and save them as csv files.
    """
    df_players = pd.read_csv("data/interim/player.csv")

    # Drop the columns that are not needed
    df_players = df_players.drop(columns=["player_api_id"])

    # Transform the weight column from pounds to kilograms
    df_players["weight"] = df_players["weight"].apply(lambda x: x * 0.453592)

    # Change date format in birthday column to YYYY-MM-DD
    df_players["birthday"] = pd.to_datetime(df_players["birthday"]).dt.strftime(
        "%Y-%m-%d"
    )

    # Save the processed table
    df_players.to_csv("data/interim/player_preprocessed.csv", index=False)


if __name__ == "__main__":
    main()
