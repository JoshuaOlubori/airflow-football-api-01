import numpy as np
import pandas as pd
from include.global_variables import global_variables as gv

def core_filter_1(df):
    try:
        gv.task_log.info("Applying core filtering logic 1...  This would take some time.\n")

        # Initialize an empty list to store the column values
        won_3_games_or_more = []

        def team_won_3_games_or_more(team, g):
            """
                Core logic of the program:
                Function to check for the counts of wins of both teams in the current fixture,
                against teams in their last 5 games
            """
            win_count = 0
            for index, row in g.iterrows():
                if row["home_team"] != team:
                    # Swap home and away team names and scores
                    g.loc[index, "home_team"] = row["away_team"]
                    g.loc[index, "away_team"] = row["home_team"]

                    g.loc[index, "home_team_score"] = row["away_team_score"]
                    g.loc[index, "away_team_score"] = row["home_team_score"]

            win_count = 0
            for index, row in g.iterrows():
                if g.loc[index, "home_team_score"] > g.loc[index, "away_team_score"]:
                    win_count += 1
            if win_count >= 3:
                return True
            else:
                return False

        # Find all match containers
        match_containers = df.to_dict('records')

        # Loop through each match container
        for match in match_containers:
            home_team = match['home_team']
            away_team = match['away_team']

            # Get the last 5 fixtures for both home and away teams
            last_5_home_team_fixtures = df[(df['home_team'] == home_team) | (df['away_team'] == home_team)].sort_values(by='date', ascending=False).head(5)
            last_5_away_team_fixtures = df[(df['home_team'] == away_team) | (df['away_team'] == away_team)].sort_values(by='date', ascending=False).head(5)

            # Get all opponents for either teams, including both home and away teams
            last_5_home_team_opps = pd.unique(last_5_home_team_fixtures[["home_team", "away_team"]].stack())
            last_5_away_team_opps = pd.unique(last_5_away_team_fixtures[["home_team", "away_team"]].stack())

            # Convert NumPy arrays to Python sets
            last_5_home_team_opps_set = set(last_5_home_team_opps)
            last_5_away_team_opps_set = set(last_5_away_team_opps)

            # Find common opponents
            common_opponents = last_5_home_team_opps_set.intersection(last_5_away_team_opps_set)

            # Assuming home_team and away_team are variables that represent the current teams
            current_teams = set([home_team, away_team])
            common_opponents = common_opponents.difference(current_teams)

            # common opponents - home team
            condition_1 = last_5_home_team_fixtures['home_team'].isin(common_opponents) | last_5_home_team_fixtures['away_team'].isin(common_opponents)
            g = last_5_home_team_fixtures[condition_1]

            # common opponents - away team
            condition_2 = last_5_away_team_fixtures['home_team'].isin(common_opponents) | last_5_away_team_fixtures['away_team'].isin(common_opponents)
            h = last_5_away_team_fixtures[condition_2]

            # Check if count of common opponents fixtures is greater than 2
            if len(common_opponents) > 2:
                if team_won_3_games_or_more(home_team, g) or team_won_3_games_or_more(away_team, h):
                    won_3_games_or_more.append(1)
                else:
                    won_3_games_or_more.append(0)
            else:
                won_3_games_or_more.append(0)

        # Add the new column to the original dataframe if the length is equal
        try:
            if len(won_3_games_or_more) == len(df):
                df['won_3_games_or_more'] = won_3_games_or_more
                gv.task_log.info("New column 'won_3_games_or_more' added to the dataframe.")
            else:
                gv.task_log.warning("Length of 'won_3_games_or_more' list does not match the length of the dataframe.")
        except Exception as e:
            gv.task_log.warning(f"Error adding new column: {e}")

    except Exception as e:
        gv.task_log.warning(f"Error during processing: {e}")
    return df


def core_filter_1_optimised(df):
    try:
        gv.task_log.info("Applying filtering logic...  This would take some time.\n")

        # Find all match containers
        match_containers = df.to_dict('records')

        # Initialize an empty list to store the column values
        won_3_games_or_more = []

        # Vectorized implementation
        teams = df[['home_team', 'away_team']].stack().unique()
        for team in teams:
            team_fixtures = df[(df['home_team'] == team) | (df['away_team'] == team)].sort_values(by='date', ascending=False).head(5)
            opponents = team_fixtures[['home_team', 'away_team']].stack().unique()
            opponents = opponents[~np.isin(opponents, team)]  # Remove the team itself

            for opponent in opponents:
                condition_1 = (team_fixtures['home_team'] == opponent) | (team_fixtures['away_team'] == opponent)
                fixtures_against_opponent = team_fixtures[condition_1]

                if len(fixtures_against_opponent) >= 3:
                    home_won = (fixtures_against_opponent['home_team'] == team) & (fixtures_against_opponent['home_team_score'] > fixtures_against_opponent['away_team_score'])
                    away_won = (fixtures_against_opponent['away_team'] == team) & (fixtures_against_opponent['away_team_score'] > fixtures_against_opponent['home_team_score'])
                    win_count = home_won.sum() + away_won.sum()

                    if win_count >= 3:
                        won_3_games_or_more.extend([1] * len(match_containers))
                        break

            if len(won_3_games_or_more) < len(match_containers):
                won_3_games_or_more.extend([0] * (len(match_containers) - len(won_3_games_or_more)))

        # Add the new column to the original dataframe if the length is equal
        try:
            if len(won_3_games_or_more) == len(df):
                df['won_3_games_or_more'] = won_3_games_or_more
                gv.task_log.info("New column 'won_3_games_or_more' added to the dataframe.")
            else:
                gv.task_log.warning("Length of 'won_3_games_or_more' list does not match the length of the dataframe.")
        except Exception as e:
            gv.task_log.warning(f"Error adding new column: {e}")

    except Exception as e:
        gv.task_log.warning(f"Error during processing: {e}")

    return df