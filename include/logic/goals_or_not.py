import pandas as pd

def both_teams_scored_or_not(df):
    # Create a new column 'both_teams_scored' using vectorized operations
    df['both_teams_scored'] = ((df['home_team_score'] > 0) & (df['away_team_score'] > 0)).astype(int)
    df['teams_did_not_score'] = ((df['home_team_score'] == 0) & (df['away_team_score'] == 0)).astype(int)
    return df


