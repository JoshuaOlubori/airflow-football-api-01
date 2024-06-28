import pandas as pd

def add_over_columns(df):
    total_goals = df['home_team_score'] + df['away_team_score']
    df['over_0_5'] = (total_goals > 0).astype(int)
    df['over_1_5'] = (total_goals > 1).astype(int)
    df['over_2_5'] = (total_goals > 2).astype(int)
    df['over_3_5'] = (total_goals > 3).astype(int)
    return df

def add_under_columns(df):
    total_goals = df['home_team_score'] + df['away_team_score']
    df['under_0_5'] = (total_goals == 0).astype(int)
    df['under_1_5'] = (total_goals <= 1).astype(int)
    df['under_2_5'] = (total_goals <= 2).astype(int)
    df['under_3_5'] = (total_goals <= 3).astype(int)
    df['under_4_5'] = (total_goals <= 4).astype(int)
    return df

def add_draw_columns(df):
    df['draw'] =  (df['home_team_score'] == df['away_team_score']).astype(int)
    return df
