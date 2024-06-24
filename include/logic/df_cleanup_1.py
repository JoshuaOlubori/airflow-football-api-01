import pandas as pd
from include.global_variables import global_variables as gv

def df_cleanup_1(df):

    df = df.drop_duplicates()
    df = df[df["match_status"] == "Match Finished"]

    df['date'] = pd.to_datetime(df['date'], utc=True)

    df['home_team_score'] = pd.to_numeric(
        df['home_team_score'], errors='coerce').astype('Int64')
    
    df['away_team_score'] = pd.to_numeric(
        df['away_team_score'], errors='coerce').astype('Int64')

    gv.task_log.info("Clean up successful.\n")
    # gv.task_log.info("Import successful for all files.\n")
    gv.task_log.info(df.info())
    # gv.task_log.info("\n")
    return df