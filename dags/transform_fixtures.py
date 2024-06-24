"DAG that runs a transformation on data in DuckDB using the Astro SDK."

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime
import pandas as pd

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #
from include.logic import df_cleanup_1 as dfc
from include.logic import core_filter_1 as cf1
from include.logic import core_filter_2 as cf2

from include.global_variables import global_variables as gv



# from include.logic import apply_filtering_logic, won_last_5_matches


def lenzi(df):
    """Function to check for empty dataframe
    https://stackoverflow.com/questions/19828822/how-do-i-check-if-a-pandas-dataframe-is-empty
    """
    return len(df.index) == 0

# creating a dataframe to show if no fixtures obey filtering conditions
default_df = pd.DataFrame(columns=['Date', 'HomeTeam', 'HomeScore', 'AwayScore', 'AwayTeam'])

row_data = {
    'Date': 'no',
    'HomeTeam': 'fixtures',
    'HomeScore': 'satisfies',
    'AwayScore': 'condition',
    'AwayTeam': ' yet'
}
default_df = default_df.append(row_data, ignore_index=True)


# ----------------- #
# Astro SDK Queries #
# ----------------- #

@aql.dataframe(pool="duckdb")
def find_fixtures(in_table: pd.DataFrame):
    gv.task_log.info(in_table)

    df = in_table
    df = dfc.df_cleanup_1(df)
    
    gv.task_log.info(f"{df}")
    df = cf1.core_filter_1(df)
    gv.task_log.info(f"{df}")
    output_df = cf2.core_filter_2(df)
    # output_df = apply_filtering_logic(df)
    
    gv.task_log.info(output_df)
    if lenzi(output_df) == True:
        gv.task_log.info("df is empty")
        return default_df

    return output_df

# @aql.dataframe(pool="duckdb")
# def find_fixtures_c2(in_table: pd.DataFrame):
#     gv.task_log.info(in_table)

#     df = in_table

#     output_df = won_last_5_matches(df)
    
#     gv.task_log.info(output_df)
#     if lenzi(output_df) == True:
#         gv.task_log.info("df is empty")
#         return default_df

#     return output_df


# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_DUCKDB_IN_FIXTURES],
    catchup=False,
    default_args=gv.default_args,
    description="Runs transformations on fixtures data in DuckDB.",
    tags=["transformation"],
)
def e_transform_fixtures():


    find_fixtures(
        in_table=Table(
            name=gv.FIXTURES_IN_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
        output_table=Table(
            name=gv.REPORTING_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
    )

    # find_fixtures_c2(
    #     in_table=Table(
    #         name=gv.FIXTURES_IN_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
    #     ),
    #     output_table=Table(
    #         name=gv.REPORTING_TABLE_NAME_2, conn_id=gv.CONN_ID_DUCKDB
    #     ),
    # )


e_transform_fixtures()
