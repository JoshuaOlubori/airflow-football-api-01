import os
import csv
import requests
from requests.exceptions import RequestException
from urllib3.exceptions import NewConnectionError, ConnectTimeoutError
from time import sleep

from include.global_variables import global_variables as gv


unique_league_ids = set(gv.LEAGUE_IDS)

url = gv.API_ENDPOINT
headers = {
    "X-RapidAPI-Key": gv.API_KEY,
    "X-RapidAPI-Host": gv.API_HOST
}

total_calls = len(unique_league_ids)
current_call = 0

def fetch_data(chosen_season="2023"):
    global current_call
    for league_id in unique_league_ids:
        try:
            querystring = {"league": str(league_id), "season": chosen_season}
            response = requests.get(url, headers=headers, params=querystring)

            response.raise_for_status()

            data = response.json()['response']

            csv_data = []
            for fixture in data:
                fixture_data = {
                    'date': fixture['fixture']['date'],
                    'season': fixture['league']['season'],
                    'league_name': fixture['league']['name'],
                    'country': fixture['league']['country'],
                    'home_team': fixture['teams']['home']['name'],
                    'home_team_score': fixture['goals']['home'],
                    'away_team_score': fixture['goals']['away'],
                    'away_team': fixture['teams']['away']['name'],
                    'match_status': fixture['fixture']['status']['long']
                }
                csv_data.append(fixture_data)

            folder_name = os.path.join(gv.FIXTURES_DATA_FOLDER, f"{csv_data[0]['league_name']}_{csv_data[0]['country']}")
            os.makedirs(folder_name, exist_ok=True)

            csv_file_name = f"{csv_data[0]['league_name']}_{csv_data[0]['season']}.csv"
            csv_file_path = os.path.join(folder_name, csv_file_name)

            with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
                fieldnames = ['date', 'season', 'league_name', 'country', 'home_team',
                            'home_team_score', 'away_team_score', 'away_team', 'match_status']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                writer.writeheader()

                writer.writerows(csv_data)

            current_call += 1
            gv.task_log.info(f"{csv_data[0]['country']} {csv_data[0]['league_name']} called: ({current_call}/{total_calls})")
            gv.task_log.info(f"\nCSV file saved at: {csv_file_path}")


        except (NewConnectionError, ConnectTimeoutError) as e:
            gv.task_log.warning(f"Connection error in API call for league {league_id}: {e}")
            gv.task_log.warning("Ensure your internet connection is stable. Exiting the program.")

        except RequestException as e:
            gv.task_log.warning(f"Error in API call for league {league_id}: {e}")

        except Exception as e:
            gv.task_log.warning(f"Unexpected error in processing league {league_id}: {e}")

        # Sleep for 3 seconds before the next API call
        sleep(3)
