# # import os
# # import csv
# # import requests
# # from requests.exceptions import RequestException
# # from urllib3.exceptions import NewConnectionError, ConnectTimeoutError
# # from time import sleep

# # from include.global_variables import global_variables as gv


# # unique_league_ids = set(gv.LEAGUE_IDS)

# # url = gv.API_ENDPOINT
# # headers = {
# #     "X-RapidAPI-Key": gv.API_KEY,
# #     "X-RapidAPI-Host": gv.API_HOST
# # }

# # total_calls = len(unique_league_ids)
# # current_call = 0

# # def fetch_data(chosen_season="2023"):
# #     global current_call
# #     for league_id in unique_league_ids:
# #         try:
# #             querystring = {"league": str(league_id), "season": chosen_season}
# #             response = requests.get(url, headers=headers, params=querystring)

# #             response.raise_for_status()

# #             data = response.json()['response']

# #             csv_data = []
# #             for fixture in data:
# #                 fixture_data = {
# #                     'date': fixture['fixture']['date'],
# #                     'season': fixture['league']['season'],
# #                     'league_name': fixture['league']['name'],
# #                     'country': fixture['league']['country'],
# #                     'home_team': fixture['teams']['home']['name'],
# #                     'home_team_score': fixture['goals']['home'],
# #                     'away_team_score': fixture['goals']['away'],
# #                     'away_team': fixture['teams']['away']['name'],
# #                     'match_status': fixture['fixture']['status']['long']
# #                 }
# #                 csv_data.append(fixture_data)

# #             folder_name = os.path.join(gv.FIXTURES_DATA_FOLDER, f"{csv_data[0]['league_name']}_{csv_data[0]['country']}")
# #             os.makedirs(folder_name, exist_ok=True)

# #             csv_file_name = f"{csv_data[0]['league_name']}_{csv_data[0]['season']}.csv"
# #             csv_file_path = os.path.join(folder_name, csv_file_name)

# #             with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
# #                 fieldnames = ['date', 'season', 'league_name', 'country', 'home_team',
# #                             'home_team_score', 'away_team_score', 'away_team', 'match_status']
# #                 writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

# #                 writer.writeheader()

# #                 writer.writerows(csv_data)

# #             current_call += 1
# #             gv.task_log.info(f"{csv_data[0]['country']} {csv_data[0]['league_name']} called: ({current_call}/{total_calls})")
# #             gv.task_log.info(f"\nCSV file saved at: {csv_file_path}")


# #         except (NewConnectionError, ConnectTimeoutError) as e:
# #             gv.task_log.warning(f"Connection error in API call for league {league_id}: {e}")
# #             gv.task_log.warning("Ensure your internet connection is stable. Exiting the program.")

# #         except RequestException as e:
# #             gv.task_log.warning(f"Error in API call for league {league_id}: {e}")

# #         except Exception as e:
# #             gv.task_log.warning(f"Unexpected error in processing league {league_id}: {e}")

# #         # Sleep for 3 seconds before the next API call
# #         sleep(3)



# import os
# import csv
# import requests
# from requests.exceptions import RequestException
# from urllib3.exceptions import NewConnectionError, ConnectTimeoutError
# from time import sleep

# from include.global_variables import global_variables as gv


# unique_league_ids = set(gv.LEAGUE_IDS)

# url = gv.API_ENDPOINT_1
# headers = {
#     "X-RapidAPI-Key": gv.API_KEY,
#     "X-RapidAPI-Host": gv.API_HOST
# }

# total_calls = len(unique_league_ids)
# current_call = 0

# def fetch_data(chosen_season="2023"):
#     global current_call
#     for league_id in unique_league_ids:
#         try:
#             querystring = {"league": str(league_id), "season": chosen_season}
#             response = requests.get(url, headers=headers, params=querystring)
#             response.raise_for_status()

#             data = response.json()['response']
#             gv.task_log.info("data recieved")

#             csv_data = []
#             for fixture in data:
#                 fixture_id = fixture['fixture']['id']
#                 fixture_data = {
#                     'date': fixture['fixture']['date'],
#                     'season': fixture['league']['season'],
#                     'league_name': fixture['league']['name'],
#                     'country': fixture['league']['country'],
#                     'home_team': fixture['teams']['home']['name'],
#                     'home_team_score': fixture['goals']['home'],
#                     'away_team_score': fixture['goals']['away'],
#                     'away_team': fixture['teams']['away']['name'],
#                     'match_status': fixture['fixture']['status']['long'],
#                     'home_corners': None,
#                     'away_corners': None,
#                     'home_yellow_cards': None,
#                     'away_yellow_cards': None,
#                     'home_red_cards': None,
#                     'away_red_cards': None
#                 }

#                 sleep(3)
#                 # Fetch statistics data for the fixture
#                 stats_url = gv.API_ENDPOINT_1
#                 stats_querystring = {"fixture": str(fixture_id)}
#                 stats_response = requests.get(stats_url, headers=headers, params=stats_querystring)
#                 stats_response.raise_for_status()
                
#                 stats_data = stats_response.json()['response']
#                 gv.task_log.info("\n getting stats data \n")

#                 for stat in stats_data:
#                     if stat['team']['name'] == fixture_data['home_team']:
#                         for item in stat['statistics']:
#                             if item['type'] == 'Corner Kicks':
#                                 fixture_data['home_corners'] = item['value']
#                             elif item['type'] == 'Yellow Cards':
#                                 fixture_data['home_yellow_cards'] = item['value']
#                             elif item['type'] == 'Red Cards':
#                                 fixture_data['home_red_cards'] = item['value']
#                     if stat['team']['name'] == fixture_data['away_team']:
#                         for item in stat['statistics']:
#                             if item['type'] == 'Corner Kicks':
#                                 fixture_data['away_corners'] = item['value']
#                             elif item['type'] == 'Yellow Cards':
#                                 fixture_data['away_yellow_cards'] = item['value']
#                             elif item['type'] == 'Red Cards':
#                                 fixture_data['away_red_cards'] = item['value']

#                 gv.task_log.info("appending to csv")
#                 csv_data.append(fixture_data)

#             folder_name = os.path.join(gv.FIXTURES_DATA_FOLDER, f"{csv_data[0]['league_name']}_{csv_data[0]['country']}")
#             os.makedirs(folder_name, exist_ok=True)

            
#             csv_file_name = f"{csv_data[0]['league_name']}_{csv_data[0]['season']}.csv"
#             csv_file_path = os.path.join(folder_name, csv_file_name)

#             with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
#                 fieldnames = ['date', 'season', 'league_name', 'country', 'home_team',
#                             'home_team_score', 'away_team_score', 'away_team', 'match_status',
#                             'home_corners', 'away_corners', 'home_yellow_cards', 'away_yellow_cards',
#                             'home_red_cards', 'away_red_cards']
#                 writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

#                 writer.writeheader()

#                 writer.writerows(csv_data)

#             current_call += 1
#             gv.task_log.info(f"{csv_data[0]['country']} {csv_data[0]['league_name']} called: ({current_call}/{total_calls})")
#             gv.task_log.info(f"\nCSV file saved at: {csv_file_path}")


#         except (NewConnectionError, ConnectTimeoutError) as e:
#             gv.task_log.warning(f"Connection error in API call for league {league_id}: {e}")
#             gv.task_log.warning("Ensure your internet connection is stable. Exiting the program.")

#         except RequestException as e:
#             gv.task_log.warning(f"Error in API call for league {league_id}: {e}")

#         except Exception as e:
#             gv.task_log.warning(f"Unexpected error in processing league {league_id}: {e}")

#         # Sleep for 3 seconds before the next API call
#         sleep(3)


# \\\\\\\\\\\\\\\\\\\\

# import os
# import csv
# import requests
# from requests.exceptions import RequestException
# from urllib3.exceptions import NewConnectionError, ConnectTimeoutError
# from time import sleep

# from include.global_variables import global_variables as gv


# unique_league_ids = set(gv.LEAGUE_IDS)

# url = gv.API_ENDPOINT_1
# headers = {
#     "X-RapidAPI-Key": gv.API_KEY,
#     "X-RapidAPI-Host": gv.API_HOST
# }

# total_calls = len(unique_league_ids)
# current_call = 0

# def fetch_data(chosen_season="2023"):
#     global current_call

#     try:
#         csv_data = []

#         for league_id in unique_league_ids:
#             querystring = {"league": str(league_id), "season": chosen_season}
#             response = requests.get(url, headers=headers, params=querystring)
#             response.raise_for_status()

#             data = response.json()['response']
#             gv.task_log.info(f"Received data for league ID: {league_id}")

#             for fixture in data:
#                 fixture_id = fixture['fixture']['id']
#                 fixture_data = {
#                     'date': fixture['fixture']['date'],
#                     'season': fixture['league']['season'],
#                     'league_name': fixture['league']['name'],
#                     'country': fixture['league']['country'],
#                     'home_team': fixture['teams']['home']['name'],
#                     'home_team_score': fixture['goals']['home'],
#                     'away_team_score': fixture['goals']['away'],
#                     'away_team': fixture['teams']['away']['name'],
#                     'match_status': fixture['fixture']['status']['long'],
#                     'home_corners': None,
#                     'away_corners': None,
#                     'home_yellow_cards': None,
#                     'away_yellow_cards': None,
#                     'home_red_cards': None,
#                     'away_red_cards': None
#                 }

#                 csv_data.append(fixture_data)

#             current_call += 1
#             gv.task_log.info(f"Processed data for league ID: {league_id} ({current_call}/{total_calls})")

#             sleep(3)
#             # Batch request for statistics
            
#             # fixture_ids = [fixture['fixture']['id'] for fixture in data]
            
#             stats_url = gv.API_ENDPOINT_2
#             stats_querystring = {"fixture": str(fixture_id)}
#             stats_response = requests.get(stats_url, headers=headers, params=stats_querystring)
#             stats_response.raise_for_status()

            

#             stats_data = stats_response.json()['response']

#             # Update csv_data with statistics
#             for fixture_data in csv_data:
#                 fixture_id = fixture_data['fixture']['id']  # Extract fixture_id directly from fixture_data
#                 for stat in stats_data:  # Loop through stats_data['response'] directly
#                     if stat['team']['name'] == fixture_data['home_team']:
#                         for item in stat['statistics']:
#                             if item['type'] == 'Corner Kicks':
#                                 fixture_data['home_corners'] = item['value']
#                             elif item['type'] == 'Yellow Cards':
#                                 fixture_data['home_yellow_cards'] = item['value']
#                             elif item['type'] == 'Red Cards':
#                                 fixture_data['home_red_cards'] = item['value']
#                     elif stat['team']['name'] == fixture_data['away_team']:
#                         for item in stat['statistics']:
#                             if item['type'] == 'Corner Kicks':
#                                 fixture_data['away_corners'] = item['value']
#                             elif item['type'] == 'Yellow Cards':
#                                 fixture_data['away_yellow_cards'] = item['value']
#                             elif item['type'] == 'Red Cards':
#                                 fixture_data['away_red_cards'] = item['value']

#             gv.task_log.info("appending to csv")
#             csv_data.append(fixture_data)
#         # Save to CSV
#         folder_name = os.path.join(gv.FIXTURES_DATA_FOLDER, f"{csv_data[0]['league_name']}_{csv_data[0]['country']}")
#         os.makedirs(folder_name, exist_ok=True)

#         csv_file_name = f"{csv_data[0]['league_name']}_{csv_data[0]['season']}.csv"
#         csv_file_path = os.path.join(folder_name, csv_file_name)

#         with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
#             fieldnames = ['date', 'season', 'league_name', 'country', 'home_team',
#                           'home_team_score', 'away_team_score', 'away_team', 'match_status',
#                           'home_corners', 'away_corners', 'home_yellow_cards', 'away_yellow_cards',
#                           'home_red_cards', 'away_red_cards']
#             writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
#             writer.writeheader()
#             writer.writerows(csv_data)

#         gv.task_log.info(f"CSV file saved at: {csv_file_path}")

#     except (NewConnectionError, ConnectTimeoutError) as e:
#         gv.task_log.warning(f"Connection error: {e}")
#         gv.task_log.warning("Ensure your internet connection is stable. Exiting the program.")
#     except RequestException as e:
#         gv.task_log.warning(f"Error in API call: {e}")
#     except Exception as e:
#         gv.task_log.warning(f"Unexpected error: {e}")
#     finally:
#         # Sleep for 3 seconds before the next API call
#         sleep(3)





import os
import csv
import requests
from requests.exceptions import RequestException
from urllib3.exceptions import NewConnectionError, ConnectTimeoutError
from time import sleep

from include.global_variables import global_variables as gv


unique_league_ids = set(gv.LEAGUE_IDS)

url = gv.API_ENDPOINT_1
headers = {
    "X-RapidAPI-Key": gv.API_KEY,
    "X-RapidAPI-Host": gv.API_HOST
}

total_calls = len(unique_league_ids)
current_call = 0

# def fetch_data(chosen_season="2023"):
#     global current_call

#     try:
#         csv_data = []

#         for league_id in unique_league_ids:
#             querystring = {"league": str(league_id), "season": chosen_season}
#             response = requests.get(url, headers=headers, params=querystring)
#             response.raise_for_status()

#             data = response.json()['response']
#             data = data[:10]
#             gv.task_log.info(f"Received data for league ID: {league_id}")

#             for fixture in data:
#                 fixture_id = fixture['fixture']['id']
#                 fixture_data = {
        
#                     'date': fixture['fixture']['date'],
#                     'season': fixture['league']['season'],
#                     'league_name': fixture['league']['name'],
#                     'country': fixture['league']['country'],
#                     'home_team': fixture['teams']['home']['name'],
#                     'home_team_score': fixture['goals']['home'],
#                     'away_team_score': fixture['goals']['away'],
#                     'away_team': fixture['teams']['away']['name'],
#                     'match_status': fixture['fixture']['status']['long'],
#                     'home_corners': None,
#                     'away_corners': None,
#                     'home_yellow_cards': None,
#                     'away_yellow_cards': None,
#                     'home_red_cards': None,
#                     'away_red_cards': None
#                 }


#                 sleep(3)
#                 stats_url = gv.API_ENDPOINT_2
#                 stats_querystring = {"fixture": str(fixture_id)}
#                 stats_response = requests.get(stats_url, headers=headers, params=stats_querystring)
#                 stats_response.raise_for_status()
#                 stats_data = stats_response.json()['response']

#                 # Update csv_data with statistics
#                 # Assuming csv_data is already populated with fixture_data

#                 for stat in stats_data:
#                     if stat['team']['name'] == fixture_data['home_team']:
#                         for item in stat['statistics']:
#                             if item['type'] == 'Corner Kicks':
#                                 fixture_data['home_corners'] = item['value']
#                             elif item['type'] == 'Yellow Cards':
#                                 fixture_data['home_yellow_cards'] = item['value']
#                             elif item['type'] == 'Red Cards':
#                                 fixture_data['home_red_cards'] = item['value']
#                     elif stat['team']['name'] == fixture_data['away_team']:
#                         for item in stat['statistics']:
#                             if item['type'] == 'Corner Kicks':
#                                 fixture_data['away_corners'] = item['value']
#                             elif item['type'] == 'Yellow Cards':
#                                 fixture_data['away_yellow_cards'] = item['value']
#                             elif item['type'] == 'Red Cards':
#                                 fixture_data['away_red_cards'] = item['value']

#             csv_data.append(fixture_data)
#             sleep(3)

#         # Save to CSV
#         if csv_data:
#             folder_name = os.path.join(gv.FIXTURES_DATA_FOLDER, f"{csv_data[0]['league_name']}_{csv_data[0]['country']}")
#             os.makedirs(folder_name, exist_ok=True)

#             csv_file_name = f"{csv_data[0]['league_name']}_{csv_data[0]['season']}.csv"
#             csv_file_path = os.path.join(folder_name, csv_file_name)

#             with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
#                 fieldnames = ['date', 'season', 'league_name', 'country', 'home_team',
#                             'home_team_score', 'away_team_score', 'away_team', 'match_status',
#                             'home_corners', 'away_corners', 'home_yellow_cards', 'away_yellow_cards',
#                             'home_red_cards', 'away_red_cards']
#                 writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
#                 writer.writeheader()
#                 writer.writerows(csv_data)

#             gv.task_log.info(f"CSV file saved at: {csv_file_path}")
#         else:
#             gv.task_log.warning("No data to save to CSV.")
#     except (NewConnectionError, ConnectTimeoutError) as e:
#         gv.task_log.warning(f"Connection error: {e}")
#         gv.task_log.warning("Ensure your internet connection is stable. Exiting the program.")
#     except RequestException as e:
#         gv.task_log.warning(f"Error in API call: {e}")
#     except Exception as e:
#         gv.task_log.warning(f"Unexpected error: {e}")
#     finally:
#         # Sleep for 3 seconds before the next API call
#         sleep(3)
def fetch_data(chosen_season="2023"):
    global current_call

    try:
        for league_id in unique_league_ids:
            csv_data = []  # Reset csv_data for each league
            querystring = {"league": str(league_id), "season": chosen_season}
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()

            data = response.json()['response']
            data = data[:10]
            gv.task_log.info(f"Received data for league ID: {league_id}")

            for fixture in data:
                fixture_id = fixture['fixture']['id']
                fixture_data = {
                    # 'id':  fixture['fixture']['id'],
                    'date': fixture['fixture']['date'],
                    'season': fixture['league']['season'],
                    'league_name': fixture['league']['name'],
                    'country': fixture['league']['country'],
                    'home_team': fixture['teams']['home']['name'],
                    'home_team_score': fixture['goals']['home'],
                    'away_team_score': fixture['goals']['away'],
                    'away_team': fixture['teams']['away']['name'],
                    'match_status': fixture['fixture']['status']['long'],
                    'home_corners': None,
                    'away_corners': None,
                    'home_yellow_cards': None,
                    'away_yellow_cards': None,
                    'home_red_cards': None,
                    'away_red_cards': None
                }

                sleep(3)
                stats_url = gv.API_ENDPOINT_2
                stats_querystring = {"fixture": str(fixture_id)}
                stats_response = requests.get(stats_url, headers=headers, params=stats_querystring)
                stats_response.raise_for_status()
                stats_data = stats_response.json()['response']

                for stat in stats_data:
                    if stat['team']['name'] == fixture_data['home_team']:
                        for item in stat['statistics']:
                            if item['type'] == 'Corner Kicks':
                                fixture_data['home_corners'] = item['value']
                            elif item['type'] == 'Yellow Cards':
                                fixture_data['home_yellow_cards'] = item['value']
                            elif item['type'] == 'Red Cards':
                                fixture_data['home_red_cards'] = item['value']
                    elif stat['team']['name'] == fixture_data['away_team']:
                        for item in stat['statistics']:
                            if item['type'] == 'Corner Kicks':
                                fixture_data['away_corners'] = item['value']
                            elif item['type'] == 'Yellow Cards':
                                fixture_data['away_yellow_cards'] = item['value']
                            elif item['type'] == 'Red Cards':
                                fixture_data['away_red_cards'] = item['value']

                csv_data.append(fixture_data)
                sleep(3)

            # Save to CSV for this league
            if csv_data:
                league_name = csv_data[0]['league_name']
                country = csv_data[0]['country']
                folder_name = os.path.join(gv.FIXTURES_DATA_FOLDER, f"{league_name}_{country}")
                os.makedirs(folder_name, exist_ok=True)

                csv_file_name = f"{league_name}_{chosen_season}.csv"
                csv_file_path = os.path.join(folder_name, csv_file_name)

                with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
                    fieldnames = ['date', 'season', 'league_name', 'country', 'home_team',
                                  'home_team_score', 'away_team_score', 'away_team', 'match_status',
                                  'home_corners', 'away_corners', 'home_yellow_cards', 'away_yellow_cards',
                                  'home_red_cards', 'away_red_cards']
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(csv_data)

                gv.task_log.info(f"CSV file saved at: {csv_file_path}")
            else:
                gv.task_log.warning(f"No data to save for league ID: {league_id}")

    except (NewConnectionError, ConnectTimeoutError) as e:
        gv.task_log.warning(f"Connection error: {e}")
        gv.task_log.warning("Ensure your internet connection is stable. Exiting the program.")
    except RequestException as e:
        gv.task_log.warning(f"Error in API call: {e}")
    except Exception as e:
        gv.task_log.warning(f"Unexpected error: {e}")
    finally:
        # Sleep for 3 seconds before the next API call
        sleep(3)