from time import sleep
import psycopg2
from psycopg2.extras import execute_values
import json
import requests

## // API Key Information
valt_url = "https://api.henrikdev.xyz/valorant/v3/matches/na/shellgames/0001?filter=unrated"
payload={}
headers = {'User-Agent': 'Mozilla/5.0'}

## // API Call
response = requests.request("GET", valt_url, headers=headers, data=payload)
resp_json = json.loads(response.text)
data = response.json()

for player in data['data']:
    if player.get('metadata'):
        player_map = player.get('metadata')['map']
        match_date = player.get('metadata')['game_start_patched']
        match_dateFormat ="'{}'" .format(match_date)
        if player.get('players'):
            player_get = player.get('players')['all_players']
            for key in player_get:
                if key['name'] == 'shellgames':
                    kdr_kills = key['stats']['kills']
                    kdr_deaths = key['stats']['deaths']
                    headshots = key['stats']['headshots']
                    headshots_format = [headshots]
                    bodyshots = key['stats']['bodyshots']
                    bodyshots_format = [bodyshots]
                    kills = key['stats']['kills']
                    kills_format = [kills]
                    deaths = key['stats']['deaths']
                    deaths_format = [deaths]
                    assists = key['stats']['assists']
                    assists_format = [assists]
                    character = key['character']

## // PostgresQL Table Insertion
                    pg_conn = psycopg2.connect(
                    host="",
                    database="",
                    user="",
                    password="")
                    print('Connecting to DB...')
                    sleep(3)
                    pg_cursor = pg_conn.cursor()
                    pg_cursor.execute('SHOW server_version;')
                    db_version = pg_cursor.fetchone()
                    print('PostgresQL DB Version:', db_version)
                    insert_query = 'INSERT INTO gamehistory (date, headshots, bodyshots, kills, deaths, assists, character, map) VALUES %s'
                    insert_data = [(match_date, headshots, bodyshots, kills, deaths, assists, character, player_map)]
                    pg_select = pg_cursor.execute('SELECT * FROM gamehistory;')
                    execute_values(pg_cursor, insert_query, insert_data, pg_select) 
                    pg_conn.commit()
                    print(pg_select)
