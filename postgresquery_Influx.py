import psycopg2
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from time import sleep
import requests
import influxdb_client

## // Postgres Connectivity
pg_conn = psycopg2.connect(
    host="host with postgres on it",
    database="database",
    user="psql user",
    password="")

## // InfluxDB Information:
token = "INFLUX TOKEN GOES HERE"
org = "Val_Tracker"
bucket = "Val_Tracker"
url="http://InfluxDBHost:8086"

client = InfluxDBClient(
    url=url,
    token=token,
    org=org
)

def pg_headshotsTotal():
    print('Connecting to DB...')
    sleep(3)
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute('SELECT SUM (headshots) AS total FROM gamehistory;')
    pg_headshots = pg_cursor.fetchall()
    pg_headshots = str(pg_headshots)[1:-1][1:-1][0:-1]
    print(pg_headshots)
    client = InfluxDBClient(
        url=url,
        token=token,
        org=org
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    headshots_count = influxdb_client.Point("headshots in game").field("total headshots", pg_headshots)
    write_api.write(bucket=bucket, org=org, record=headshots_count)
pg_headshotsTotal()

def totalKills():
    #print('Connecting to DB...')
    sleep(3)
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute('SELECT SUM (kills) AS total FROM gamehistory;')
    pg_kills = pg_cursor.fetchall()
    pg_kills = str(pg_kills)[1:-1][1:-1][0:-1]
    print(pg_kills)
    client = InfluxDBClient(
        url=url,
        token=token,
        org=org
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    headshots_count = influxdb_client.Point("total kills").field("total kills", pg_kills)
    write_api.write(bucket=bucket, org=org, record=headshots_count)
totalKills()

def totalKdr():
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute('SELECT SUM (kills) AS total FROM gamehistory;')
    pg_kills = pg_cursor.fetchall()
    pg_kills = str(pg_kills)[1:-1][1:-1][0:-1]
    pg_cursor.execute('SELECT SUM (deaths) AS total FROM gamehistory;')
    pg_deaths = pg_cursor.fetchall()
    pg_deaths = str(pg_deaths)[1:-1][1:-1][0:-1]
    kdr_math = int(pg_kills) / int(pg_deaths)
    kdr_math = "{:.2f}".format(kdr_math)
    print(kdr_math)
    client = InfluxDBClient(
        url=url,
        token=token,
        org=org
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    kdr_math = influxdb_client.Point("total kdr").field("total kdr", kdr_math)
    write_api.write(bucket=bucket, org=org, record=kdr_math)

totalKdr()

def totalGames():
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute('SELECT count(*) FROM gamehistory;')
    pg_games = pg_cursor.fetchall()
    pg_gamesStr = str(pg_games)[1:-1][1:-1][0:-1]
    client = InfluxDBClient(
        url=url,
        token=token,
        org=org
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    kdr_math = influxdb_client.Point("total games played").field("games total", pg_gamesStr)
    write_api.write(bucket=bucket, org=org, record=kdr_math)
    # print(pg_gamesStr)

totalGames()

def charKills():
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute('SELECT character, kills FROM gamehistory;')
    pg_Char = pg_cursor.fetchall()
    for pg_Char in pg_Char:
        pg_strChar = str(pg_Char[0])
        pg_intChar = pg_strChar
        pg_strstatsChar = str(pg_Char[1])
        pg_intstatsChar = int(pg_strstatsChar)
        pg_cursor.execute
        # print(pg_strChar)
        # print(pg_strstatsChar)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        kdr_math = influxdb_client.Point("char played kills - stats").tag("characters played", pg_intChar).field("char stats - kills", pg_intstatsChar)
        write_api.write(bucket=bucket, org=org, record=kdr_math)
charKills()


def characterplayedTotal():
    pg_cursor = pg_conn.cursor()
# // Cypher Games Played
    pg_cursor.execute("SELECT count(character) FROM gamehistory where character like 'Cypher';")
    pg_charPlayed = pg_cursor.fetchall()
    pg_strcharPlayed = str(pg_charPlayed)[1:-1][1:-1][0:-1]
    pg_intcharPlayed = int(pg_strcharPlayed)
# // KJ Games Played
    pg_cursor.execute("SELECT count(character) FROM gamehistory where character like 'Killjoy';")
    pg_charKJPlayed = pg_cursor.fetchall()
    pg_strKJPlayed = str(pg_charKJPlayed)[1:-1][1:-1][0:-1]
    pg_intKJPlayed = int(pg_strKJPlayed)
# // Reyna Games Played
    pg_cursor.execute("SELECT count(character) FROM gamehistory where character like 'Reyna';")
    pg_charReynaPlayed = pg_cursor.fetchall()
    pg_strReynaPlayed = str(pg_charReynaPlayed)[1:-1][1:-1][0:-1]
    pg_intReynaPlayed = int(pg_strReynaPlayed)
# // Fade Games Played
    pg_cursor.execute("SELECT count(character) FROM gamehistory where character like 'Fade';")
    pg_charFadePlayed = pg_cursor.fetchall()
    pg_strFadePlayed = str(pg_charFadePlayed)[1:-1][1:-1][0:-1]
    pg_intFadePlayed = int(pg_strFadePlayed)
    # print(pg_strcharPlayed)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    cypher_math = influxdb_client.Point("cypher kills").field("cypher played total", pg_intcharPlayed)
    killjoy_math = influxdb_client.Point("killjoy total").field("killjoy played total", pg_intKJPlayed)
    reyna_math = influxdb_client.Point("reyna total").field("reyna played total", pg_intReynaPlayed)
    fade_math = influxdb_client.Point("fade total").field("fade played total", pg_intFadePlayed )
    write_api.write(bucket=bucket, org=org, record=cypher_math)
    write_api.write(bucket=bucket, org=org, record=killjoy_math)
    write_api.write(bucket=bucket, org=org, record=reyna_math)
    write_api.write(bucket=bucket, org=org, record=fade_math)
characterplayedTotal()

def map_playedCounts():
    pg_cursor = pg_conn.cursor()
# // ASCENT
    pg_cursor.execute("SELECT count(map) FROM gamehistory where map like 'Ascent';")
    pg_ascentPlayed = pg_cursor.fetchall()
    pg_strascentPlayed = str(pg_ascentPlayed)[1:-1][1:-1][0:-1]
    pg_intascentPlayed = int(pg_strascentPlayed)
# // BIND
    pg_cursor.execute("SELECT count(map) FROM gamehistory where map like 'Bind';")
    pg_bindPlayed = pg_cursor.fetchall()
    pg_strbindPlayed = str(pg_bindPlayed)[1:-1][1:-1][0:-1]
    pg_intbindPlayed = int(pg_strbindPlayed)
# // BREEZE
    pg_cursor.execute("SELECT count(map) FROM gamehistory where map like 'Breeze';")
    pg_breezePlayed = pg_cursor.fetchall()
    pg_strbreezePlayed = str(pg_breezePlayed)[1:-1][1:-1][0:-1]
    pg_intbreezePlayed = int(pg_strbreezePlayed)
# // ICEBOX
    pg_cursor.execute("SELECT count(map) FROM gamehistory where map like 'Icebox';")
    pg_iceboxPlayed = pg_cursor.fetchall()
    pg_striceboxPlayed = str(pg_iceboxPlayed)[1:-1][1:-1][0:-1]
    pg_inticeboxPlayed = int(pg_striceboxPlayed)
# // SPLIT
    pg_cursor.execute("SELECT count(map) FROM gamehistory where map like 'Split';")
    pg_splitPlayed = pg_cursor.fetchall()
    pg_strsplitPlayed = str(pg_splitPlayed)[1:-1][1:-1][0:-1]
    pg_intsplitPlayed = int(pg_strsplitPlayed)
# // HAVEN
    pg_cursor.execute("SELECT count(map) FROM gamehistory where map like 'Haven';")
    pg_havenPlayed = pg_cursor.fetchall()
    pg_strhavenPlayed = str(pg_havenPlayed)[1:-1][1:-1][0:-1]
    pg_inthavenPlayed = int(pg_strhavenPlayed)
# // FRACTURE
    pg_cursor.execute("SELECT count(map) FROM gamehistory where map like 'Fracture';")
    pg_fracturePlayed = pg_cursor.fetchall()
    pg_strfracturePlayed = str(pg_fracturePlayed)[1:-1][1:-1][0:-1]
    pg_intfracturePlayed = int(pg_strfracturePlayed)

## // INFLUX
    write_api = client.write_api(write_options=SYNCHRONOUS)
    ascent_maps_total = influxdb_client.Point("maps played total").tag("maps","ascent" ).field("ascent played", pg_intascentPlayed)
    breeze_maps_total = influxdb_client.Point("maps played total").tag("maps","breeze").field("breeze played", pg_intbreezePlayed)
    icebox_maps_total = influxdb_client.Point("maps played total").tag("maps","icebox").field("icebox played", pg_inticeboxPlayed)
    split_maps_total = influxdb_client.Point("maps played total").tag("maps","split").field("split played", pg_intsplitPlayed)
    haven_maps_total = influxdb_client.Point("maps played total").tag("maps","haven").field("haven played", pg_inthavenPlayed)
    fracture_maps_total = influxdb_client.Point("maps played total").tag("maps","fracture").field("fracture played", pg_intfracturePlayed)
    bind_maps_total = influxdb_client.Point("maps played total").tag("maps","bind").field("bind played", pg_intbindPlayed)
    write_api.write(bucket=bucket, org=org, record=ascent_maps_total)
    write_api.write(bucket=bucket, org=org, record=breeze_maps_total)
    write_api.write(bucket=bucket, org=org, record=icebox_maps_total)
    write_api.write(bucket=bucket, org=org, record=split_maps_total)
    write_api.write(bucket=bucket, org=org, record=haven_maps_total)
    write_api.write(bucket=bucket, org=org, record=fracture_maps_total)
    write_api.write(bucket=bucket, org=org, record=bind_maps_total)
map_playedCounts()
