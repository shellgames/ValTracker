import psycopg2
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from time import sleep
import requests
import influxdb_client

## // Postgres Connectivity
pg_conn = psycopg2.connect(
    host="host",
    database="databaser",
    user="user",
    password="pass")

## // InfluxDB Information:
token = "influx API token"
org = "Val_Tracker"
bucket = "Val_Tracker"
url="http://influxdb:8086"

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

def totalkdr():
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
