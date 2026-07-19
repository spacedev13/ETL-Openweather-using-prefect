import psycopg2 as pg
import os
import dotenv
from extract import *
dotenv.load_dotenv()
from prefect import flow

dbname = os.environ['pg_database']
port = os.environ['pg_port']
user = os.environ['pg_user']
password = os.environ['pg_pass']
host = os.environ['pg_host']

def get_connect():
    print("=" * 50)
    print("HOST:", host)
    print("PORT:", port)
    print("DB:", dbname)
    print("USER:", user)
    print("=" * 50)
    return pg.connect(
        dbname = dbname,
        port = port,
        user = user,
        password = password,
        host = host
    )

@task(name='Escrevendo no banco - weather', retries=3, retry_delay_seconds=10)
def escrever_weather():
    weather_df = insert_weather()
    conn = get_connect()
    cur = conn.cursor()
    cur.executemany('insert into openweather.weather(id, main, description, icon) values(%s,%s,%s,%s)', weather_df)
    conn.commit()
    cur.close()
    conn.close()

@task(name='Escrevendo no banco - coord', retries=3, retry_delay_seconds=10)
def escrever_coord():
    coord_df = insert_coord()
    conn = get_connect()
    cur = conn.cursor()
    cur.executemany('insert into openweather.coord(lon, lat) values(%s,%s)', coord_df)
    conn.commit()
    cur.close()
    conn.close()

@task(name='Escrevendo no banco - main', retries=3, retry_delay_seconds=10)
def escrever_main():
    main_df = insert_main()
    conn = get_connect()
    cur = conn.cursor()
    cur.executemany('insert into openweather.main(temp, feels_like, temp_min, temp_max, pressure, humidity, sea_level, ground_level) values(%s,%s,%s,%s,%s,%s,%s,%s)', main_df)
    conn.commit()
    cur.close()
    conn.close()

@task(name='Escrevendo no banco - sys', retries=3, retry_delay_seconds=10)
def escrever_sys():
    sys_df = insert_sys()
    conn = get_connect()
    cur = conn.cursor()
    cur.executemany('insert into openweather.sys(type,id,country,sunrise,sunset) values(%s,%s,%s,%s,%s)', sys_df)
    conn.commit()
    cur.close()
    conn.close()

@task(name='Escrevendo no banco - wind', retries=3, retry_delay_seconds=10)
def escrever_wind():
    wind_df = insert_wind()
    conn = get_connect()
    cur = conn.cursor()
    cur.executemany('insert into openweather.wind(speed, deg, gust) values(%s,%s,%s)', wind_df)
    conn.commit()
    cur.close()
    conn.close()

@flow(name="Fluxo de extração Openweather", retries=3, retry_delay_seconds=10)
def insert_dados():
    escrever_weather()
    escrever_coord()
    escrever_main()
    escrever_sys()
    escrever_wind()


