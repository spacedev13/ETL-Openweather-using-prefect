import requests
from rich import print
import os
import dotenv

dotenv.load_dotenv()

key = os.environ['openweather_api']

def weather():
    params_weather = {
        'appid': key,
        'lat': -3.7304512,
        'lon': -38.5217989,
        'lang': 'pt_br'
    }

    url_weather = 'https://api.openweathermap.org/data/2.5/weather'

    try:
        consumo = requests.get(url=url_weather, params=params_weather)
        consumo.raise_for_status()
    except requests.HTTPError as e:
        print(f'Erro: {e}')
    else:
        resposta = consumo.json()
        return resposta
    
"""weather()"""