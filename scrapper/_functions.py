# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 12:19:06 2024

@author: Working-Ntb
"""


import time
import requests
from requests.exceptions import Timeout, ConnectionError, HTTPError

import unicodedata
import re

from bs4 import BeautifulSoup
# import pandas as pd
import numpy as np

from diccionario_ligas import ligas_dict

DEBUG=False

request_headers = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
}

url_base = 'https://www.transfermarkt.es'


lista_comunitarios = ['Alemania', 'Austria', 'Bélgica', 'Bulgaria', 'Chipre',
                      'Croacia' 'Dinamarca', 'Eslovaquia' 'Eslovenia',
                      'España', 'Estonia', 'Finlandia', 'Francia', 'Grecia',
                      'Hungría', 'Irlanda', 'Islandia', 'Italia', 'Letonia',
                      'Liechtenstein', 'Lituania', 'Luxemburgo', 'Malta',
                      'Noruega', 'Países Bajos', 'Polonia', 'Portugal',
                      'República Checa', 'Rumania', 'Suecia', 'Suiza']
# https://www.goal.com/es-ar/noticias/que-es-futbolista-comunitario-extracomunitario/epa2dp4zli5z1rtclyu9lmgut


def parse_name(name):
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')
    name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
    parsed_name = '-'.join(name.lower().split())
    return parsed_name


def parse_club_name(name):
    # Convertir a minúsculas y normalizar eliminando acentos
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')

    # Eliminar símbolos no deseados (mantener solo letras, números y espacios)
    name = re.sub(r'[^a-zA-Z0-9\s]', '', name)

    # Convertir a formato con guiones
    parsed_name = '-'.join(name.lower().split())

    return parsed_name


def value_to_int(value):
    value = value.strip().replace('.', '').replace('\'', '')
    try:
        int(value)
    except ValueError:
        value = np.nan
    return value


def eliminar_ii(texto):
    try:
        # Verifica si el texto no es None y termina con " II"
        if texto and texto.endswith(" II"):
            return texto[:-3]
        return texto
    except AttributeError:
        return None


def valueText_to_int(value):
    value = value.strip().replace('.', '').replace(',', '.')
    if value.endswith('mil €'):
        return float(value.replace('mil €', '').strip()) * 1000
    elif value.endswith('mill €'):
        if value.endswith('mil mill €'):
            return round(float(value.replace('mil mill €', '').strip()) * 1000000000, 0)
        return round(float(value.replace('mill €', '').strip()) * 1000000, 0)
    elif value == "Libre":
        return 0
    elif value == "-":
        return np.nan
    elif value == "?":
        return np.nan
    else:
        try:
            return float(value.replace('€', '').strip())
        except ValueError:
            return np.nan
    return value


def make_request(url, params=None, retries=5, timeout=20, retry_delay=5, **kwargs):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response

        except Timeout:
            print(f"Intento {attempt + 1} de {retries}: Tiempo de espera excedido. Reintentando...")
            time.sleep(retry_delay)

        except ConnectionError as e:
            print(f"Intento {attempt + 1} de {retries}: Error de conexión: {e}. Reintentando...")
            time.sleep(retry_delay)

        except HTTPError as e:
            if response.status_code in [403, 500, 502, 504]:
                error_type = "502 Bad Gateway" if response.status_code == 502 else "500 or 504 Gateway Time-out"
                print(f"Intento {attempt + 1} de {retries}: Error {error_type}. Reintentando en {retry_delay} segundos...")
                time.sleep(retry_delay)
            # elif response.status_code == 403:
            #     print(f"Intento {attempt + 1} de {retries}: Error 403 - Esperando 1 minutos para reintentar...")
            #     time.sleep(60)  # Espera de 1 minuto
            else:
                print(f"Error HTTP: {e}. No es un error recuperable, deteniendo reintentos.")
                return None

    print("Error: No se pudo establecer la conexión después de varios intentos.")
    return None

