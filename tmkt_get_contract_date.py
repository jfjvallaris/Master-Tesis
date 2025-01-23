# -*- coding: utf-8 -*-
"""
Created on Mon Jan 13 15:29:35 2025

@author: Working-Ntb
"""

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import re

from _functions import *


# %%
# Función para verificar y obtener la URL de transferencia
def get_transfer_url(row):
    """
    Para una fila del DataFrame, verifica si la transferencia coincide y devuelve la URL.
    """
    player_id = row['player_id']
    club_to_code = row['code_to']
    club_from_code = row['code_from']
    transfer_fee = row['transfer_fee']
    transfer_date = row['transfer_date']

    # print(player_id, transfer_date , club_from_code, club_to_code , transfer_fee)
    
    # Obtener datos de la API
    url_base = f"https://www.transfermarkt.es"
    url = f"https://www.transfermarkt.es/ceapi/transferHistory/list/{player_id}"
    # print(url)
    response = requests.get(url, headers=request_headers)
    if response.status_code != 200:
        print(f"Error al acceder a {url}")
        return None

    # Procesar la respuesta JSON
    data = response.json()
    transfers = data.get("transfers", [])

    # print(transfers)

    for transfer in transfers:
        # Comparar los valores de transferencia
        api_date = transfer.get("dateUnformatted")  # Formato YYYY-MM-DD
        api_from_code = transfer.get("from", {}).get("href", "").split("/")[-3]
        api_to_code = transfer.get("to", {}).get("href", "").split("/")[-3]
        api_fee = valueText_to_int(transfer.get("fee"))

        # print(api_date, api_from_code, api_to_code, api_fee)
        
        # Verificar si coinciden todos los datos
        if (api_date == transfer_date and
            api_from_code == club_from_code and
            api_to_code == club_to_code and
            api_fee == transfer_fee):
            return transfer.get("url")  # Devolver la URL si coincide
    
    return None  # No se encontró ninguna coincidencia


def get_contract_expiration(row, DEBUG=True):
    """
    Extrae la fecha de vencimiento del contrato desde la página Transfermarkt.
    """
    # Construir la URL completa y obtener el transfer_id
    transfer_url = get_transfer_url(row)

    if not transfer_url:
        if DEBUG:
            print(f"La transferencia de {row['player_name']} del {row['club_from']} a {row['club_to']} el dia {row['transfer_date']} URL de transferencia no encontrada o inválida.")
        return row['ends_contract_date'], row['url_contract']  # Sin cambios

    url = f"https://www.transfermarkt.es{transfer_url}"

    transfer_id = url.split("/")[-1]

    # Realizar la solicitud
    response = requests.get(url, headers=request_headers)
    if response.status_code != 200:
        return row['ends_contract_date'], row['url_contract']

    # Parsear el contenido HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    # Buscar la <div> correspondiente al transfer_id
    transfer_box = soup.find("div", id=transfer_id)
    if not transfer_box:
        return row['ends_contract_date'], row['url_contract']

    # Buscar todos los <td> en la tabla dentro de la box
    table = transfer_box.find("table")
    if not table:
        return row['ends_contract_date'], row['url_contract']

    tds = table.find_all("td")
    # if len(tds) >= 17:
    candidate_td = tds[-2]  # Penúltima celda
    candidate_text = candidate_td.text.strip()
    match = re.search(r"\((\d{2}/\d{2}/\d{4})\)", candidate_text)
    if match:
        if DEBUG:
            print(f"Informacion encontrada {row['player_name']}")
        return match.group(1), url  # Actualizar valores

    return row['ends_contract_date'], row['url_contract']  # Sin cambios


def update_contract_date(df):
    """
    Procesa las filas en paralelo pero actualiza el DataFrame secuencialmente.
    """

    df['ends_contract_date'] = None
    df['url_contract'] = None


    for index, row in df.iterrows():
        print(f"Revisando fila {index} - {row['player_name']}")
        ends_contract_date, url = get_contract_expiration(row)
        df.at[index, 'ends_contract_date'] = ends_contract_date
        df.at[index, 'url_contract'] = url

    df['ends_contract_date'] = pd.to_datetime(df['ends_contract_date'], errors='coerce', dayfirst=True)
    df['ends_contract_date'] = df['ends_contract_date'].dt.strftime('%d/%m/%Y')

    return df

#%%
# =============================================================================
# Ejemplo de uso

# df = pd.read_excel('df_tfmkt.xlsx', dtype={
#     'player_id': str,
#     'code_to': str,
#     'code_from': str
# })

# # Formatear transfer_date en el DataFrame
# df['transfer_date'] = pd.to_datetime(df['transfer_date'], errors='coerce').dt.strftime('%Y-%m-%d')

# df_2 = update_contract_date(df)

# df_2['ends_contract_date'] = pd.to_datetime(df_2['ends_contract_date'], errors='coerce').dt.strftime('%Y-%m-%d')
# =============================================================================
