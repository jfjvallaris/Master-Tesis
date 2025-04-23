# -*- coding: utf-8 -*-
"""
Created on Sat Jun 22 22:09:45 2024

@author: Terminal-NTB
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import json
from datetime import datetime

from tmkt_rendimiento import *
from tmkt_info_club import *
from tmkt_get_contract_date import *
from _functions import *

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# %%

def obtener_url(season, seas_w='ambas', prestamos='sin', internos=False, pais='Argentina'):
    for k, v in ligas_dict[pais].items():
        liga_name = k
        liga_code = v

    if (pais=='Argentina') and (int(season)>2023):
        liga_code='ARG1'

    url = f'{url_base}/{liga_name}/transfers/wettbewerb/{liga_code}/plus/?saison_id={season}'

    if seas_w == 'verano':
        url += '&s_w=s'
    elif seas_w == 'invierno':
        url += '&s_w=w'
    
    if prestamos == 'todos':
        url += '&leihe=1'
    elif prestamos == 'solo':
        url += '&leihe=2'
    elif prestamos == 'sin_vuelven':
        url += '&leihe=3'
    elif prestamos == 'sin':
        url += '&leihe=0'
    
    if internos:
        url += '&intern=1'
    else:
        url += '&intern=0'
    
    return url

def obtener_valor_transferencia(dic_player, DEBUG=False):
    '''
    dic_player = {
        "player_name": player_name,
        "player_url": player_url,
        "player_id": player_id,
        "age": age,
        "nationality": nationality,
        "position": position,
        "club_from": club_from,
        "club_to": club_to,
        "code_from": code_from,
        "code_to": code_from,
        "transfer_fee": transfer_fee,
        "season": seas,
        "season_w": seas_w
    }

    player_id = 55904
    player_id = 45317 # Retirado
    club_from_code = '12301'
    club_to_code = '6062'
    season = 2021
    '''

    player_name = dic_player["player_name"]

    player_id = dic_player["player_id"]
    club_from_code = dic_player['code_from']
    club_to_code = dic_player['code_to']
    season = dic_player['season']
    season = f"{str(season)[-2:]}/{str(season + 1)[-2:]}"
    transfer_fee = dic_player["transfer_fee"]

    url = f"{url_base}/ceapi/transferHistory/list/{player_id}"
    response = make_request(url, headers=request_headers)
    # response.raise_for_status()  # Asegurarse de que la solicitud fue exitosa
    data = response.json()

    transfers = data.get("transfers", [])# Transformar la temporada
    
    data = {k: [] for k in ['market_value', 'transfer_date']}
    
    # df_transfers = pd.DataFrame(transfers)
    
    for transfer in transfers:
        # transfer = transfers[1]
        transfer_season = transfer.get("season")
        from_club = transfer.get("from", {}).get("href", "").split("/")[-3]
        to_club = transfer.get("to", {}).get("href", "").split("/")[-3]
        fee = valueText_to_int(transfer.get("fee"))
        
        # print(transfer_fee, fee)

        # if DEBUG:
        #     print(season, transfer_season)
        #     print(club_from_code, from_club)
        #     print(club_to_code, to_club)
        #     print(market_value)

        if transfer_season == season and from_club == club_from_code and\
            to_club == club_to_code and transfer_fee == fee:

            market_value = transfer.get("marketValue")
            date = datetime.strptime(transfer.get("dateUnformatted"), "%Y-%m-%d").date()

            data['market_value'].append(valueText_to_int(market_value))
            data['transfer_date'].append(date)

    if len(data['market_value']) == 1:
        data['market_value'] = data['market_value'][0]
        data['transfer_date'] = data['transfer_date'][0]

    elif len(data['market_value']) > 1:
        data['market_value'] = -1
        data['transfer_date'] = -1

    else:
        data['market_value'] = np.nan
        data['transfer_date'] = np.nan

    return data

def obtener_caracteristicas(player_url):
    # player_url = '/francisco-fydriszewski/profil/spieler/337921'
    url = f"{url_base}{player_url}"
    response = make_request(url, headers=request_headers)
    # response.raise_for_status()  # Asegurarse de que la solicitud fue exitosa

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extraer altura y convertir a cm
    altura_elem = soup.find('span', text='Altura:')
    altura_text = altura_elem.find_next('span', class_='info-table__content--bold').text.strip() if altura_elem else None
    try:
        altura = int(float(altura_text.replace('m', '').replace(',', '.').strip()) * 100) if altura_text else None
    except ValueError:
        altura = None

    # Extraer posición
    posicion_elem = soup.find('span', text='Posición:')
    posicion_text = posicion_elem.find_next('span', class_='info-table__content--bold').text.strip() if posicion_elem else None
    if posicion_text:
        if '-' in posicion_text:
            posicion, demarcacion = posicion_text.split(' - ')
        else:
            posicion, demarcacion = posicion_text, 'Portero'
    else:
        posicion, demarcacion = None, None

    # Extraer posición secundaria en detalle
    posicion_secundaria_elem = soup.find('dt', text='Posición secundaria:')
    dem2 = posicion_secundaria_elem.find_next('dd').text.strip() if posicion_secundaria_elem else 'No tiene'

    # Extraer pie
    pie_elem = soup.find('span', text='Pie:')
    pie = pie_elem.find_next('span', class_='info-table__content--bold').text.strip() if pie_elem else 'Desconocido'

    # Extraer agente
    agente_elem = soup.find('span', text='Agente:')
    if agente_elem:
        # Buscar el siguiente span sin especificar las clases
        agente_container = agente_elem.find_next('span')
        if agente_container:
            # Verificar si el agente está en un <a> o en un <span>
            agente_span = agente_container.find('span', class_='cp')
            if agente_span:
                # Extraer el valor del atributo 'title' (nombre completo del agente)
                agente = agente_span.get('title').strip() if agente_span.get('title') else agente_span.text.strip()
            else:
                agente = agente_container.text.strip()  # Caso general si no hay un <span>
        else:
            agente = None
    else:
        agente = None

    if agente == 'Sin agente':
        agente = None

    return {
        'altura': altura,
        'posicion': posicion,
        'demarcacion': demarcacion,
        'demarcacion_sec': dem2,
        'pie': pie,
        'agente': agente
    }

def obtener_valor(data_entry, DEBUG=False):
    """
    Función auxiliar para obtener el valor de mercado y la fecha de traspaso.
    """
    player_name = data_entry['player_name']
    try:
        if DEBUG:
            print(f'... ... Revisando el valor de mercado de {player_name}')
        info_valor = obtener_valor_transferencia(data_entry, DEBUG=False)
        return info_valor
    except Exception as e:
        if DEBUG:
            print(f'Error obteniendo valor de mercado de {player_name}: {e}')
        return {}

def obtener_datos_jugador(data_entry, incluir_caracteristicas, incluir_rendimiento,
                          rendimiento_actual, longseas, DEBUG=False):
    """
    Función auxiliar que maneja la obtención de características y rendimiento.
    """
    player_name = data_entry['player_name']
    caracteristicas = {}
    rendimiento = {}

    if DEBUG:
        print(f'... ... Revisando las características y rendimiento de {player_name}')

    # Obtener características del jugador si está habilitado
    if incluir_caracteristicas:
        try:
            caracteristicas = obtener_caracteristicas(data_entry['player_url'])
        except Exception as e:
            if DEBUG:
                print(f'Error obteniendo características de {player_name}: {e}')

    # Obtener estadísticas de rendimiento si está habilitado
    if incluir_rendimiento:
        try:
            stats_ant, stats_act = get_last_stats(data_entry, filtrar=not longseas)
            rendimiento['stats_ant'] = stats_ant
            if rendimiento_actual:
                rendimiento['stats_act'] = stats_act
        except Exception as e:
            if DEBUG:
                print(f'Error obteniendo rendimiento de {player_name}: {e}')

    return caracteristicas, rendimiento

def leer_transferidos(season, seas_w='ambas', prestamos='sin', internos=False,
                      desafectados=False, precio_desc=False,
                      incluir_valor=False, incluir_caracteristicas=False,
                      incluir_rendimiento=False, rendimiento_actual=False, longseas=False,
                      incluir_info_club=False, incluir_contract_expiration=False,
                      liga='Argentina',
                      DEBUG=True):
    # Eliminado parámetro max_workers ya que no usaremos multiprocessing
    #longseas no esta programado
    '''
    

    Parameters
    ----------
    season : TYPE
        DESCRIPTION.
    seas_w : TYPE, optional
        DESCRIPTION. The default is 'ambas'.
    prestamos : TYPE, optional
        DESCRIPTION. The default is 'sin'.
    internos : TYPE, optional
        DESCRIPTION. The default is False.
    desafectados : TYPE, optional
        DESCRIPTION. The default is False.
    precio_desc : TYPE, optional
        DESCRIPTION. The default is False.
    incluir_valor : TYPE, optional
        DESCRIPTION. The default is False.
    incluir_caracteristicas : TYPE, optional
        DESCRIPTION. The default is False.
    incluir_rendimiento : TYPE, optional
        DESCRIPTION. The default is False.
    rendimiento_actual : TYPE, optional
        DESCRIPTION. The default is False.
    longseas : TYPE, optional
        DESCRIPTION. The default is False.
    incluir_info_club : TYPE, optional
        DESCRIPTION. The default is False.
    liga : TYPE, optional
        DESCRIPTION. The default is 'Argentina'.
    DEBUG : TYPE, optional
        DESCRIPTION. The default is True.

    Raises
    ------
    ValueError
        DESCRIPTION.

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    '''

    if (incluir_rendimiento or rendimiento_actual) and not incluir_valor:
        incluir_valor = True
        if DEBUG:
            print('Sea ha configurado incluir_valor=True, ya que no se puede analizar el rendimiento sin la fecha de transferencia')
        # raise ValueError(f'No se puede ejecutar incluir_rendimiento sin incluir_valor')

    url = obtener_url(season, seas_w, prestamos, internos, pais=liga)
    response = make_request(url, headers=request_headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    print(url)

    if DEBUG:
        print(f"Revisando la temporada {season} de la liga de {liga}, durante el periodo de {seas_w}")

    # Obtener la lista de clubes argentinos de primera división
    club_boxes = soup.select("div.wappenleiste-box span.wappenleiste a img")
    club_names = [club.get("title") for club in club_boxes]

    # Listas para almacenar los datos extraídos
    data = []

    # Encontrar todos los contenedores de clubes
    club_boxes = soup.select("div.box h2.content-box-headline")
    i = 0  # Contador de jugadores procesados

    # Procesar cada club y tabla de transferencias
    for club_box in club_boxes:
        # Extraer el nombre y código del club
        club_name_element = club_box.find("a")
        if not club_name_element:
            continue
        club_name = club_name_element.get("title")
        club_code = club_name_element.get("href").split("/")[-3].split("?")[0]

        if DEBUG:
            print(f'Revisando {club_name}')

        # Extraer tablas de transferencias (Altas y Bajas)
        club_container = club_box.find_parent("div", class_="box")
        tables = club_container.select("div.responsive-table table")

        for table in tables:
            rows = table.find_all("tr")
            if len(rows) == 1 and "sin fichajes" in rows[0].text.lower():
                continue

            # Determinar si es tabla de Altas o Bajas
            is_alta = "Altas" in rows[0].text

            # Procesar cada jugador de la tabla
            for row in rows[1:]:  # Saltar encabezado
                cols = row.find_all("td")
                player_link = cols[0].find("a")
                if player_link is None:
                    continue

                player_name = player_link.get("title")
                player_url = player_link.get("href")
                player_id = player_url.split("/")[-1]
                age = cols[1].text.strip()
                nationality = [img.get("title") for img in cols[2].find_all("img")]
                position = cols[3].text.strip()

                # Información de comunitario y extranjero
                comunitario = int(any(n in lista_comunitarios for n in nationality))
                extranjero = 0 if 'Argentina' in nationality else 1

                # Si es Alta, obtener el club de origen
                if is_alta:
                    club_element = cols[7].find("a")
                    club_to = club_name
                    code_to = club_code
                    club_nationality_to = liga
                    club_from = club_element.get("title") if club_element else None
                    try:
                        code_from = club_element.get("href").split("/")[-3].split("?")[0]
                        # Buscar la imagen con la bandera para la nacionalidad del club de origen
                        club_nationality_img = cols[7].find("img")
                        club_nationality_from = club_nationality_img.get("title") if club_nationality_img else None
                    except AttributeError:
                        if DEBUG:
                            print(f'... ... El jugador {player_name} no tiene club de origen')
                        continue

                # Si es Baja, obtener el club de destino
                else:
                    club_element = cols[7].find("a")
                    club_from = club_name
                    code_from = club_code
                    club_nationality_from = liga
                    club_to = club_element.get("title") if club_element else None
                    try:
                        code_to = club_element.get("href").split("/")[-3].split("?")[0]
                        # Buscar la imagen con la bandera para la nacionalidad del club de destino
                        club_nationality_img = cols[7].find("img")
                        club_nationality_to = club_nationality_img.get("title") if club_nationality_img else None
                    except AttributeError:
                        if DEBUG:
                            print(f'... ... El jugador {player_name} no tiene club de destino')
                        continue

                    # Evitar duplicado si el club destino está en la lista de clubes
                    if club_to in club_names:
                        if DEBUG:
                            print(f'... ... El jugador {player_name} se analizará como alta')
                        continue  # Omitir duplicado porque se tratará en la tabla de "Altas"

                # Obtener datos de transferencias
                transfer_fee_text = cols[8].text.strip()
                if transfer_fee_text == "Libre":
                    transfer_fee = 0
                elif transfer_fee_text == "-":
                    if desafectados:
                        transfer_fee = np.nan
                    else:
                        continue
                elif transfer_fee_text == "?":
                    if precio_desc:
                        transfer_fee = np.nan
                    else:
                        continue
                else:
                    transfer_fee = valueText_to_int(transfer_fee_text)

                # Armar entrada de datos
                data_entry = {
                    "player_name": player_name,
                    "player_url": player_url,
                    "player_id": player_id,
                    "age": age,
                    "nationality": nationality,
                    "comunitario": comunitario,
                    "extranjero": extranjero,
                    "position": position,
                    "transfer_fee": transfer_fee,
                    "season": season,
                    "season_part": seas_w,
                    "code_from": code_from,
                    "code_to": code_to,
                    "club_from": club_from,
                    "club_to": club_to,
                    "club_nationality_from": club_nationality_from,
                    "club_nationality_to": club_nationality_to
                }

                data.append(data_entry)
                i += 1

    # Procesamiento secuencial en lugar de paralelo
    for data_entry in data:
        # Obtener valor si está habilitado
        if incluir_valor:
            try:
                valor = obtener_valor(data_entry, DEBUG)
                data_entry.update(valor)
            except Exception as exc:
                if DEBUG:
                    print(f'Error obteniendo valor para {data_entry["player_name"]}: {exc}')
        
        # Obtener características y rendimiento
        try:
            caracteristicas, rendimiento = obtener_datos_jugador(
                data_entry, 
                incluir_caracteristicas, 
                incluir_rendimiento, 
                rendimiento_actual, 
                longseas,
                DEBUG
            )
            data_entry.update(caracteristicas)
            data_entry.update(rendimiento.get('stats_ant', {}))
            if rendimiento_actual:
                data_entry.update(rendimiento.get('stats_act', {}))
        except Exception as exc:
            if DEBUG:
                print(f'Error obteniendo datos para {data_entry["player_name"]}: {exc}')

    # Agregar la información del club si está habilitado
    df = pd.DataFrame(data)
    df['prev_season'] = df['season'] - 1
    club_sets = [('code_from', 'club_from', 'prev_season', '_from'),
                 ('code_to', 'club_to', 'prev_season', '_to')]

    if incluir_info_club and not df.empty:
        df = add_infoclub_to_df(df, club_sets, DEBUG=DEBUG)

    df.drop(columns='prev_season', inplace=True)

    if incluir_contract_expiration:
        # Formatear transfer_date en el DataFrame
        df['transfer_date'] = pd.to_datetime(df['transfer_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        # Modificar el parámetro multiprocess a False en la llamada a update_contract_date
        df = update_contract_date(df, multiprocess=False)
        df['ends_contract_date'] = pd.to_datetime(df['ends_contract_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    if DEBUG:
        print(df.head(), df.tail())

    return df
