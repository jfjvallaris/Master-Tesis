# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 15:09:24 2024

@author: Working-Ntb


"""

import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

from _functions import *

#%%

# Función para cargar la cache desde un archivo CSV
def load_cache_from_csv(cache_filename="club_info.csv"):
    if os.path.exists(cache_filename):
        return pd.read_csv(cache_filename, converters = {'club_code': str})
    else:
        # Si no existe el archivo, retornamos un DataFrame vacío
        return pd.DataFrame(columns=['club_code', 'club_name', 'season', 'division', 'league_position', 
                                     'puntos', 'partidos_jugados', 'goles_favor', 'goles_contra', 
                                     'valor_plantel', 'mean_plantel'])

# Función para guardar la cache a un archivo CSV
def save_cache_to_csv(cache, cache_filename="club_info.csv"):
    cache = cache.sort_values(['nationality', 'club_code', 'season'])
    cache.to_csv(cache_filename, index=False)


def add_infoclub_to_df(df, column_sets, cache_filename="club_info.csv", DEBUG=False, block_size=1000):
    cache_club_info = load_cache_from_csv(cache_filename)
    nuevos_clubes = []

    clubes = pd.DataFrame()

    for codigo_col, nombre_col, temporada_col, sufijo in column_sets:
        clubes_temp = df[[codigo_col, nombre_col, temporada_col]].drop_duplicates(subset=[codigo_col, temporada_col])
        clubes_temp = clubes_temp[clubes_temp[nombre_col] != 'Sin equipo']
        clubes_temp = clubes_temp.rename(columns={codigo_col: 'club_code', nombre_col: 'club_name', temporada_col: 'season'})
        clubes = pd.concat([clubes, clubes_temp], ignore_index=True)

    clubes = clubes.drop_duplicates(subset=['club_code', 'season']).dropna(subset=['club_code'])

    bloques = [clubes.iloc[i:i + block_size] for i in range(0, len(clubes), block_size)]

    for i, bloque in enumerate(bloques):
        if DEBUG:
            print(f"Procesando bloque {i + 1}/{len(bloques)} con {len(bloque)} clubes...")

        clubes_con_cache = bloque.merge(cache_club_info, how='left', on=['club_code', 'season'], suffixes=('', '_'), indicator=True)
        clubes_con_cache = clubes_con_cache.drop('club_name_', axis=1)

        clubes_cacheados = clubes_con_cache[clubes_con_cache['_merge'] == 'both'].drop(columns=['_merge'])
        clubes_para_scraping = clubes_con_cache[clubes_con_cache['_merge'] == 'left_only']
        clubes_para_scraping = clubes_para_scraping.drop_duplicates(subset=['club_code', 'season'])

        if not clubes_para_scraping.empty:
            # Procesamos secuencialmente en lugar de usar threads
            for _, row in clubes_para_scraping.iterrows():
                try:
                    club_info = get_club_info(row['club_name'], row['club_code'], row['season'], DEBUG=DEBUG)
                    if club_info:
                        nuevos_clubes.append({
                            'club_code': row['club_code'], 'club_name': row['club_name'], 'season': row['season'], **club_info
                        })
                except Exception as e:
                    print(f"Error obteniendo datos del club {row['club_name']}: {e}")

            if nuevos_clubes:
                nuevos_clubes_df = pd.DataFrame(nuevos_clubes)
                nuevos_clubes_df = nuevos_clubes_df.drop_duplicates(subset=['club_code', 'season'])
                cache_club_info = pd.concat([cache_club_info, nuevos_clubes_df], ignore_index=True).drop_duplicates(subset=['club_code', 'season'])
                save_cache_to_csv(cache_club_info, cache_filename)
                clubes_cacheados = pd.concat([clubes_cacheados, nuevos_clubes_df], ignore_index=True).drop_duplicates(subset=['club_code', 'season'])

    for codigo_col, nombre_col, temporada_col, sufijo in column_sets:
        clubes_cacheados_sufijo = clubes_cacheados.add_suffix(sufijo)
        df = df.merge(clubes_cacheados_sufijo, how='left', left_on=[codigo_col, temporada_col],
                      right_on=[f"club_code{sufijo}", f"season{sufijo}"])
        df = df.drop(columns=[f"club_code{sufijo}", f"club_name{sufijo}"], errors='ignore')

    if DEBUG:
        scrapeos_evitar = len(clubes_cacheados) - len(nuevos_clubes)
        print(f"Scrapeos evitados: {scrapeos_evitar}")

    return df


def get_club_info(name_club, code_club, season, sufijo="", DEBUG=False):
    if DEBUG:
        print(f'Revisando {code_club}.{name_club} en la temporada {season}')

    name_club = parse_club_name(name_club)
    info_club = {}

    # Ejecutar las funciones secuencialmente en lugar de con threads
    try:
        valor_result = get_club_value(name_club, code_club, season)
        info_club.update(valor_result)
    except Exception as e:
        print(f"Error al obtener valor_plantilla de {name_club}: {e}")

    try:
        posicion_result = get_club_position(name_club, code_club, season)
        info_club.update(posicion_result)
    except Exception as e:
        print(f"Error al obtener posicion_liga de {name_club}: {e}")

    # Actualizar las claves de los diccionarios con el sufijo
    info_club = {f"{key}{sufijo}": value for key, value in info_club.items()}

    return info_club


def get_club_value(name_club, code_club, season):
    # name_club, code_club, season = ('a', '209', 2023)
    url = f'https://www.transfermarkt.es/{name_club}/kader/verein/{code_club}/saison_id/{season}'
    response = requests.get(url, headers=request_headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Corregir temporada del hemisferio sur
    code_season = soup.find('title').text.strip().split('|')[0].split(' ')[-2]
    try:
        int(code_season)
        season -=1
        url = f'https://www.transfermarkt.es/{name_club}/kader/verein/{code_club}/saison_id/{season}'
        response = requests.get(url, headers=request_headers)
        soup = BeautifulSoup(response.content, 'html.parser')

    except ValueError:
        pass

    print(url)

    # Localizar la tabla en la estructura HTML usando el <tfoot>
    table_footer = soup.find('tfoot')
    if table_footer:
        # Extraer las celdas relevantes (valor total y valor medio)
        total_value_cell = table_footer.find_all('td', class_='rechts')[1]  # Valor total
        average_value_cell = table_footer.find_all('td', class_='rechts')[2]  # Valor promedio

        # Obtener los valores como texto
        total_value_text = total_value_cell.get_text(strip=True)
        average_value_text = average_value_cell.get_text(strip=True)

        # Convertir los valores a números usando la función valueText_to_int
        total_value = valueText_to_int(total_value_text)
        average_value = valueText_to_int(average_value_text)

        return {
            'valor_plantel': total_value,
            'mean_plantel': average_value
        }
    else:
        print(f"No se encontró la tabla de valores de {name_club}")
        return {}

def get_argentina_second_division_data(code_club, DEBUG=False):
    """
    Obtiene la información de la Segunda División Argentina (Primera Nacional) directamente 
    desde la tabla completa de la temporada.
    """

    url = f"https://www.transfermarkt.es/primera-nacional/jahrestabelle/wettbewerb/ARG2/saison_id/2021"
    response = requests.get(url, headers=request_headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    if DEBUG:
        print(f"Accediendo a datos de la Segunda División Argentina: {url}")

    # Inicializar el diccionario con valores por defecto
    posicion_dict = {
        "division": "Segunda categoría",
        "league_position": None,
        "puntos": None,
        "partidos_jugados": None,
        "partidos_ganados": None,
        "partidos_empatados": None,
        "partidos_perdidos": None,
        "goles_favor": None,
        "goles_contra": None
    }

    # Buscar la tabla dentro de la estructura correcta
    table = soup.select_one("#tm-main > div.row > div.large-8.columns > div > table")
    if not table:
        if DEBUG:
            print("⚠️ No se encontró la tabla de posiciones.")
        return posicion_dict  # Devolver con valores vacíos si no se encuentra la tabla

    tbody = table.find("tbody")
    if not tbody:
        if DEBUG:
            print("⚠️ No se encontró el cuerpo de la tabla.")
        return posicion_dict

    # Iterar sobre las filas para buscar el club correcto
    for row in tbody.find_all("tr"):
        cols = row.find_all("td")

        # Extraer el ID del club desde el enlace
        club_link = cols[2].find("a")["href"] if cols[2].find("a") else ""
        club_id = int(club_link.split('/')[-3])

        if club_id != code_club:
            continue  # No es el club que buscamos

        # Extraer los datos
        posicion_dict["league_position"] = int(cols[0].text.strip())  # Posición en la liga
        posicion_dict["partidos_jugados"] = int(cols[3].text.strip())
        posicion_dict["partidos_ganados"] = int(cols[4].text.strip())
        posicion_dict["partidos_empatados"] = int(cols[5].text.strip())
        posicion_dict["partidos_perdidos"] = int(cols[6].text.strip())

        # Extraer goles
        goles_favor, goles_contra = map(int, cols[7].text.strip().split(":"))
        posicion_dict["goles_favor"] = goles_favor
        posicion_dict["goles_contra"] = goles_contra

        posicion_dict["puntos"] = int(cols[9].text.strip())  # Puntos obtenidos

        if DEBUG:
            print(f"Datos extraídos: {posicion_dict}")

        break  # No es necesario seguir iterando

    if posicion_dict["league_position"] == float('inf'):
        posicion_dict["league_position"] = None

    return posicion_dict


def get_club_position(name_club, code_club, season, DEBUG=False):
    """
    Obtiene la posición en la liga y la nacionalidad de un club para una temporada específica.
    Si el club pertenece a la Segunda División Argentina y aparece en el enlace específico,
    obtiene la información directamente desde la tabla de la temporada.
    """

    # URL de la tabla de posiciones estándar
    url = f'https://www.transfermarkt.es/{name_club}/platzierungen/verein/{code_club}/'
    response = requests.get(url, headers=request_headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    season_formatted = f"{str(season)[-2:]}/{str(season+1)[-2:]}" 

    if DEBUG:
        print(url, season_formatted)

    posicion_dict = {
        "division": None,
        "league_position": float('inf'),
        "puntos": 0,
        "partidos_jugados": 0,
        "partidos_ganados": 0,
        "partidos_empatados": 0,
        "partidos_perdidos": 0,
        "goles_favor": 0,
        "goles_contra": 0,
        "nationality": 'Argentina'
    }

    division_ranking = {
        "Primera categoría": 3,
        "Segunda categoría": 2,
        "Tercera categoría": 1
    }

    tablas = soup.find_all('table')

    if not tablas:
        posicion_dict["league_position"] = None
        posicion_dict["division"] = "Desconocida"
        return posicion_dict

    for tabla in tablas:
        tbody = tabla.find('tbody')
        if tbody is None:
            continue  

        for row in tbody.find_all('tr'):
            season_td = row.find('td', class_='zentriert')
            if season_td and season_td.text.strip() == season_formatted:
                cols = row.find_all('td')
                competition_link = cols[2].find("a")["href"] if cols[2].find("a") else ""

                # Detectar si el torneo pertenece a la Segunda División Argentina con el enlace específico
                if "/primera-nacional/startseite/wettbewerb/ARG2/saison_id/2020" in competition_link:
                    if DEBUG:
                        print("📢 Detectado torneo de Segunda División Argentina de 2021. Obteniendo datos desde la tabla específica.")
                        return get_argentina_second_division_data(code_club, DEBUG)

                else:
                    division = cols[3].text.strip()
                    goles = cols[7].text.strip()
                    puntos = int(cols[9].text.strip())
                    posicion = int(cols[10].text.strip())
        
                    partidos_ganados = int(cols[4].text.strip())
                    partidos_empatados = int(cols[5].text.strip())
                    partidos_perdidos = int(cols[6].text.strip())
        
                    partidos_jugados = partidos_ganados + partidos_empatados + partidos_perdidos
        
                    goles_favor, goles_contra = map(int, goles.split(':'))
        
                    posicion_dict["puntos"] += puntos
                    posicion_dict["partidos_jugados"] += partidos_jugados
                    posicion_dict["partidos_ganados"] += partidos_ganados
                    posicion_dict["partidos_empatados"] += partidos_empatados
                    posicion_dict["partidos_perdidos"] += partidos_perdidos
                    posicion_dict["goles_favor"] += goles_favor
                    posicion_dict["goles_contra"] += goles_contra
        
                    posicion_dict["league_position"] = min(posicion_dict["league_position"], posicion)
        
                    if division in division_ranking:
                        if (posicion_dict["division"] is None or 
                            division_ranking[division] > division_ranking.get(posicion_dict["division"], 0)):
                            posicion_dict["division"] = division

    if posicion_dict["league_position"] == float('inf'):
        posicion_dict["league_position"] = None

    # 1. Extraer la nacionalidad del club desde el atributo "title" de la imagen de la bandera
    try:
        nacionalidad_element = soup.select_one('header div span span a img[title]')
        nacionalidad = nacionalidad_element['title'] if nacionalidad_element else None
        if DEBUG:
            print(f"Nacionalidad del club: {nacionalidad}")
        posicion_dict['nationality'] = nacionalidad
    except Exception as e:
        if DEBUG:
            print(f"Error al extraer la nacionalidad: {e}")
        posicion_dict['nationality'] = None


    return posicion_dict
