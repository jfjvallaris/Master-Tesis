# -*- coding: utf-8 -*-
"""
Created on Sat Jun 22 22:09:45 2024

@author: Terminal-NTB
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime

from _functions import *


def extraer_datos_tabla(soup, tipos_competencia,
                        detallado=False, incluir_regional=False):
    # Encuentra todas las tablas de rendimiento
    tablas = soup.find_all('table', class_='items')

    # Lista para almacenar los datos
    rows = []

    # Iterar sobre las tablas y extraer datos
    for tabla, tipo_competencia in zip(tablas, tipos_competencia):
        if "liga" in tipo_competencia.lower():
            if detallado:
                competencia = 'Liga'
            else:
                competencia = 'Local'

        elif "copa nacional" in tipo_competencia.lower():
            if detallado:
                competencia = 'Copa'
            else:
                competencia = 'Local'

        elif "regional" in tipo_competencia.lower():
            if detallado:
                if incluir_regional:
                    competencia = 'Regional'
                else:
                    continue
            else:
                competencia = 'Local'

        elif "copa internacional" in tipo_competencia.lower():
            competencia = 'Internacional'

        else:
            print('Alerta du competencia')
            competencia = 'Desconocida'

        for fila in tabla.find('tbody').find_all('tr'):
            celdas = fila.find_all('td')

            row_data = []
            i = 0
            for col in celdas:
                i += 1
                if col.find('a'):
                    if 'zentriert' in col.get('class', []) and "hauptlink" not in col.get('class', []):
                        row_data.append(col.find('a').get('href'))
                        row_data.append(col.get_text(strip=True))
                    else:
                        title = col.find('a').get('title')
                        href = col.find('a').get('href')
                        
                        # print(i, href)
                        if title:
                            row_data.append(title)
                        else:
                            row_data.append(col.get_text(strip=True))
                        if href:
                            i += 1
                            if i == 4:
                                href = href.split("/")[-1]
                            elif i == 6:
                                href = href.split("/")[-3]
                                
                            row_data.append(href)

                else:
                    row_data.append(col.get_text(strip=True))
                if i == 2:
                    row_data[1] = competencia
                elif i > 6:
                    row_data[-1] = value_to_int(row_data[-1])
            rows.append(row_data)

    try:
        headers = ['Season', 'type_competition','Competition', 'code_compet','Club', 'id_club', 'href_stats','pj', 'Goals', 'Assists', 'Own goals', 'Subbed In', 'Subbed out', 'Yellow Cards', 'Double yellow', 'Red Cards', 'Penalty Kicks', 'Minutes per goal', 'Minutes played']
        datos = pd.DataFrame(rows, columns=headers)
    except ValueError:
        headers = ['Season', 'type_competition','Competition', 'code_compet','Club', 'id_club', 'href_stats', 'pj', 'Goals', 'Own goals', 'Subbed In', 'Subbed out', 'Yellow Cards', 'Double yellow', 'Red Cards', 'Goals Conceded', 'Clean Sheets','Minutes played']
        datos = pd.DataFrame(rows, columns=headers)
    datos[datos.columns[7:]] = datos.iloc[:,7:].astype(float)

    datos = datos.dropna(subset = ['pj'])

    return datos


def obtener_tipo_competencia(soup):
    encabezados = soup.find_all('h2', class_='content-box-headline')
    tipos_competencia = [h2.text.strip() for h2 in encabezados]
    return tipos_competencia[1:]


def get_player_played_data(player_name, played_id):
    url = f'https://www.transfermarkt.es/{player_name}/detaillierteleistungsdaten/spieler/{played_id}/plus/1'
    response = make_request(url, headers=request_headers)
    # response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    tipos_competencia = obtener_tipo_competencia(soup)
    datos_totales = extraer_datos_tabla(soup, tipos_competencia)

    df = pd.DataFrame(datos_totales)
    return df


def dfstats_to_dict(df, t='_ant', filtrar=True):
    if df.empty:
        return {}

    # Filtrar datos si es necesario (por ejemplo, excluir ciertas competiciones)
    df = df.drop(df[(df['Season'] == '2020') & (df['Competition'] == 'Copa Argentina')].index)

    # Obtener los valores únicos de 'Club' y 'id_club' para añadirlos al final
    club_playing = df['Club'].unique().tolist()
    club_playing_code = df['id_club'].unique().tolist()

    # Crear una lista de tuplas con (Competition, Season) para cada type_competition
    competition_season_tuples = df[['type_competition', 'Competition', 'Season']].drop_duplicates() \
        .groupby('type_competition', group_keys=False)[['Competition', 'Season']] \
        .apply(lambda x: list(x.itertuples(index=False, name=None))).to_dict()

    # ***Lógica de manejo de la columna 'long_season'***
    if 'long_season' in df.columns:
        # Crear un diccionario que indique si la temporada es larga para cada tipo de competencia
        long_season_dict = df.groupby('type_competition')['long_season'].any().to_dict()
        # Convertir True/False a 1/0 para cada tipo de competencia
        long_season_dict = {f'{comp}_long_season{t}': int(val) for comp, val in long_season_dict.items()}
    else:
        # Si no existe 'long_season', asumir que no hay temporada larga
        long_season_dict = {f'{comp}_long_season{t}': 0 for comp in df['type_competition'].unique()}

    # Ignorar la columna 'long_season' en los cálculos de las estadísticas
    df = df.drop(columns=['long_season'], errors='ignore')

    # Agrupar las estadísticas, ignorando la columna 'Season' ya que se considera una única temporada
    df_grouped = df.groupby(['type_competition']).sum(numeric_only=True).reset_index()

    # Verificar si hay competiciones internacionales y añadir una fila si es necesario
    is_int = 'Internacional' in df_grouped['type_competition'].unique()
    
    if not is_int:
        # Añadir una fila para 'Internacional' con valores en 0
        new_row = pd.DataFrame({
            'type_competition': ['Internacional']
        })
        # Rellenar con ceros las columnas numéricas y añadir la columna long_season = 0
        for col in df_grouped.columns:
            if col not in ['type_competition']:
                new_row[col] = 0
        # Asegurarse de que la columna long_season tenga un valor de 0
        new_row['long_season'] = 0
        df_grouped = pd.concat([df_grouped, new_row], ignore_index=True)

    # Pivotar el DataFrame para organizar por 'type_competition', sin separar por 'Season'
    df_pivoted = df_grouped.pivot(columns='type_competition', values=df_grouped.columns[1:])
    
    # Rellenar valores NaN con 0
    df_pivoted = df_pivoted.fillna(0)

    # Aplanar las columnas y poner primero el tipo de competencia, luego la estadística, y el sufijo
    df_pivoted.columns = [f'{col[1]}_{col[0]}{t}' for col in df_pivoted.columns.values]
    df_pivoted = df_pivoted.sum()

    # Convertir el DataFrame a un diccionario
    result_dict = df_pivoted.to_dict()

    # Añadir 'Club_playing' y 'id_club_playing' al diccionario
    result_dict[f'Club_playing{t}'] = club_playing
    result_dict[f'id_club_playing{t}'] = club_playing_code

    # Añadir la lista de tuplas (Competition, Season) al diccionario
    result_dict[f'competition_season_tuples{t}'] = competition_season_tuples

    # Añadir las claves binarias de long_season al diccionario
    result_dict.update(long_season_dict)

    return result_dict


def get_last_stats(dic_player, DEBUG=False, filtrar=True):
    """
    Obtiene estadísticas de la temporada anterior y actual de un jugador,
    con la opción de filtrar o indicar si la temporada es más larga.

    Args:
    - dic_player (dict): Diccionario con los datos del jugador.
    - DEBUG (bool): Si True, imprime información adicional.
    - filtrar (bool): Si True, aplica el filtrado de fechas. Si False, indica si la temporada es más larga.

    Returns:
    - Tuple[dict, dict]: Estadísticas de la temporada anterior y actual.
    """

    player_name = parse_name(dic_player['player_name'])
    player_id = dic_player['player_id']
    season = dic_player['season']  # Para obtener t-2 restar uno aca
    seas_w = dic_player['season_part']
    club_to_code = str(dic_player['code_to'])
    transfer_date = dic_player['transfer_date']

    df = get_player_played_data(player_name, player_id)

    # Comprobar si es portero (18 columnas) o jugador de campo
    is_por = len(df.columns) == 18

    try:
        df = df.drop('Minutes per goal', axis=1)
    except KeyError:
        print('... ... ... Minutes per goal not in columns')

    if DEBUG:
        print(df)

    season_s_ant = str(season - 1)
    season_s_act = str(season)
    season_n_ant = f"{str(season - 1)[-2:]}/{str(season)[-2:]}"
    season_n_act = f"{str(season)[-2:]}/{str(season + 1)[-2:]}"
    sufijo_actual = '_act'

    if seas_w == 'invierno':
        df_sur = df[df['Season'] == season_s_ant]
        df_norte = df[df['Season'] == season_n_ant]
        df_ant = pd.concat([df_sur, df_norte])
        df_act = df[(df.Season == season_n_act) & (df.id_club != club_to_code)]

    elif seas_w == 'verano':
        df_sur = df[df['Season'] == season_s_ant]
        df_norte = df[df['Season'] == season_n_ant]
        df_ant = pd.concat([df_sur, df_norte])
        df_act = df[(df.Season == season_s_act) & (df.id_club != club_to_code)]

    # Temporada anterior
    cant_seas = len(df_ant['Season'].unique())
    if cant_seas > 1:
        print('... ... ... Más de 1 temporada encontrada')
        df_ant = start_before_last(df_ant, season, seas_w, transfer_date, filtrar)

    # Procesar los datos de la temporada anterior
    if cant_seas == 0:
        stats_ant = {}
    else:
        df_ant = df_ant.drop('href_stats', axis=1)
        stats_ant = dfstats_to_dict(df_ant)

    # Temporada actual
    cant_seas = len(df_act['Season'].unique())
    if cant_seas > 1:
        df_act = start_before_last(df_act, season, seas_w, transfer_date, filtrar)

    if cant_seas == 0:
        stats_act = {}
    else:
        df_act = df_act.drop('href_stats', axis=1)
        stats_act = dfstats_to_dict(df_act, t='_act')

    if DEBUG:
        print(stats_ant, stats_act)

    return stats_ant, stats_act


def start_before_last(df, season, seas_w, transfer_date, filtrar=True):
    """
    Filtra las filas del DataFrame según la fecha mínima de las estadísticas detalladas generadas
    o añade columna indicando temporada más larga si no se filtra.

    Args:
    - df (pd.DataFrame): DataFrame que contiene la columna 'href_stats' para obtener estadísticas.
    - season (int): La temporada actual (por ejemplo, 2023).
    - seas_w (str): Parte de la temporada ("verano" o "invierno").
    - transfer_date (datetime): Fecha de la transferencia.
    - filtrar (bool): Si True, filtra según la fecha. Si False, agrega columna indicando temporada más larga.

    Returns:
    - pd.DataFrame: DataFrame filtrado o con columna 'long_season' si no se filtra.
    """
    # Definir la fecha límite según la temporada anterior y la parte de la temporada
    if seas_w == 'verano':
        fecha_limite = datetime(season - 1, 6, 30)  # 30 de junio de la temporada anterior
    elif seas_w == 'invierno':
        fecha_limite = datetime(season - 2, 12, 31)  # 31 de diciembre de la temporada anterior
    else:
        raise ValueError("El valor de 'seas_w' debe ser 'verano' o 'invierno'.")

    df_filtrado = pd.DataFrame()

    for _, row in df.iterrows():
        try:
            df_temp = get_player_detailed_data(row['href_stats'])
            df_temp['fecha'] = pd.to_datetime(df_temp['fecha'], format='%d/%m/%y')
            fecha_min = df_temp['fecha'].min()

            if fecha_min.date() < transfer_date:
                if filtrar:
                    if fecha_min > fecha_limite:
                        df_filtrado = pd.concat([df_filtrado, pd.DataFrame([row])], ignore_index=True)
                else:
                    row['long_season'] = fecha_min <= fecha_limite
                    df_filtrado = pd.concat([df_filtrado, pd.DataFrame([row])], ignore_index=True)
        except Exception as e:
            print(f"Error al procesar {row['href_stats']}: {e}")

    return df_filtrado


def produce_seas_df_stats(df, is_por=False):
    ''' 
    Funcion en construcción. Debe devolver una fila de dataframe similar a
    que se se scrapea en rendimiento por tipo de competencia en tfmkt.

    Se ha detectado un problema en las estadisticas de los porteros en las copas
    al exponer el resultado de la prorroga o de los penales, no se puede 
    establecer si el portero encajo o tuvo valla invicta sin recorrer el partido.
    Se deberá determinar si vale la pena el costo computacional

    '''
    df_stats = pd.DataFrame()

    for index, row in df.iterrows():
        # Obtener el href_stats para esta fila
        href = row['href_stats']
        print(href)

        # Obtener las 6 primeras columnas de la fila actual
        first_6_columns = row[:6]  # Esto selecciona las primeras 6 columnas

        # Llamar a la función para obtener los datos detallados
        df_temp = get_player_detailed_data(href)

        # Añadir las 6 primeras columnas al inicio del DataFrame temporal
        for col_idx, col_name in enumerate(first_6_columns.index):
            df_temp.insert(col_idx, col_name, first_6_columns[col_name])

        # Concatenar los datos temporales al DataFrame final
        df_stats = pd.concat([df_stats, df_temp])

    df_stats['fecha'] = pd.to_datetime(df_stats['fecha'], format='%d/%m/%y')
    df_stats = df_stats.sort_values(by='fecha')
    print(is_por, df_stats)

    return df_stats


def get_player_detailed_data(href_stats):
    url = 'https://www.transfermarkt.es/' + href_stats
    response = make_request(url, headers=request_headers)
    # response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    datos_totales = extraer_datos_tabla_jxj(soup)

    df2 = pd.DataFrame(datos_totales)
    return df2

def extraer_datos_tabla_jxj(soup):

    # Buscar la tabla utilizando la clase 'responsive-table'
    tabla = soup.find_all('div', class_='responsive-table')[1]

    # Verificar si la tabla fue encontrada
    if tabla is None:
        raise Exception("No se encontró la tabla en la página")

    # Inicializar una lista para almacenar los datos
    datos_partidos = []

    # Recorrer las filas del cuerpo de la tabla (tbody)
    for fila in tabla.find('tbody').find_all('tr'):
        celdas = fila.find_all('td')

        row_data = []
        for col in celdas:
            if col.find('a'):
                if 'zentriert' in col.get('class', []) and "hauptlink" not in col.get('class', []):
                    row_data.append(col.get_text(strip=True))
                else:
                    title = col.find('a').get('title')
                    row_data.append(title if title else col.get_text(strip=True))
            else:
                row_data.append(col.get_text(strip=True))

        datos_partidos.append(row_data)

    try:
        headers = ['jornada', 'fecha', 'localidad', 'equipo_local', 'pos_local', '', 
                   'equipo_adversario', 'resultado', 'posicion', 'goles', 'asistencias', 
                   'tarjetas_amarillas', 'segunda_amarilla', 'roja', 'minutos_jugados']
        datos_df = pd.DataFrame(datos_partidos, columns=headers)
    except ValueError:
        headers = ['jornada', 'fecha', 'localidad', 'equipo_local', '', 
                   'equipo_adversario', 'resultado', 'posicion', 'goles', 'asistencias', 
                   'tarjetas_amarillas', 'segunda_amarilla', 'roja', 'minutos_jugados']
        datos_df = pd.DataFrame(datos_partidos, columns=headers)

    return datos_df