# -*- coding: utf-8 -*-
"""
Created on Wed Jan  1 20:34:14 2025

@author: Terminal-NTB
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup

import time
from waybackpy import WaybackMachineCDXServerAPI
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from _functions import make_request

import os


def get_closest_archive(url, transfer_date, max_retries=5, base_delay=5, 
                        max_delay=30, max_age=None):
    """
    Consulta la API de Wayback Machine para obtener el snapshot más cercano antes de transfer_date.
    Maneja errores de conexión con reintentos y un tiempo de espera creciente.
    Si se especifica max_age, filtra los snapshots que excedan la antigüedad máxima.
    Si max_retries es None, usa un bucle infinito con incrementos en el tiempo de espera.

    :param url: URL a consultar.
    :param transfer_date: Fecha de transferencia como límite superior para los snapshots.
    :param max_retries: Número máximo de intentos en caso de error, o None para intentar indefinidamente.
    :param base_delay: Tiempo de espera inicial entre reintentos.
    :param max_age: Antigüedad máxima permitida para el snapshot en días. None para no aplicar restricción.
    :return: Diccionario con información del snapshot más cercano.
    """
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    end_timestamp = pd.to_datetime(transfer_date).strftime("%Y%m%d")

    # Calcular start_timestamp si max_age está definido
    start_timestamp = None
    if max_age is not None:
        start_timestamp = (pd.to_datetime(transfer_date) - pd.Timedelta(days=max_age)).strftime("%Y%m%d")

    delay = base_delay
    attempt = 1

    while max_retries is None or attempt <= max_retries:
        try:
            print(f"Intento {attempt} para {url}...")
            cdx = WaybackMachineCDXServerAPI(url, user_agent, end_timestamp=end_timestamp, start_timestamp=start_timestamp)
            snapshots = list(cdx.snapshots())

            if snapshots:
                # Filtrar snapshots para asegurarse de que estén dentro del rango permitido
                transfer_date_obj = pd.to_datetime(transfer_date)
                snapshots = [s for s in snapshots if pd.to_datetime(s.timestamp) <= transfer_date_obj]

                if snapshots:
                    last_snapshot = snapshots[-1]
                    return {
                        "available": True,
                        "url": last_snapshot.archive_url,
                        "timestamp": last_snapshot.timestamp
                    }

            return {"available": False, "url": None, "timestamp": None}

        except requests.exceptions.RequestException as e:
            print(f"Error al consultar Wayback Machine en intento {attempt}: {e}")

            # Si no hay límite de intentos, usar un bucle infinito con incremento de espera
            retry_delay = min(delay, max_delay)  # Límite superior de 15 segundos entre intentos
            print(f"Esperando {retry_delay} segundos antes del próximo intento...")
            time.sleep(retry_delay)
            delay += base_delay

            if max_retries is not None:
                attempt += 1

    print(f"Error crítico después de {max_retries} intentos.")
    return {"available": False, "url": None, "timestamp": None}


def normalize_date(date_text):
    """
    Normaliza una fecha en diferentes formatos al formato "dd/mm/yyyy".
    
    :param date_text: Texto de la fecha extraída.
    :return: Fecha normalizada en formato "dd/mm/yyyy" o el texto original si no se puede convertir.
    """
    try:
        # Intentar convertir el texto al formato conocido
        return datetime.strptime(date_text, "%d/%m/%Y").strftime("%d/%m/%Y")
    except ValueError:
        pass  # Continuar si falla el primer intento
        
    try:
        # Intentar convertir el texto al formato conocido
        return datetime.strptime(date_text, "%b %d, %Y").strftime("%d/%m/%Y")
    except ValueError:
        pass  # Continuar si falla el primer intento

    try:
        # Formato con puntos (e.g., "31.12.2021")
        return datetime.strptime(date_text, "%d.%m.%Y").strftime("%d/%m/%Y")
    except ValueError:
        pass

    # Si no se puede convertir, retornar el texto original
    print(f"Formato de fecha desconocido: {date_text}")
    return date_text if date_text.strip()=='-' else None


def extract_contract_date(soup, code_from, transfer_date, archive_date):
    """
    Extrae la fecha de vencimiento del contrato y valida el código del club propietario.
    
    :param soup: Objeto BeautifulSoup del contenido HTML de la página.
    :param code_from: Código del club vendedor desde el DataFrame.
    :return: Fecha del contrato si es válida, o None si no se encuentra o no pasa la validación.
    """
    try:
        # Detectar idioma
        html_tag = soup.find("html")
        lang = html_tag.get("lang", "es")  # Idioma por defecto: español
        print(f"Idioma detectado: {lang}")

        # Definir palabras clave por idioma
        keywords = {
            "es": {"contract": "Contrato hasta:", "loan_contract": "Contrato allí hasta:", "current": "Club actual:", "loan_from": "Prestado de:"},
            "en": {"contract": "Contract expires:", "loan_contract": "Contract there expires:", "current": "Current club:", "loan_from": "On loan from:"},
            "de": {"contract": "Vertrag bis:", "loan_contract": "Vertrag dort bis:", "current": "Aktueller Verein:", "loan_from": "Ausgeliehen von:"}
        }
        keyword = keywords.get(lang, keywords["es"])  # Usar español por defecto si el idioma no es soportado

        def extract_club_id(key, archive_date):
            """
            Extrae el ID del club actual del HTML, manejando estructuras modernas y alternativas.
            
            :param soup: Objeto BeautifulSoup del HTML de la página.
            :param key: Palabra clave opcional para buscar en estructuras alternativas.
            :return: ID del club como entero, o None si no se encuentra.
            """
            print(f"Buscando: {key}")
            try:
                # El condicional
                fecha_limite = pd.to_datetime('2021-09-01')
                if pd.to_datetime(archive_date) > fecha_limite:
                    # código para fechas posteriores al 1 de septiembre 2021
                    print(f"new version ({archive_date})")
                    info_table = soup.find("div", class_="info-table info-table--right-space")
                    if info_table:
                        info_club = info_table.find("span", class_="info-table__content info-table__content--regular",
                                                    string=lambda text: text and key in text.strip())
                        if info_club:
                            info_club = info_club.find_next_sibling("span")

                            # Intentando con los links con title
                            link = info_club.find("a", href=True, title=True)
                            # Si no tiene club, devolver 515
                            if link and "verein" in link["href"]:  # Verificar que el enlace contiene "verein"
                                if "without-club" in link["href"]:
                                    return 515
                                try:
                                    return int(link["href"].split("/")[-1])  # Extraer el ID desde la URL
                                except ValueError:
                                    pass  # Continuar si no se puede convertir a entero

                            # Intentando recorriendo los links
                            links = info_club.find_all("a", href=True)
                            for link in links:
                                try:
                                    print(int(link["href"].split("/")[-1]))  # Extraer el ID desde la URL
                                except ValueError:
                                    pass  # Continuar si no se puede convertir a entero

                else:
                    # código para fechas anteriores o iguales al 1 de septiembre 2021
                    print(f"old version ({archive_date})")
                    # Intento 3: Estructuras antiguas en tablas
                    if key:
                        table = soup.find("table", class_="auflistung")
                        if table:
                            row = table.find("th", string=lambda x: x and key in x)
                            if row:
                                link = row.find_next("a", id=True)
                                if link and link.get("id"):
                                    try:
                                        return int(link["id"])  # Extraer el ID desde el atributo 'id'
                                    except ValueError:
                                        pass  # Continuar si no se puede convertir a entero
        
                # Si no se encuentra el ID, retornar None
                print("No se encontró el ID del club en las estructuras disponibles.")
                return None
        
            except Exception as e:
                print(f"Error en extract_club_id: {e}")
                return None

        # Verificar si el jugador está cedido
        loan_club_id = extract_club_id(keyword["loan_from"], archive_date)
        owning_club_id = extract_club_id(keyword["current"], archive_date) if not loan_club_id else loan_club_id


        # Verificar condiciones
        if owning_club_id == 515 and (pd.to_datetime(transfer_date) - pd.to_datetime(archive_date)).days < 60:
            print(f"Se cumple la condición de jugador libre.")
            transfer_date = pd.to_datetime(transfer_date)
            archive_date = pd.to_datetime(archive_date)
            if transfer_date.month in [7, 8, 9]:  # Julio, Agosto, Septiembre
                contract_date = transfer_date.replace(month=6, day=30)  # Último día de junio del mismo año
            elif transfer_date.month in [1, 2]:  # Enero, Febrero
                contract_date = transfer_date.replace(year=transfer_date.year - 1, month=12, day=31)  # Último día de diciembre del año anterior
            else:
                # Calcular el último día del mes anterior
                first_day_of_month = transfer_date.replace(day=1)  # Primer día del mes de transfer_date
                last_day_prev_month = first_day_of_month - timedelta(days=1)  # Día previo al primer día del mes
                contract_date = last_day_prev_month
            print(f"Fecha del contrato: {contract_date}")
            return contract_date.strftime("%d/%m/%Y")

        if owning_club_id != code_from:
            print(f"El código del club propietario ({owning_club_id}) no coincide con el código del DataFrame ({code_from}).")

            return None

        # Extraer fecha del contrato
        def extract_date(key, archive_date):
            print(f"Buscando: {key}")
            fecha_limite = pd.to_datetime('2021-09-01')
            if pd.to_datetime(archive_date) > fecha_limite:
                # Nueva estructura
                print(f"new version ({archive_date})")
                info_table = soup.find("div", class_="info-table info-table--right-space")
                if info_table:
                    info_contract =  info_table.find("span", class_="info-table__content info-table__content--regular",
                                                     string=key)
                    if info_contract:
                        info_contract = info_contract.find_next_sibling("span")
                        if info_contract:
                            return normalize_date(info_contract.text.strip())
            else:
                # Antigua estructura
                print(f"old version ({archive_date})")
                table = soup.find("table", class_="auflistung")
                if table:
                    row = table.find("th", string=lambda x: x and key in x)
                    if row:
                        content = row.find_next("td")
                        if content:
                            return normalize_date(content.text.strip())
            return None

        # Buscar la fecha del contrato dependiendo de si es cesión o no
        contract_date = extract_date(keyword["loan_contract"] if loan_club_id else keyword["contract"], archive_date)

        if contract_date:
            return contract_date


        print("No se encontró una fecha válida del contrato.")
        return None

    except Exception as e:
        print(f"Error al extraer la fecha del contrato: {e}")
        return None



def scrape_transfermarkt_archive(player_url, transfer_date, code_from, paralel_mirrors=False, aditional_mirrors=False):
    """
    Busca la página archivada más cercana antes de transfer_date en los dominios de Transfermarkt
    y procesa el snapshot más cercano con validación del código del club propietario.
    """
    mirrors = ['es', 
                'com.ar', 'com', 'de'
               ]
    if aditional_mirrors:
        mirrors += ['us', 'co.uk', 'co', 'pe', 'mx']

    print(f"Fecha de transferencia: {transfer_date}")
    timestamp = pd.to_datetime(transfer_date).strftime("%Y%m%d")
    print(f"Timestamp generado: {timestamp}")

    closest_snapshots = []

    def process_mirror(mirror):
        url = f"https://www.transfermarkt.{mirror}{player_url}"
        print(f"Llamando a Wayback Machine para URL: {url}")
        try:
            return get_closest_archive(url, transfer_date, max_retries=5), mirror
        except requests.exceptions.RequestException as e:
            print(f"Error al consultar el mirror {mirror}: {e}")
            return None, mirror

    if paralel_mirrors:
        with ThreadPoolExecutor(max_workers=len(mirrors)) as executor:
            future_to_mirror = {executor.submit(process_mirror, mirror): mirror for mirror in mirrors}
            for future in as_completed(future_to_mirror):
                try:
                    result, mirror = future.result()
                    if result and result["available"]:
                        closest_snapshots.append({
                            "url": result["url"],
                            "timestamp": result["timestamp"],
                            "mirror": mirror
                        })
                except Exception as e:
                    print(f"Error procesando el mirror {future_to_mirror[future]}: {e}")
    else:
        for mirror in mirrors:
            try:
                time.sleep(30)  # Respetar el tiempo entre solicitudes
                archive_data, mirror = process_mirror(mirror)
                if archive_data and archive_data["available"]:
                    closest_snapshots.append({
                        "url": archive_data["url"],
                        "timestamp": archive_data["timestamp"],
                        "mirror": mirror
                    })
            except Exception as e:
                print(f"Error procesando el mirror {mirror}: {e}")

    closest_snapshots.sort(key=lambda x: x["timestamp"], reverse=True)

    for snapshot in closest_snapshots:
        archived_url = snapshot["url"]
        archive_date = snapshot["timestamp"]
        mirror_used = snapshot["mirror"]

        print(f"Intentando con snapshot: {archived_url} (Fecha: {archive_date}, Mirror: {mirror_used})")
        try:
            response = make_request(archived_url, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                contract_date = extract_contract_date(soup, code_from, transfer_date, archive_date)
                if contract_date:
                    archive_date_formatted = datetime.strptime(archive_date[:8], "%Y%m%d").strftime("%d/%m/%Y")
                    print(f"Fecha de contrato válida encontrada: {contract_date}")
                    return contract_date, archive_date_formatted, archived_url
                else:
                    print(f"No se encontró una fecha de contrato válida en el snapshot de {mirror_used}.")
            else:
                print(f"Error al acceder al snapshot de {mirror_used}: Estado HTTP {response.status_code}.")
        except requests.exceptions.RequestException as e:
            print(f"Error al descargar el snapshot {archived_url}: {e}")

    print(f"No se encontraron snapshots válidos para {player_url}.")
    return None, None, None


def update_contract_dates(df):
    """
    Actualiza el DataFrame agregando información de 'ends_contract_date' y 'archive_date'
    mediante el scraping en las filas donde falta esta información y validando con el código del club propietario.
    Agrega una columna 'processing_status' para indicar el estado del procesamiento de cada fila.
    
    :param df: DataFrame con las columnas 'player_url', 'transfer_date', 'ends_contract_date', 'code_from'.
    :return: DataFrame actualizado con las columnas 'ends_contract_date', 'archive_date', y 'processing_status'.
    """
    # Inicializar columnas adicionales si no existen
    if 'archive_date' not in df.columns:
        df['archive_date'] = None
    if 'processing_status' not in df.columns:
        df['processing_status'] = None
        # Marcar como 'processed' las filas que ya tienen información en 'ends_contract_date'
        df.loc[df['ends_contract_date'].notnull(), 'processing_status'] = "processed"

    # Filtrar filas a procesar
    rows_to_scrape = df[
        (df['ends_contract_date'].isnull())  # Falta fecha de contrato
        & (df['processing_status'] != "no_mirrors")  # No intentar filas sin mirrors válidos
        & (df['archive_date'].isnull())  # Añadir condición para filas sin archive_date
        # & (df['code_from'] != 515)  # Filtro adicional según lógica original
        # & (df['code_to'] != 515)    # Filtro adicional según lógica original
    ]

    for idx, row in rows_to_scrape.iterrows():
        try:
            print(f"Procesando fila {idx} con URL {row['player_url']} y fecha de transferencia {row['transfer_date']}...")

            # Scrapeo con validación del club propietario
            snapshots = scrape_transfermarkt_archive(
                row['player_url'],
                row['transfer_date'],
                row['code_from']
            )

            if snapshots == (None, None, None):  # No hay mirrors válidos
                df.at[idx, 'processing_status'] = "no_mirrors"
                print(f"No se encontraron mirrors válidos para la fila {idx}.")
            else:
                contract_date, archive_date, archive_url = snapshots
                if contract_date:  # Contrato encontrado
                    df.at[idx, 'ends_contract_date'] = contract_date
                    df.at[idx, 'archive_date'] = archive_date
                    df.at[idx, 'url_contract'] = archive_url
                    df.at[idx, 'processing_status'] = "processed"
                    print(f"Fila {idx} actualizada con fecha de contrato {contract_date} y fecha de archivo {archive_date}.")
                else:  # Mirrors válidos pero sin contrato
                    df.at[idx, 'processing_status'] = "contract_not_found"
                    print(f"Mirrors válidos pero no se encontró contrato para la fila {idx}.")

        except KeyboardInterrupt:
            # Permitir interrupciones manuales del proceso
            break
        except Exception as e:
            print(f"Error procesando la fila {idx}: {e}")

    try:
        # Primero convertir a datetime
        df["transfer_date"] = pd.to_datetime(df["transfer_date"], format='%Y-%m-%d', errors='coerce')
        df["ends_contract_date"] = pd.to_datetime(df["ends_contract_date"], format='mixed', dayfirst=True, errors='coerce')
        
        # Luego convertir al formato deseado
        df["transfer_date"] = df["transfer_date"].dt.strftime('%d/%m/%Y')
        df["ends_contract_date"] = df["ends_contract_date"].dt.strftime('%d/%m/%Y')
    
    except TypeError as e:
        print(f"Error de tipo en la conversión de fechas: {e}")
        print("Verificar tipos de datos en las columnas de fecha")
    
    except ValueError as e:
        print(f"Error de valor en la conversión de fechas: {e}")
        print("Posible problema con formato de fecha no reconocido")
    
    except AttributeError as e:
        print(f"Error de atributo en la conversión de fechas: {e}")
        print("Verificar que las columnas existan y sean del tipo correcto")
    
    except Exception as e:
        print(f"Error inesperado en la conversión de fechas: {e}")


    return df

# =============================================================================
# Ejemplo de uso
# Carga el DataFrame
# df = pd.read_excel('df_tfmkt.xlsx')
# df_updated = update_contract_dates(df)  # Usa tus funciones actuales
# df_updated.to_excel("df_tfmkt_updated.xlsx", index=False)
# =============================================================================



