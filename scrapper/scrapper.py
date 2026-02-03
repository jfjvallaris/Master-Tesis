# -*- coding: utf-8 -*-

# file: transferencias_scraper.py

from obtener_transferidos import leer_transferidos
import pandas as pd
import time
from typing import List, Tuple

def obtener_transferencias(
    ligas: List[str],
    anios: List[int],
    temporadas: List[str] = ['verano', 'invierno'],
    excluir: List[Tuple[int, str]] = [(2024, 'invierno')],
    incluir_caracteristicas: bool = True,
    incluir_valor: bool = True,
    incluir_rendimiento: bool = True,
    rendimiento_actual: bool = True,
    incluir_info_club: bool = True,
    incluir_contract_expiration: bool = True,
    guardar_excel: bool = False,
    nombre_excel: str = "transferencias.xlsx",
    mostrar_logs: bool = True,
) -> pd.DataFrame:
    """
    Descarga transferencias de fútbol de múltiples ligas, años y temporadas.

    Devuelve un DataFrame limpio y opcionalmente guarda en un archivo Excel.
    """

    def _log(msg: str):
        if mostrar_logs:
            print(msg)

    start_time = time.time()
    transferencias = []

    for liga in ligas:
        for anio in anios:
            for temporada in temporadas:
                if (anio, temporada) in excluir:
                    continue

                _log(f"Procesando: {liga} - {anio} {temporada}")

                df = leer_transferidos(
                    anio,
                    seas_w=temporada,
                    incluir_caracteristicas=incluir_caracteristicas,
                    incluir_valor=incluir_valor,
                    incluir_rendimiento=incluir_rendimiento,
                    rendimiento_actual=rendimiento_actual,
                    incluir_info_club=incluir_info_club,
                    incluir_contract_expiration=incluir_contract_expiration,
                    liga=liga,
                )

                transferencias.append(df)
                time.sleep(2)

    df_final = pd.concat(transferencias, ignore_index=True)

    df_final = df_final.drop_duplicates(
        subset=['player_id', 'code_from', 'code_to', 'season', 'season_part']
    )

    df_final = df_final[
        ~((df_final['code_from'] == 515) | (df_final['code_to'] == 515))
    ]

    if guardar_excel:
        df_final.to_excel(nombre_excel, index=False)
        _log(f"Archivo guardado en: {nombre_excel}")

    tiempo_total = time.time() - start_time
    _log(f"Tiempo total: {tiempo_total // 60:.0f} min {tiempo_total % 60:.2f} seg")

    return df_final


# file: ejemplo_uso_transferencias.py

from transferencias_scraper import obtener_transferencias

# Definimos parámetros
ligas = ['Argentina', 'Brasil']
anios = list(range(2021, 2024))  # De 2021 a 2023
temporadas = ['verano', 'invierno']
excluir = [(2023, 'invierno')]  # Opcional

# Ejecutamos el scraping
df = obtener_transferencias(
    ligas=ligas,
    anios=anios,
    temporadas=temporadas,
    excluir=excluir,
    guardar_excel=True,
    nombre_excel="transferencias_filtradas.xlsx",
    mostrar_logs=True
)

# Mostramos un resumen
print(df.groupby(['liga', 'season']).size().reset_index(name='transferencias'))

