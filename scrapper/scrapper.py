import pandas as pd
from obtener_transferidos import leer_transferidos
import time
import os
from tqdm import tqdm  # Para barras de progreso

def descargar_transferencias(ligas=['Argentina'], años=range(2021, 2025), guardar_excel=True):
    """
    Versión mejorada del scraper de Transfermarkt con:
    - Barra de progreso
    - Manejo de errores robusto
    - Opción de guardado o retorno de DataFrame
    - Validación de resultados
    """
    
    # Configuración
    TEMPORADAS = ['verano', 'invierno']
    DATOS = []
    ARCHIVO_EXCEL = 'transferencias.xlsx'
    
    try:
        print("🔍 Iniciando descarga de datos de Transfermarkt...")
        inicio = time.time()
        
        # Barra de progreso general
        with tqdm(total=len(ligas)*len(años)*len(TEMPORADAS), desc="Progreso total") as pbar:
            
            for liga in ligas:
                for año in años:
                    for temp in TEMPORADAS:
                        
                        # Saltar temporada de invierno del año actual
                        if año == 2024 and temp == 'invierno':
                            pbar.update(1)
                            continue
                        
                        try:
                            # Descarga con barra de progreso individual
                            with tqdm(desc=f"{liga} {año} {temp}", leave=False) as pbar_temp:
                                df = leer_transferidos(
                                    season=año,
                                    seas_w=temp,
                                    incluir_caracteristicas=True,
                                    incluir_valor=True,
                                    incluir_rendimiento=True,
                                    liga=liga
                                )
                                pbar_temp.update(1)
                                time.sleep(0.5)  # Espera corta
                                
                                if not df.empty:
                                    DATOS.append(df)
                                    pbar_temp.set_postfix(registros=len(df))
                                    
                        except Exception as e:
                            print(f"\n⚠️ Error en {liga} {año} {temp}: {str(e)}")
                            continue
                            
                        pbar.update(1)
        
        # Procesamiento final
        if not DATOS:
            print("\n❌ No se obtuvieron datos válidos")
            return None
            
        df_final = pd.concat(DATOS)
        
        # Limpieza de datos
        df_final = df_final.drop_duplicates(
            ['player_id', 'code_from', 'code_to', 'season', 'season_part']
        ).query("code_from != 515 and code_to != 515")
        
        # Guardado opcional en Excel
        if guardar_excel:
            try:
                df_final.to_excel(ARCHIVO_EXCEL, index=False)
                print(f"\n💾 Datos guardados en: {os.path.abspath(ARCHIVO_EXCEL)}")
            except Exception as e:
                print(f"\n❌ Error al guardar Excel: {str(e)}")
                # Intento alternativo
                try:
                    alt_path = os.path.expanduser('~/Desktop/transferencias.xlsx')
                    df_final.to_excel(alt_path, index=False)
                    print(f"💾 Guardado alternativo en el Escritorio: {alt_path}")
                except:
                    print("⚠️ No se pudo guardar en ninguna ubicación")
        
        print(f"\n✅ Descarga completada en {time.time()-inicio:.1f} segundos")
        print(f"📊 Registros obtenidos: {len(df_final)}")
        print(f"👥 Jugadores únicos: {df_final['player_id'].nunique()}")
        
        return df_final
        
    except Exception as e:
        print(f"\n❌ Error crítico: {str(e)}")
        return None

if __name__ == "__main__":
    # Ejecución con opciones
    datos = descargar_transferencias(
        ligas=['Argentina'],  # Puedes agregar más ligas
        años=range(2024, 2025),
        guardar_excel=True
    )
    
    if datos is not None:
        print("\n🔍 Vista previa de los datos:")
        print(datos[['nombre', 'edad', 'valor_mercado', 'club_destino']].head())