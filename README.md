# 🧠 Transfermarkt + FBRef Scraper & Matcher

Este repositorio implementa un pipeline completo de scraping, limpieza, emparejamiento y análisis de datos de transferencias y rendimiento de jugadores a partir de fuentes como **Transfermarkt** y **FBRef**.

---

## 📁 Estructura del Proyecto

```
.
.
├── Bases_generadas/
│   └── *.csv                      # Bases limpias y combinadas desde notebooks
├── fuzzy_clubes/
│   ├── rosseta_club.ipynb  # Matching entre clubes Transfermarkt y FBRef
│   ├── club_mapping_with_code.json         # Diccionario de IDs matcheados
│   ├── unmatched_clubs.csv      # Clubes sin match
├── fuzzy_jugadores/
│   ├── rossetta_tfmkt-fbref.ipynb # Matching entre jugadores Transfermarkt y FBRef
│   ├── player_id_mapping.json
│   ├── unmatched_players.csv
├── scrapper/
│   ├── obtener_transferidos.py   # Core scraper
│   ├── tmkt_rendimiento.py       # Estadísticas detalladas por jugador
│   ├── tmkt_info_club.py         # Info del club por temporada
│   ├── tmkt_get_contract_date.py # Fechas de contrato desde Transfermarkt
│   ├── get_contract.py           # Soporte vía Wayback Machine
│   ├── scrapper.py               # Runner principal
│   └── ...
├── df_fbref.csv                  # Base scrapeada desde FBRef
├── df_transfermarkt.xlsx        # Base scrapeada desde Transfermarkt
├── Analisis_tfmkt-fbref.ipynb   # Comparación, gráficos y métricas entre ambas fuentes
├── analisis_t.ipynb             # Exploración de Transfermarkt
├── merge_t2_and_fbref-by_id.ipynb # Unión final por jugador_id entre bases
├── Correct_season_and_concat_t-2.ipynb # Unión de temporadas y ajuste por fechas
```

---

## 🧪 Componentes Clave

### 🔎 Scrapper Transfermarkt (`/scrapper`)
- Extrae jugadores transferidos, valores, rendimiento (actual y anterior), características y vencimiento de contrato.
- Soporta múltiples temporadas y ventanas (`verano`, `invierno`).
- Ajustes inteligentes por:
  - Fecha de transferencia
  - Liga hemisférica (verano/invierno)
  - Temporadas largas (`long_season`)
  - Códigos de clubes
  - Scraping de páginas archivadas si el dato contractual está perdido.

### 🔁 Matching jerárquico fuzzy
- `fuzzy_clubes/`: Matcheo entre clubes por similitud textual desde ligas equivalentes.
- `fuzzy_jugadores/`: Emparejamiento de jugadores basado en club, posición, edad y nombre.
- Algoritmo en cascada que minimiza falsos positivos usando criterios jerárquicos.

### 📊 Análisis y Fusión
- `Analisis_tfmkt-fbref.ipynb`: Validación cruzada de métricas y detección de outliers.
- `merge_t2_and_fbref-by_id.ipynb`: Unión final basada en IDs matcheados, con posibilidad de analizar rendimiento pre y post transferencia.

---

## ▶️ Ejecución del Pipeline

1. **Scrapear Transfermarkt**  
   Desde `scrapper/scrapper.py`:

   ```bash
   python scrapper.py
   ```

2. **Scrapear FBRef (externo)**  
   Asegúrate de tener `df_fbref.csv` correctamente generado.

3. **Emparejar clubes y jugadores**  
   Ejecutar los notebooks de fuzzy matching en orden:

   - `fuzzy_clubes/fuzzy_match_clubes.ipynb`
   - `fuzzy_jugadores/fuzzy_match_jugadores.ipynb`

4. **Analizar y unir bases**  
   Con las notebooks:
   - `merge_t2_and_fbref-by_id.ipynb`
   - `Analisis_tfmkt-fbref.ipynb`

---

## 📦 Output Esperado

- CSV/Excel con rendimiento antes y después de la transferencia
- Valor de mercado, fees, edad, contrato, etc.
- Datasets para modelado de regresión, clasificación o clustering

---

## ⚙️ Requisitos

```txt
pandas
numpy
beautifulsoup4
requests
waybackpy
scikit-learn
fuzzywuzzy
unidecode
```

---

## 🧠 Autoría y Uso

Este código está diseñado para fines académicos y analíticos. Si lo reutilizas, por favor cita el proyecto o contacta con el autor.