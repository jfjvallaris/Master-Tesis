# Guía de estilo — Presentacion.qmd

Referencia rápida de los patrones ya usados en el deck, para mantener
consistencia sin tener que releer el `.qmd` completo en cada sesión.

## Formato base
- `revealjs`, tema `night`, `width: 1600`, `height: 900`
- `mathjax: true` (LaTeX disponible en cualquier slide)
- `chalkboard: true` (incompatible con `embed-resources`, no activar ambos)
- Motor: `jupyter: python3`, `execute: echo: false, warning: false`

## Paleta de colores (definida en chunk `setup`)
```python
COLORES = {
    'mco': '#7f8c8d',       # Gris (Sesgado)
    'tobit': '#e74c3c',     # Rojo (Corregido)
    'probit': '#3498db',    # Azul (Probabilidad)
    'positivo': '#2ecc71',  # Verde
    'negativo': '#c0392b',  # Rojo oscuro
    'neutro': '#bdc3c7'     # Gris claro
}
```
Paleta secundaria (chunk `setup2`, para forest plot / agrupación temática
de variables):
```python
COLOR_TOBIT = '#e74c3c'
COLOR_MCO = '#7f8c8d'
COLOR_POSICION = '#9b59b6'     # variables de posición del jugador
COLOR_NEGOCIO = '#2ecc71'      # ventanas restantes / negociación
COLOR_RENDIMIENTO = '#3498db'  # pj, goles
COLOR_EDAD = '#c0392b'
```
Regla: mantener el mismo color para la misma variable/grupo en todos los
gráficos del deck (no reasignar colores por gráfico).

`pio.templates.default = "plotly_dark"` — todos los gráficos Plotly
heredan tema oscuro por default, coherente con `theme: night`.

## Revelado progresivo
- `.fragment` simple para aparición secuencial de bullets/callouts.
- `.fragment .fade-in-then-out` + `fragment-index` dentro de `.r-stack`
  para alternar contenido en el mismo espacio (usado en "Antecedentes
  (Deporte)" y en "Modelo de Precios Hedónicos con poder de negociación").
- `auto-animate=true` en el header del slide + `data-id` en el bloque que
  persiste, cuando un mismo elemento se transforma entre slides
  consecutivas (usado en "Justificación del problema" y en toda la
  secuencia de "Información Muestral").

## Callouts (para agrupar argumentos/citas de autores)
- `.callout-note appearance="minimal" icon=false` → definiciones o
  preguntas centrales.
- `.callout-tip appearance="simple/minimal" icon=false` → argumentos a
  favor / evidencia que sostiene una hipótesis.
- `.callout-important appearance="simple/minimal" icon=false` → críticas,
  contraargumentos, supuestos que se descartan.
- `.callout-warning appearance="simple/minimal" icon=false` → efectos
  ambiguos, tensiones no resueltas.
- Título del callout va en `title="..."`, texto interno en
  `style="font-size: 1.1em;"` o similar.

## Tamaño de fuente
- Texto secundario / notas al pie de figura: `font-size: 0.5em` a `0.6em`
- Texto de columnas comparativas: `font-size: 0.75em` a `0.8em`
- Cuerpo de callouts: `font-size: 0.85em` a `1.1em` según jerarquía
- Slides con tablas densas: `{.smaller}` en el header del slide

## Tablas grandes (Tabla 9, 10, 11, 12)
Decisión: se muestran **completas**, nunca resumidas de entrada.
- Primer intento: `{.smaller}` en el header.
- Si no entra legible: `.scrollable: true` en el header del slide
  (decisión tomada para B1/C1, preferido sobre dividir en `panel-tabset`).

## Columnas y layout
- `.columns` + `.column width="X%"` para layouts de 2 bloques.
- `display: flex; align-items: center;` cuando se necesita centrado
  vertical entre columnas de distinta altura (usado en "Antecedentes" y
  en "Modelo de Precios Hedónicos con poder de negociación").
- `grid-template-columns: 1fr 1fr; gap: 20px;` como alternativa a
  `.columns` cuando se quieren 4 bloques en grilla 2x2 (usado en
  "Objetivos de la Investigación" e "Hipótesis").

## Gráficos Plotly interactivos
- Botones de tipo `updatemenus` con `type="buttons"` para alternar series
  (usado en "Temporalidad de la muestra": Total/Con Precio/Sin Precio).
- Mantener `hovermode="x unified"` en series temporales.
- Anotaciones de conteo (`N=...`) sobre el eje superior cuando se quiere
  mostrar tamaño muestral por tramo, sin saturar el gráfico.

## Tono narrativo
- Presentación académica, no corporativa: evitar slides "punchline" o
  gráficos que sustituyan una tabla de resultados ya mostrada — los
  gráficos son complemento de lectura, no reemplazo (ver decisión en
  `estado_presentacion.md`).
- El foco en modelos explicativos son coeficientes y significatividad,
  no bondad de ajuste (R²) como mensaje central.

## Fuente de datos
`_transferencias_21-24_ppt.xlsx` (superset de `_transferencias_21-24.xlsx`
con agregados). Carga estándar en chunk `setup`:
```python
df_raw = pd.read_excel('_transferencias_21-24_ppt.xlsx')
df = df_raw
```
