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
Paleta de **grupos de variables** (chunk `setup2`), siguiendo la
descomposición del modelo $Z_{it}=(J,R,L,S)$ más $B$:
```python
COLOR_JUGADOR     = '#9b59b6'  # J: edad, altura, nacionalidad, posición
COLOR_RENDIMIENTO = '#3498db'  # R: pj, goles, asis, tarjetas, arquero
COLOR_TIEMPO      = '#f39c12'  # L, S: ventanas restantes, temporada, verano
COLOR_NEGOCIO     = '#2ecc71'  # B: valor de plantel comprador y vendedor
COLOR_TOBIT = '#e74c3c'
COLOR_MCO   = '#7f8c8d'
```
El dict `GRUPOS` mapea nombre → color y es la fuente única para colorear
gráficos y tablas.

Regla: mantener el mismo color para la misma variable/grupo en todos los
gráficos del deck (no reasignar colores por gráfico). En particular
`rest_ventana_transfer` va en **contrato y tiempo** (naranja), aunque
teóricamente se interprete como poder de negociación: el criterio es
seguir la descomposición formal que enseñan las slides de metodología.

`pio.templates.default = "plotly_dark"` — todos los gráficos Plotly
heredan tema oscuro por default, coherente con `theme: night`.

## Revelado progresivo
- `.fragment` simple para aparición secuencial de bullets/callouts cortos
  (poca cantidad, todos caben juntos en pantalla). **Ojo:** los fragments
  siguen ocupando espacio en el flujo del documento aunque estén ocultos
  por opacidad — apilar varios callouts completos con `.fragment` (uno
  por modelo/paso) puede desbordar el slide. Para eso usar la opción
  siguiente.
- `.fragment .fade-in-then-out` + `fragment-index` dentro de `.r-stack`
  para alternar contenido en el mismo espacio, ideal cuando cada paso es
  un bloque completo (callout, fórmula, comparación antes/después) que
  no debe convivir en pantalla con los demás (usado en "Antecedentes
  (Deporte)", "Modelo de Precios Hedónicos con poder de negociación",
  "Especificación empírica" y "Estrategia de Estimación: Modelos
  Anidados" — este último con plantilla fija de 4 líneas
  Objetivo/Agrega/Quita/R² por paso, para que la altura no salte entre
  estados).
- `auto-animate=true` en el header del slide + `data-id` en el bloque que
  persiste, cuando un mismo elemento se transforma entre slides
  consecutivas (usado en "Justificación del problema" y en toda la
  secuencia de "Información Muestral").
- **Fragments que modifican en lugar de aparecer**: `.fragment .strike`,
  `.semi-fade-out`, `.highlight-current-blue`. El elemento está visible
  desde el inicio y el fragment solo le cambia el estilo. Son los
  correctos para "esto estaba y ahora se saca" (tachado real *in situ*)
  y para steppers de progreso. Regla semántica: **nunca escribir texto
  ya tachado**; el tachado solo tiene sentido sobre algo que el público
  vio sin tachar antes. Usado en "Estrategia de Estimación: Modelos
  Anidados" (inventario de variables que se van tachando de un modelo al
  siguiente).
- Fragments anidados (`[[x]{.fragment .strike fragment-index=4}]{.fragment
  .fade-in fragment-index=2}`) para un elemento que aparece en un paso y
  se tacha en otro posterior.

## Estabilidad de layout (evitar saltos)
El CSS global centra verticalmente (`justify-content: space-evenly`), así
que cualquier cambio de altura del contenido mueve todo el slide.
- Agregar `{.top}` al header del slide fuerza `justify-content:
  flex-start` (regla ya definida en `styles.css`). Es la solución
  estándar del deck para secuencias con revelado progresivo.
- Además, darle **altura fija** a las zonas que cambian de contenido
  (ej.: `::: {style="height: 215px;"}` envolviendo un `.r-stack` de
  callouts de largo distinto). Sin eso, el `r-stack` toma la altura del
  hijo más alto y el resto del slide se corre.
- Los fragments ocultos siguen ocupando su lugar en el layout, así que
  un `.fragment .fade-in` dentro de una lista no desplaza a los demás.
- **Volumen de texto: oración resumen en pantalla, desarrollo en
  `::: {.notes}`.** Si un tema exige más de ~3 bloques de texto, no se
  divide en más slides ni se apilan callouts largos: se deja una oración
  resumen por idea (2 líneas) y el argumento completo va a las notas del
  expositor, que funcionan de ayuda memoria. Usado en "La Censura del
  Precio" (pasó de 6 bloques a 3 oraciones + notas).
- **Notas por fragmento.** Reveal las soporta de forma nativa: un
  `::: {.notes}` *dentro* de un `::: {.fragment}` se muestra solo cuando
  ese fragmento es el actual, y queda excluido del bloque de notas
  general del slide (el plugin filtra con `closest('.fragment')`). Sirve
  para que cada callout tenga su propia ayuda memoria en vez de un
  chorizo único. El `::: {.notes}` que quede fuera de todo fragmento es
  el que se ve al entrar al slide (útil para datos de contexto).
- El tamaño de letra de la vista del expositor no es configurable desde
  el `.qmd`, pero el plugin copia el `innerHTML` del `aside.notes`: si
  se envuelve el contenido en `::: {style="font-size: 0.8em;"}`, el
  estilo viaja a la ventana de notas.
- **No afirmar lo que no se muestra.** Si un callout dice "en el
  histograma se observa…", el histograma tiene que estar en el slide.
- **Medida de línea: un párrafo = un callout, y de a dos en columnas.**
  El CSS global da `width: 110%` a las secciones, así que un callout a
  todo el ancho obliga a recorrer el slide con la mirada para leer. En
  vez de un callout con dos párrafos, usar dos callouts (uno por
  párrafo, cada uno con su propio título) en
  `grid-template-columns: 1fr 1fr; gap: 26px`. Además de resolver el
  recorrido visual, obliga a titular cada idea. Usado en "Estrategia de
  Estimación: La Censura del Precio".
- **Inventarios de variables: en filas, no en columnas.** Una columna de
  8 ítems consume ~350px de alto y empuja el resto del slide fuera de
  pantalla. La misma información en una fila (`grid-template-columns:
  250px 1fr`, etiqueta del grupo a la izquierda, variables separadas por
  `·` a la derecha, `border-left` del color del grupo) ocupa ~45px. Si
  quedan grupos cortos, agruparlos de a dos en una fila partida
  (`grid-template-columns: 1fr 1fr`). Usado en "Estrategia de
  Estimación: Modelos Anidados".
- `panel-tabset`: **evitar salvo que el contenido de cada tab tenga la
  misma altura**, o envolverlo en `::: {style="height: XXXpx;"}` (como en
  "Definición de Variables"). Sin altura fija, el slide entero se
  recentra verticalmente (`justify-content: space-evenly` del CSS global)
  cada vez que cambia de tab, generando un salto visual molesto. Se
  descartó por este motivo en "Estrategia de Estimación" (2026-07-27),
  prefiriendo slides separados por tema.

## Dimensiones de color (no mezclarlas)
El deck tiene dos clasificaciones que se arrastran a lo largo de toda la
exposición, y no pueden compartir tono:
- **Grupos de variables** → el color: violeta jugador, azul rendimiento,
  naranja contrato y tiempo, verde negociación.
- **Observaciones con / sin precio** → **el relleno, no el color**: punto
  o barra sólida = con precio, hueco = sin precio, rayado = censurado.
  Se codifica así porque es un binario, y porque de ese modo el verde y
  el violeta quedan libres para los grupos de variables.

Los colores de **estimadores** (gris MCO, rojo Tobit) son locales a los
dos gráficos de comparación y no constituyen una tercera dimensión.

## Semántica de los callouts
El tipo de callout **significa algo**, no se elige por variedad visual:
- `note` (azul) → **neutral**. Exposición, definiciones, descripciones.
  Es el default: si el bloque no juzga nada, va azul.
- `tip` (verde) → lo que sube, lo bueno, lo que está a favor, lo que se
  incluye o se corrige.
- `important` (rojo) → lo que baja, lo malo, lo que está en contra, lo
  que se excluye.
- `warning` (amarillo) → el intermedio de esas escalas, y las
  advertencias o tensiones no resueltas.

Error a evitar: en "Truncado, Censurado y Tobit" los colores estaban
invertidos (verde al truncado, rojo al Tobit), lo que sugería que el
modelo preferido era el malo. Corregido a rojo → amarillo → verde según
cuánto corrige cada uno la censura.

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

## HTML crudo dentro de fenced divs
Pandoc tiene activa `markdown_in_html_blocks`: una tabla escrita como
HTML suelto **no** se trata como bloque opaco, sino que su contenido se
parsea como markdown y cada etiqueta sale como `RawBlock` separado. A
nivel de slide eso funciona, pero **dentro de un `:::` el seguimiento de
fences se rompe** y los cierres terminan como texto literal (Quarto avisa
"The following string was found in the document: ::::").

Regla: si una tabla HTML va dentro de un fenced div, envolverla en un
bloque raw explícito:

````
```{=html}
<table class="tabla-reg">…</table>
```
````

Así queda un único `RawBlock` y el anidamiento de divs se respeta.

Para diagnosticar este tipo de problema sin compilar el deck: extraer la
slide a un archivo aparte y correr el pandoc que trae Quarto
(`.../Quarto/bin/tools/pandoc.exe -f markdown -t native frag.md`).
Si aparece un `Para [ Str "::::" ]`, ese fence no se reconoció.

## Tablas de regresión (clase `.tabla-reg`)
Las tablas de resultados econométricos van en **HTML explícito**, no en
markdown, para reproducir el formato de la tesis en LaTeX:
- Cada modelo ocupa **dos columnas**: coeficiente (`td.c`) y error
  estándar entre paréntesis (`td.e`).
- **Alineación al punto decimal**: `td.c` va alineado a la derecha y las
  estrellas de significatividad viven en un `<span class="s">` de ancho
  fijo (2.1em). Así las estrellas no corren el punto decimal, que es lo
  que resuelve `r@{.}l` en LaTeX. Requiere `font-variant-numeric:
  tabular-nums` y que todos los valores tengan la misma cantidad de
  decimales.
- **Celdas vacías combinadas**: cuando un modelo no incluye la variable,
  `<td class="n" colspan="2"></td>` (equivale a `\multicolumn{2}{c}{}`).
- **Separadores**: `tr.sep` dibuja la línea horizontal entre grupos de
  variables (`\midrule`). Entre modelos **no van líneas verticales**: la
  separación se resuelve con `padding-left` en la celda que abre cada
  bloque (`.c`, `.n`, `.st`), nunca en el error estándar, que tiene que
  quedar pegado a su coeficiente.
- **Resaltar una columna** (el modelo preferido) con `<colgroup>` y
  `col.m3`, porque el fondo de `<col>` se pinta debajo de las celdas
  combinadas, cosa que `nth-child` no logra.
- La línea de color del grupo de variables va como `border-left` de
  `td.v`, con clase de grupo en el `<tr>` (`g-jug`, `g-rend`,
  `g-tiempo`, `g-negoc`).
- **Variante angosta** (`.tabla-angosta`): para tablas de pocas columnas.
  El `width: 100%` del estilo base estira la columna de nombres hasta
  dejar la tabla desparramada; el modificador la pasa a `width: auto` con
  `margin: 0 auto`, así se ajusta al contenido y queda centrada. Permite
  además subir la letra (0.95em), porque ocupa menos ancho.
- **Ocultar un slide sin borrarlo**: `{visibility="hidden"}` en el
  encabezado. Queda en el `.qmd` pero fuera de la presentación; se
  reactiva sacando el atributo.
- **Variante exploratoria** (`.tabla-hover`): duplicar el slide y en la
  primera copia habilitar el resaltado de fila y de bloque de modelo al
  pasar el mouse. Cada celda lleva `data-m="N"` con el número de modelo
  (el error estándar comparte el `data-m` de su coeficiente, así el
  resaltado toma el par completo). La segunda copia va sin el efecto,
  con el modelo preferido resaltado, para la exposición preparada. El
  criterio: el hover sirve para responder preguntas sueltas del jurado,
  y tenerlo activo mientras se expone satura visualmente.

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
- **Tipografía**: el default de Plotly (12px) es ilegible proyectado en
  un slide de 1600×900. Usar `font=dict(size=20)` como base, con
  `tickfont` 20, título de eje 22 y etiquetas de datos 17. Ampliar el
  margen izquierdo (`margin=dict(l=300)`) cuando las categorías del eje Y
  son nombres largos de variables.
- **Fondo transparente**: `paper_bgcolor` y `plot_bgcolor` en
  `'rgba(0,0,0,0)'` para que se vea el fondo del slide.
- **Orden de dibujo**: en comparaciones entre modelos, el modelo
  preferido va siempre encima (se agrega último), incluidos sus
  marcadores de "no estimada". Plotly dibuja las trazas en el orden en
  que se agregan; el orden de la leyenda se controla aparte con
  `legendrank`.

## Notas del expositor
Son **ayuda-memoria**, para leer de reojo y rápido durante la
exposición. Nunca párrafos: una línea de contexto y después viñetas
cortas, del tipo `**variable** valor → valor. Lectura en una frase.`
Tienen que entrar sin scrollear en la ventana del expositor. El
desarrollo largo va al capítulo de la tesis, no acá. Tamaño de letra
`0.7em` (el estilo inline viaja a la ventana de notas).
- Botones de tipo `updatemenus` con `type="buttons"` para alternar series
  (usado en "Temporalidad de la muestra": Total/Con Precio/Sin Precio).
- Mantener `hovermode="x unified"` en series temporales.
- Anotaciones de conteo (`N=...`) sobre el eje superior cuando se quiere
  mostrar tamaño muestral por tramo, sin saturar el gráfico.

## Redacción
- **Conservar el grado de certeza del original.** Al condensar texto de
  la tesis para una slide es fácil borrar los atenuantes ("muy
  probablemente", "puede ser un indicador", "es esperable que") y
  convertir una conjetura en afirmación. En una defensa eso es
  exactamente lo que el jurado ataca. Si el capítulo hedgea, la slide
  hedgea. Cuando un bloque es una explicación tentativa, decirlo en el
  título del callout y usar `.callout-warning`, reservado para lo
  ambiguo o no resuelto.
- **No usar la construcción "no es X, sino Y"** (ni "la diferencia no
  está en A, sino en B", "no se trata de A, es B"). Es una muletilla de
  redacción publicitaria, ajena al registro académico. Afirmar
  directamente: *"Las tres estimaciones se diferencian en el tratamiento
  que le dan a las transferencias sin precio"*.
- Preferir la afirmación directa al contraste retórico. Si el contraste
  es sustantivo (una alternativa que efectivamente se descartó), usar
  "en lugar de" / "en vez de", que es como lo redacta la tesis.

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
