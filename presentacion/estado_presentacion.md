# Estado de la presentación (Presentacion.qmd)

Última actualización: 2026-07-29

## Cómo usar este archivo
Al empezar una conversación nueva, subir este archivo + `Presentacion.qmd` +
(si aplica) el capítulo de la tesis en el que se está trabajando. No hace
falta subir los excels de datos: se gestionan por preguntas puntuales o
copiando solo el fragmento de código relevante.

## Plan general (orden acordado)
1. Metodología — modelos econométricos (puente antes de Resultados)
2. Resultados — modelos con precios positivos (Tabla 9)
3. Resultados — censura y Tobit (Tablas 10, 11, 12)
4. Conclusiones

## Tabla de estado

| # | Slide / bloque | Estado | Notas pendientes |
|---|---|---|---|
| A1 | Metodología: estrategia de modelos anidados (1→4) | ✅ Listo | Slide "Estrategia de Estimación: Modelos Anidados" con `{.top style="font-size: 0.9em;"}` (el 0.9em lo ajustó el usuario: entra justo). 4 zonas: (1) stepper 1·2·3·4 con `.fragment .highlight-current-blue`, (2) ecuación general **fija** (no muta), (3) **inventario de variables en FILAS** (una por grupo, `border-left` del color del grupo; última fila partida en 2: Contrato/tiempo + Negociación) — cada variable se tacha *in situ* con `.fragment .strike .semi-fade-out` en el modelo que la elimina, (4) `r-stack` de callouts, altura fija 250px. **Sin R²** (es Resultados). Descartes: `panel-tabset` (saltaba al cambiar tab), `.fragment` sin `r-stack` (se salía de pantalla), bullets telegráficos, v3 (número de modelo subía/bajaba, letra chica, M1 solo hablaba de la edad, tachado sobre texto nunca mostrado sin tachar), v4 (inventario en 4 columnas: los callouts quedaban fuera de pantalla → se pasó a filas) |
| A2 | Metodología: el problema de la censura (umbral L>0) | 🔄 En curso (v4) | Slide único "Estrategia de Estimación: La Censura del Precio". **2 columnas** (47/53, `align-items: center`): izquierda la Figura 5a de la tesis (`image5.png`, en tarjeta blanca porque el PNG tiene fondo blanco, **con `max-width: 540px`** — al 100% de la columna medía ~530px de alto y tiraba los callouts fuera de pantalla), derecha la ecuación de censura fija + 3 callouts breves con `.fragment` (acumulativos). **Todo el desarrollo va en `::: {.notes}`** (analogía FIFA, Royston 1992, euros/dólares, Monte Carlo) — en pantalla solo oraciones resumen. Se descartó la v2 (3 pasos × 2 callouts = 6 bloques: demasiado para el tema, y hablaba de un histograma que no se mostraba) |
| A3 | Metodología: Tobit vs. Censurado vs. Truncado | 🔄 En curso (v1) | Slide "Estrategia de Estimación: Truncado, Censurado y Tobit". Bajada fija: las 3 comparten la especificación del Modelo 3, difieren en qué hacen con las sin precio. **3 columnas** (una por estimador), cada una con una **mini-barra 388/381** que muestra qué observaciones usa: truncado = violeta punteado "excluidas"; censurado = violeta sólido "como precio cero"; Tobit = violeta rayado "censuradas en L". Debajo, callout de cierre sobre comparabilidad de coeficientes (efectos parciales) que hace de puente hacia las Tablas 10/11. Notas por fragmento: sesgo de selección, exclusión de ceros sin contrato vigente, **por qué no Heckman** (Carmichael et al. 1999) y EPA/EPP (Wooldridge 2010) |
| B1 | Tabla 9 completa (scrolleable) | 🔄 En curso (v3) | **Son DOS slides con la misma tabla.** (1) *Exploratoria*: clase `.tabla-hover`, al pasar el mouse ilumina la fila de la variable y las dos columnas del modelo (identificadas con `data-m`, insertado por script); sin resaltado del Modelo 3 para no saturar. Sirve para responder preguntas del jurado. (2) *Expositiva*: la estática, con el Modelo 3 resaltado y las notas completas. Si se edita la tabla hay que editar **las dos** |
| B1b | (detalle de formato de B1) | — | Slide "Resultados: Modelos con Precios Positivos" `{.smaller .scrollable .top}`. **Tabla en HTML explícito, no markdown** (clase `.tabla-reg`, CSS en `styles.css`), replicando el formato LaTeX del autor: par de columnas coef./error por modelo, **alineación al punto decimal** con las estrellas en una ranura de ancho fijo (equivalente a `r@{.}l`), **celdas vacías combinadas** con `colspan=2`, separadores horizontales entre grupos y línea vertical suave entre bloques de modelo. Modelo 3 resaltado vía `<colgroup>` (funciona debajo de las celdas combinadas). Nombres de variables en blanco, con línea de color a la izquierda por grupo (paleta de A1). Datos **verificados línea por línea contra `3.Resultados.md:152-224`**. Notas de expositor con la lectura de los 4 modelos |
| B2 | Forest plot Modelo 3 | 🔄 En curso (v1) | Slide "Resultados: Efectos Estimados del Modelo 3". Plotly, IC al 95% (±1.96·SE), línea punteada en cero, una traza por grupo para que la leyenda reproduzca la descomposición $J/R/L,S/B$. **`df_mco_data` reescrito**: pasó de 6 a **las 10 variables del Modelo 3** (se sumaron portero, temporada y las dos elasticidades de plantel) y se cambió la paleta a la agrupación formal de A1/B1. Orden del eje Y igual al de la Tabla 9 para leer gráfico y tabla en paralelo. Nota al pie aclarando elasticidades (log-log) vs. semi-elasticidades. Código verificado ejecutándolo aparte |
| B3 | Dumbbell Modelo 1 vs. Modelo 3 | 🔄 En curso (v3) | Igual que v2 pero: **el aspa lleva el color del modelo que NO estima la variable** (verde = la depuró el Modelo 3, gris = el Modelo 1 no la tenía). En v2 el aspa era gris siempre y se confundía con el color del Modelo 1, dando a entender que la variable desaparecía de ese modelo. Las aspas se dibujan **antes** que los puntos, para que el punto quede encima donde casi coinciden (`amonestac` = −0.004 y `verano` = 0.005 estiman prácticamente cero en el M1). Se quitó la columna lateral —su contenido pasó a las notas— para ganar ancho: más píxeles por unidad separan mejor los coeficientes chicos. Orden de leyenda forzado con `legendrank` |
| ~~B3 v2~~ | (histórico) | — | Slide "Resultados: Qué Cambia al Incorporar el Poder de Negociación". Dumbbell horizontal con **las 20 variables de la Tabla 9**: donde un modelo no estima la variable va un **aspa sobre el cero** y el tramo se dibuja **punteado**, para que no se lea como un coeficiente igual a cero. Gris `COLOR_MCO` = Modelo 1, verde `COLOR_NEGOCIO` = Modelo 3 (verde = grupo negociación: el color significa "el modelo que incorpora $B$", y evita chocar con el rojo del Tobit de C2). Layout 77/23: gráfico + columna lateral con dos callouts de lectura; **sin texto descriptivo arriba**, que es de donde salió el alto para las 20 filas (680px ≈ 31px por fila). Sin etiquetas numéricas: `pj` y `goles` se mueven 0.004 y se pisarían. Verificado: 18 valores en M1 y 10 en M3, que coincide con el `df_m` de la Tabla 9; 12 aspas; 8 segmentos sólidos |
| C1 | Tabla 10/11 completas | 🔄 En curso (v2) | **Dos slides**, ambas con `.tabla-reg`. (1) *"Modelo Censurado y Tobit"* = Tabla 10 → **OCULTA** con `visibility="hidden"`: no aportaba, queda en el fuente por si se la necesita (sacar el atributo para reactivarla). (2) *"Efectos Marginales de las Tres Estimaciones"* = Tabla 11, tres columnas sin errores estándar (la tesis no los reporta ahí), columna del Tobit resaltada; usa el modificador **`.tabla-angosta`** (`width: auto` + `margin: 0 auto` + `0.95em`) para no estirarse a todo el ancho, y se le quitó `.smaller` al slide. No hizo falta `.scrollable`. **Nomenclatura unificada**: la tesis usa `mediocampo`, `rest_transfer_window` y `season` en las Tablas 10/11 pero `mediocampista`, `rest_ventana_transfer` y `temporada` en la 9; en el deck se usan siempre las de la Tabla 9 para no cambiar de nombres a mitad de la exposición |
| C2 | Dumbbell MCO Truncado / Censurado / Tobit | 🔄 En curso (v1) | Slide "Resultados: El Efecto de Corregir la Censura". Tres puntos por variable sobre una línea de rango. **Colores:** dos grises para las dos variantes sin corrección (`COLOR_MCO` truncado, `#bdc3c7` censurado) y **rojo `COLOR_TOBIT` para el corregido**, siguiendo la semántica gris=sesgado / rojo=corregido que ya fija la paleta del chunk `setup`. El Tobit se dibuja último para quedar encima. `datos_tobit['Variable']` reescrito con los nombres crudos del resto del deck. **Sin líneas de rango y con `autorange=True`**, a propósito: así, al ocultar "MCO Censurado" desde la leyenda, el eje reescala y el gráfico funciona como zoom sobre truncado vs. Tobit (el rango pasa de [−0.657, 2.442] a [−0.181, 0.660]). Con líneas de rango eso no funcionaba: la línea se dibuja entre el mínimo y el máximo de las tres series y no responde al toggle, así que quedaba estirada apuntando a un punto oculto |
| C3 | Tabla 12 + comparación Probit/Tobit | 🔄 En curso (v1) | **Dos slides.** (1) *"¿Es Adecuado el Tobit?"* = Tabla 12 con `.tabla-angosta`, columnas Tobit/σ̂, Probit y Diferencia (esta última resaltada por `colgroup`); en negrita las dos discrepancias grandes, `delantero` (−0.207) y `lvalor_plantel_v` (−0.149). Bajada con el chequeo informal de Wooldridge (2010) y σ̂=√2.594≈1.610. **Fusionadas en una sola** (v2): tabla a la izquierda (61%, clase `.tabla-izq` que la alinea a la izquierda y la baja a 0.62em) y a la derecha tres bloques **sin título** —`border-left` gris neutro, no callouts de Quarto, que fuerzan un título— centrados en la lectura del probit: `lvalor_plantel_v` explica el precio pero no la probabilidad; `temporada` tampoco, porque la inflación opera siempre; el resto explica las dos cosas. Verificado: la columna Diferencia cierra aritméticamente en las 10 filas. **Descartados de acá:** el callout "cada modelo estima otra cosa" (gustó pero no va en este lugar — sin destino asignado todavía) y la explicación conjetural sobre las salidas libres, que sigue en `3.Resultados.md:510-524` por si el jurado pregunta |
| C4 | Síntesis de resultados | 🔄 En curso (v1) | Slide "Resultados: Síntesis". Grilla 2×2, un bloque por grupo de variables con la paleta del deck, cada uno con el efecto en el Modelo 3 y en el Tobit. Números verificados contra `3.Resultados.md`: edad −10%/−18%, pj +1.2%/+2%, goles +2.8%/+7%, ventanas ≈14% estable, temporada ≈16%, elasticidades comprador 0.54→0.66 y vendedor 0.31→0.11. **No responde las preguntas de investigación**: eso es D1, para no duplicar |
| D1 | Conclusiones: por qué y para qué se comercian | ✅ Listo | Pregunta en el recuadro rojo de la apertura + 2 columnas: "Por qué" (derechos de exclusividad, protección del patrimonio, estabilización de planteles) y "Para qué" (competir; el club maximiza **utilidad** en el sentido de Sloane 1971, con el beneficio como restricción, no ingresos) |
| D2 | Conclusiones: los atributos del precio | ✅ Listo | 5 bloques interpretativos, **sin repetir coeficientes**: edad (lineal decreciente), posición, nacionalidad (el pasaporte comunitario pierde peso al entrar los identificadores de clubes; en modelos acotados opera como señal de acceso al fútbol europeo), rendimiento y contrato |
| D3 | Conclusiones: la edad frente a la literatura internacional | ✅ Listo | Franceschi (2024): revisión sistemática de 29 artículos / 111 especificaciones, predominio europeo con 2 casos fuera. De las 82 con edad: U invertida 44 (54%), sin significancia 23 (28%), **decreciente 11 (13%) = este trabajo**, U 3 (4%). Barras proporcionales con `linear-gradient`. **Franceschi no está citado en la tesis** |
| D4 | Conclusiones: poder de negociación | ✅ Listo | Vendedores poder positivo / compradores negativo, en 2 renglones. Columna derecha al 28% con gráfico de barras (0.54 vs 0.31), en bloque `{=html}`; el pie remarca que **ambas elasticidades son positivas**, o sea que los dos empujan el precio hacia arriba |
| D5 | Conclusiones: limitaciones | ✅ Listo | Variables contractuales ausentes (salario y cláusula; solo Alemania), medición del rendimiento por posición, fiabilidad de transfermarkt en Sudamérica y ceros como dato real, alcance de la muestra ampliada |
| D6 | Conclusiones: agenda futura | ✅ Listo | Ampliar rendimiento observado (Lasso/Ridge/componentes principales), sumar lesiones/notoriedad/selección, y elegir la muestra según el objeto de estudio |
| D7 | Slide de cierre | ✅ Listo | Fondo `fondo.png`, "Gracias" + título y autoría |

Leyenda: ⬜ Pendiente | 🔄 En curso | ✅ Listo

## Decisiones ya tomadas (no volver a discutir salvo que cambie de opinión)
- Tablas de resultados (9, 10, 11, 12) se muestran **completas**, nunca
  resumidas de entrada. El R² no es el mensaje central (modelo explicativo,
  no predictivo): el foco son coeficientes y significatividad.
- Si una tabla no entra legible con `{.smaller}`, usar `.scrollable: true`
  en vez de dividir en tabs (decisión B1).
- Los gráficos (forest plot, dumbbell) son *complemento* de la tabla ya
  mostrada, no reemplazo.
- No agregar slides que sean redundantes con lo que ya se lee en la tabla
  (ej.: se descartó una slide aparte solo para R² por modelo).
- Fuente de datos: `_transferencias_21-24_ppt.xlsx` (contiene todo lo de
  `_transferencias_21-24.xlsx` más agregados). En el chunk `setup`:
  `df_raw = pd.read_excel('_transferencias_21-24_ppt.xlsx')` y luego
  `df = df_raw`.

## Inconsistencias detectadas en la tesis (a resolver por el autor)
- **Tabla 10 mal rotulada.** Se titula "Parámetros Estimados del Modelo
  Censurado y del modelo Tobit", pero la columna del Tobit no son los
  parámetros: son los **efectos marginales**, los mismos que ya figuran
  en la Tabla 11. Se comprueba con la salida de Stata: el coeficiente
  crudo de `edad` es −0.3127 y la tabla informa −0.181, que es el efecto
  marginal `ystar`. La única cifra que sí es un parámetro del Tobit en
  esa tabla es σ² = 2.593. **Decisión: la Tabla 10 queda oculta de forma
  definitiva**, porque además duplica la 11.
- **`edad²` en el Modelo 2.** `2.Metodología.md:924-928` dice que el
  Modelo 2 elimina el término cuadrático de la edad, pero la Tabla 9 lo
  muestra estimado en ese modelo (0.001, no significativo) y recién
  ausente en el Modelo 3. **Confirmado por el autor: es un error de la
  Metodología.** La presentación sigue la Tabla 9 (sale en el Modelo 3).
- **Mínimo de la variable dependiente.** `2.Metodología.md:165` menciona
  45 249 como mínimo observado; la Tabla 8 (`:801`) reporta 53 000.
  **Resuelto por el autor: el valor correcto es 45.249**; la Tabla 8
  quedó desactualizada tras la deflactación. La presentación usa 45.249
  y $L = \ln(45.000)$. *Pendiente en la tesis: actualizar la Tabla 8.*

## Errores corregidos en los chunks de datos
- **`datos_tobit`: errores estándar corridos (2026-07-28).** Mismo tipo
  de desfase que el de la edad. `Cens_se` estaba corrido desde
  `delantero` (tenía 0.452, que es el de `mediocampista`) y terminaba en
  0.000; correcto según Tabla 10:
  `[0.044, 0.490, 0.452, 0.768, 0.016, 0.058, 0.172, 0.179, 0.114, 0.170]`.
  `Tobit_se` estaba corrido desde `lvalor_plantel_c` y su último valor
  (0.202) era en realidad el error estándar de σ²; correcto:
  `[0.011, 0.112, 0.105, 0.218, 0.004, 0.013, 0.040, 0.041, 0.024, 0.040]`.
  Los coeficientes (`Cens_b`, `Tobit_b`, `MCO_b`) estaban bien.
- **SE de `edad` corrido una columna (2026-07-28).** En la fila de la edad,
  los errores estándar estaban desplazados: el Modelo 3 tenía 0.022 (que
  es el del Modelo 4) y el Modelo 4 tenía 0.000. Según la Tabla 9 los
  valores correctos son **M3 = 0.014** y **M4 = 0.022**. Corregido en los
  tres lugares donde aparecía: `datos_mco` (`MCO3_se`, `MCO4_se`),
  `datos_tobit` (`MCO_se`, que replica el Modelo 3) y `df_mco_data`
  (`SE`, usado por el forest plot). El resto de los SE de esas tablas
  coincide con la tesis.

## Muestras (no confundirlas)
- **1.484** transferencias documentadas inicialmente (scraping).
- **1.116** con contrato verificado (Tabla 2). De ellas, **453** con
  precio positivo (Tabla 3). *Es la muestra de la Figura 5.*
- **769** muestra final de regresión = **388 con precio** + **381 sin
  precio**: las que tienen información completa en todas las variables
  del modelo (slide "Muestra Final"). El N de la Tabla 9 (388/390) sale
  de acá.

## Nota sobre los datos (importante)
`_transferencias_21-24_ppt.xlsx` **ya no reproduce los N de la tesis**:
tiene 1788 filas (la tesis parte de 1484) y `tiene_contrato == 1` da 974
(la tesis reporta 1116). Tampoco se pudo reconstruir el 388/381 de la
muestra final con filtros razonables. **Consecuencia:** no regenerar
desde los datos ninguna figura que ya exista en la tesis — se
contradirían los números del documento que se defiende. Para esos casos
usar la imagen original (`image5.png`, `image6.png`, `image7.png`). El
excel sirve para gráficos nuevos que no reporten conteos muestrales.

## Notas del expositor
**Las escribe el autor** (decidido el 2026-07-28). No agregar bloques
`::: {.notes}` a las slides nuevas salvo pedido explícito.

## Ideas anotadas
- **Falta una tabla descriptiva de la muestra**, después de la definición
  de variables. Sería una mezcla entre la Tabla 7 (características por
  posición) y la Tabla 8 (resumen de rendimiento), más apropiada que
  cualquiera de las dos por separado. Sirve además para responder la
  objeción *"solo estás viendo la rama descendente de la edad"*: la
  muestra va de 18 a 39 años y el primer quintil es 18-23, así que los
  jóvenes están representados.
- **La slide de Franceschi es prescindible.** Se agregó porque el punto
  fue cuestionado en la corrección de la tesis (ver
  `Respuestas a Fernando Delbianco.docx`). Si la comparación con esa
  revisión termina complicando más de lo que aporta, se puede eliminar.
- **Candidato a `hidden`: el dumbbell "Del Modelo Base al Modelo Preferido"**
  (M1 vs M3). El autor considera que aporta poco a los resultados. *No
  ocultarlo todavía*, decidir al final.
- **Slide de discusión con la literatura internacional.** Hoy existe
  "Conclusiones: La Edad Frente a la Literatura Europea", que usa el
  meta-análisis de Franceschi (2024). La idea es una slide más amplia que
  discuta **todos** los resultados frente a la literatura, no solo la
  edad. No borrar la de la edad: se conserva.
- **Franceschi (2024) no está citado en la tesis.** Si se usa en la
  defensa, tener la referencia completa a mano y evaluar incorporarlo al
  capítulo.

## Ideas anotadas (evaluar al cerrar el grupo B)
- **Slide de principales resultados.** Idea del autor: una slide que
  sintetice los hallazgos centrales. Evaluar después de B3, viendo qué
  quedó ya cubierto entre la Tabla 9, el forest plot y el dumbbell.
- **Gráfico $y$ vs. $\hat{y}$ con ajuste lineal.** Para hablar del R² sin
  darle centralidad. Requiere traer de Stata los valores predichos y
  observados de la muestra de estimación (`predict yhat` + `lprecio`,
  exportado a csv/xlsx). **No se puede reconstruir desde
  `_transferencias_21-24_ppt.xlsx`**, porque ese archivo ya no reproduce
  la muestra de la tesis (ver "Nota sobre los datos"). Candidata a slide
  de respaldo ("si preguntan"), no a la línea principal, coherente con la
  decisión de que el R² no es el mensaje central.

## Pendiente de decidir
- Conteo de "slides lógicas" vs. "físicas" para el chequeo de tiempos de
  exposición (bloques `r-stack`/`auto-animate` cuentan como una sola
  unidad temática). Se resuelve con ensayo cronometrado real.
