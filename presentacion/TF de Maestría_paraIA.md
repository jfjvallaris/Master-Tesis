**Resumen**

Este trabajo analiza los determinantes del precio de los pases en el
mercado del fútbol argentino mediante la estimación de modelos de
precios hedónicos. A partir de un enfoque empírico, se utilizan modelos
de regresión lineal (MCO), modelos censurados y el modelo Tobit para
evaluar cómo factores individuales (edad, posición en el campo,
rendimiento pasado) y colectivos (valor del plantel, contexto del
mercado de transferencias) influyen en el valor de los jugadores. Los
resultados indican que el rendimiento previo, el valor del plantel del
club comprador y el tiempo restante de contrato, medido en ventana de
transferencias, tienen efectos significativos sobre el precio.

Además, se evidencia que el modelo Tobit es adecuado para capturar la
censura en los datos, aunque muestra diferencias respecto a los efectos
estimados en modelos de mínimos cuadrados. Se discute el rol del poder
de negociación en las transferencias sin costo y su impacto en la
interpretación de los coeficientes. Los hallazgos contribuyen a la
literatura sobre la Economía del Deporte al ofrecer evidencia sobre los
mecanismos de formación de precios en un mercado donde los clubes no
siempre maximizan beneficios, sino que también buscan otros objetivos
estratégicos y deportivos.

This study analyzes the determinants of transfer fees in the Argentine
football market through the estimation of hedonic pricing models. Using
an empirical approach, linear regression (OLS), censored models, and
Tobit models are employed to evaluate how individual factors (age,
playing position, past performance) and collective factors (squad market
value, transfer window timing) influence player valuation. The results
indicate that prior performance, the buyer club's squad value, and
remaining contract time, measured in terms of the transfer window, have
significant effects on transfer prices.

Additionally, the Tobit model proves suitable for capturing data
censoring, although it shows differences when compared to OLS estimates.
The role of bargaining power in free transfers and its impact on
coefficient interpretation is also discussed. These findings contribute
to the Sports Economics literature by providing evidence on price
formation mechanisms in a market where clubs do not always seek to
maximize profit but rather pursue strategic and sporting objectives.

**Palabras Clave**: Economía del Deporte, Poder de Negociación, Precios
Hedónicos, Mercado Laboral, Organización Industrial.

*Clasificación JEL*: L83, C5.

# Contenido {#contenido .TOC-Heading}

[Introducción [6](#introducción)](#introducción)

[1. Marco Teórico [13](#marco-teórico)](#marco-teórico)

[1.1. Antecedentes [13](#antecedentes)](#antecedentes)

[1.2. Breve repaso sobre la Economía del Deporte
[20](#breve-repaso-sobre-la-economía-del-deporte)](#breve-repaso-sobre-la-economía-del-deporte)

[1.3. El mercado de transferencias
[25](#el-mercado-de-transferencias)](#el-mercado-de-transferencias)

[1.4. El modelo de precios hedónicos
[31](#el-modelo-de-precios-hedónicos)](#el-modelo-de-precios-hedónicos)

[1.4.1. Precios hedónicos con poder de negociación
[35](#precios-hedónicos-con-poder-de-negociación)](#precios-hedónicos-con-poder-de-negociación)

[1.5. Síntesis del capítulo
[39](#síntesis-del-capítulo)](#síntesis-del-capítulo)

[2. Metodología [42](#metodología)](#metodología)

[2.1. Introducción [42](#introducción-1)](#introducción-1)

[2.2. Especificación empírica
[42](#especificación-empírica)](#especificación-empírica)

[2.3. Datos y variables [48](#datos-y-variables)](#datos-y-variables)

[2.3.1. Datos [48](#datos)](#datos)

[2.3.2. Variables [53](#variables)](#variables)

[2.3.3. Modelos econométricos
[61](#modelos-econométricos)](#modelos-econométricos)

[3. Resultados y discusión
[64](#resultados-y-discusión)](#resultados-y-discusión)

[3.1. Introducción [64](#introducción-2)](#introducción-2)

[3.2. Modelos hedónicos con Precios Positivos
[64](#modelos-hedónicos-con-precios-positivos)](#modelos-hedónicos-con-precios-positivos)

[3.3. Censura, truncamiento y estimación Tobit
[71](#censura-truncamiento-y-estimación-tobit)](#censura-truncamiento-y-estimación-tobit)

[Conclusiones [76](#conclusiones)](#conclusiones)

[Bibliografía [80](#_Toc198470891)](#_Toc198470891)

[Anexo 1 [86](#anexo-1)](#anexo-1)

[Anexo 2 [87](#anexo-2)](#anexo-2)

**\
**

**Índice de Tablas**

[Tabla 1 *Los 20 fichajes más caros de la historia del futbol a precios
de octubre de 2024* [8](#_Ref197277552)](#_Ref197277552)

[Tabla 2 *Transferencias con contrato verificado por temporada y
ventana* [50](#_Ref197615001)](#_Ref197615001)

[Tabla 3 *Transferencias con precio positivo por temporada y ventana*
[51](#_Ref197615051)](#_Ref197615051)

[Tabla 4 *Observaciones obtenidas con scraping con precio positivo sin
contrato vigente. (Precios corrientes)*
[52](#_Ref197616403)](#_Ref197616403)

[Tabla 5 *Definición de las Variables*
[53](#_Ref197682009)](#_Ref197682009)

[Tabla 6 *Precios promedio de las transferencias en el fútbol argentino
por posición y tipo de transferencia en el periodo 2021-2024 (en euros
constantes de junio 2021)* [55](#_Ref197683245)](#_Ref197683245)

[Tabla 7 *Promedio de características de los jugadores transferidos con
contrato vigente por posición en el período 2021-2024*
[57](#_Ref197947296)](#_Ref197947296)

[Tabla 8 *Resumen estadístico de las variables de rendimiento en el
período 2021-2024* [59](#_Ref197950312)](#_Ref197950312)

[Tabla 9 *Parámetros Estimados de Modelos Simples de Precios Hedónicos:
variable dependiente lprecio* [68](#_Ref198034319)](#_Ref198034319)

[Tabla 10 *Parámetros Estimados del Modelo Censurado y del modelo Tobit:
variable dependiente lprecio* [72](#_Ref198041508)](#_Ref198041508)

[Tabla 11 *Efectos Marginales de los Modelos Truncado (MCO), Censurado
(MCO) y Censurado Tobit: Variable Dependiente lprecio.*
[72](#_Ref198045226)](#_Ref198045226)

[Tabla 12 *Comparación de coeficientes Probit vs. Tobit estandarizados*
[75](#_Ref198045529)](#_Ref198045529)

[Tabla 13 *Estimación Probit sobre la probabilidad de tener precio:
Variable Dependiente tiene_precio (1 = precio \> 0)*
[86](#_Ref197959854)](#_Ref197959854)

[Tabla 14 *Regresión del valor de clubes en el período 2020-2023*
[87](#_Ref197966963)](#_Ref197966963)

[Tabla 15 *Efectos fijos de la nacionalidad del equipo en el logaritmo
del valor del plantel respecto de Argentina. Modelo (2)*
[88](#_Toc198472217)](#_Toc198472217)

**\
**

**Índice de Figuras**

[Figura 1 Distribución del negocio del fútbol argentino en el año 2013.
[9](#_Ref197281367)](#_Ref197281367)

[Figura 2 Clasificación de los tipos de productos deportivos según su
objeto social [24](#_Ref197338036)](#_Ref197338036)

[Figura 3 Modelo de precios hedonicos de equilibrio
[35](#_Ref197510592)](#_Ref197510592)

[Figura 4 Modelo de precios hedónicos de equilibrio con excedente por
distribuir [37](#_Ref197512534)](#_Ref197512534)

[Figura 5 Histograma del logaritmo del precio de las transferencias de
futbolistas con contrato para el periodo 2021-2024
[47](#_Ref197601074)](#_Ref197601074)

[Figura 6 Distribución de las transferencias por cuatrimestre
[50](#_Ref197614977)](#_Ref197614977)

**\
**

# Introducción {#introducción .Titulo-1-sin-numeracion}

El presente trabajo surge a partir de ser detectada una carencia en el
estudio económico del mercado de pases del fútbol argentino. Si bien el
campo de la Economía del Deporte es relativamente nuevo, en los últimos
30 años han proliferado las investigaciones teóricas y empíricas que,
usando conceptos, modelos y metodologías de la economía pura y aplicada,
estudian diferentes aspectos económicos y sociales que hacen al fenómeno
del deporte en general, y del fútbol en particular. Entonces, debido a
que está literatura está concentrada en el mercado europeo, se eligió de
tema de investigación el estudio de los factores que inciden en la
determinación del precio de los jugadores en el mercado de pases del
fútbol argentino.

El fútbol constituye una pasión de multitudes en gran parte de los
países del mundo, y con el paso del tiempo, este deporte (como muchos
otros) ha generado varios mercados, donde la cantidad de dinero que se
mueve en los mismos es cada vez más elevada.

Así, por ejemplo, La consultora Deloitte & Touche (2024) estima que para
la temporada 2022/23, el mercado europeo del fútbol en su totalidad
alcanzó los 35 300 millones de euros, representando un crecimiento del
16% respecto del año anterior; una tasa de crecimiento superior al de la
economía europea en su conjunto para ese año. En gran parte, este
crecimiento se explica por la recuperación lenta a la caída de los
ingresos generado por la pandemia del año 2020. Las proyecciones para la
temporada 23/24 y 24/25 son de un crecimiento menor, cercano al 6.5 y 4%
respectivamente.

A nivel local se carece de información que permita establecer
comparaciones en el tiempo. En la única investigación al respecto,
Coremberg, Sanguinetti y Wierny (2016) estiman que el Valor Bruto de
Producción (VBP) ---o facturación--- de los productos, servicios y
sectores asociados al fútbol en el año 2013 fue de ARS 32 915 millones
(aproximadamente USD 5 000 millones). Los clubes de fútbol tan sólo se
apropian del 20%, es decir, ARS 8 860 millones (aproximadamente USD
1 363 millones). Asimismo, en términos relativos, el VBP del fútbol
representa el 2.2% del consumo de los hogares argentinos, numero para
nada despreciable si se tiene en cuenta que el eje central de este
consumo está impulsado por Asociaciones Civiles Sin Fines de Lucro
(ACSFL). En este sentido, el VBP del fútbol no es muy distante a las
estimadas para España (2.1% en 1999), o para el deporte en general de
algunos países, como son: España 3.5%, Polonia 2.1%, Reino Unido 3.2%,
Austria 3.6%, y Holanda 1.3% (Coremberg et al., 2016).

Estos cambios en los ingresos tienen una estrecha relación con los
mercados de transferencias. Si se entiende que la competencia económica
y la competencia deportiva entre los clubes están relacionadas entre sí,
el mercado de transferencias funciona como una suerte de puente, que
distribuye el talento hacia los equipos de mayores ingresos y, a su vez,
distribuye el dinero hacia los equipos que poseen una mayor cantidad de
talento en sus planteles (y no pueden sostener sus costos).

En lo que respecta a valores individuales pagados por un futbolista, en
la Tabla 1 se muestran los fichajes más caros de la historia reciente
del fútbol. En la misma, puede observarse que, aun actualizando los
precios, los fichajes de los años prepandemia siguen siendo superiores a
los pagados por figuras incuestionables del pasado reciente.

El interés por la economía del deporte en la argentina tiene su primer
antecedente con Di Marco (1978), quien sostiene que: "La diversidad y
enorme difusión del deporte en la mayoría de los países del mundo ha
dado como resultado una compleja demanda de bienes y servicios para
satisfacer sus fines.". Argentina no es la excepción, sin embargo, el
campo de la Economía del Deporte casi no ha sido explorado en ninguno de
sus aspectos. Esto es algo que sorprende debido al relativo interés
social del fútbol.

En este sentido, Coremberg, Sanguinetti, y Wierny estimaron que para el
año 2013, la suma total del valor de producción de los productos y
servicios asociados al fútbol es ARS 41 774 millones (aproximadamente
USD 6 300 millones de ese año u USD 8 500 millones de dólares actuales)
para el negocio en su conjunto, lo que representó aproximadamente el
1.2% del PBI. Los clubes de fútbol explican sólo el 21% del giro del
negocio, mientras que el 79% se genera en los sectores asociados. Donde
"la prensa, publicidad y sponsors participan del 25% del giro del
negocio, en tanto que la transmisión y producción de radio y TV un 23.4%
y el merchandising (bebidas, indumentaria, etc.) un 22.6%. El turismo,
cultura, bares y restaurantes un 8% y el resto de los servicios y
sectores un 13%" (Coremberg et al., 2016).

  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  []{#_Ref197277552 .anchor}Tabla 1\                                                                                                                                                                  
  *Los 20 fichajes más caros de la historia del futbol a precios de octubre de 2024*                                                                                                                  
  ----------------------------------------------------------------------------------------------------------------- ---------- --------------- ----------- -- ----------- --------- --- ------------- --------
  **Jugador**                                                                                                       **Edad**   **Temporada**   **Precio de    **Precio                  **Ranking\    
                                                                                                                                               pase\          Actual\                   Histórico**   
                                                                                                                                               (Millones      (Millones                               
                                                                                                                                               €)**           €)**                                    

  Neymar                                                                                                            25         17/18           222.0                      285.41                      1

  Kylian Mbappé                                                                                                     19         18/19           180.0                      226.88                      3

  Philippe Coutinho                                                                                                 25         17/18           135.0                      173.56                      2

  Ousmane Dembele                                                                                                   20         17/18           135.0                      173.56                      4

  Joao Félix                                                                                                        19         19/20           127.2                      158.90                      5

  Eden Hazard                                                                                                       28         19/20           120.8                      150.14                      6

  Antoine Griezmann                                                                                                 28         19/20           120.0                      149.14                      16

  Cristiano Ronaldo                                                                                                 33         18/19           117.0                      147.47                      8

  Jack Grealish                                                                                                     25         21/22           117.5                      140.88                      7

  Paul Pogba                                                                                                        23         16/17           105.0                      137.35                      9

  Cristiano Ronaldo                                                                                                 24         09/10           94.0                       136.33                      10

  Romelu Lukaku                                                                                                     28         21/22           113.0                      135.48                      11

  Zinedine Zidane                                                                                                   29         01/02           77.5                       133.60                      12

  Gareth Bale                                                                                                       24         13/14           101.0                      132.96                      37

  Enzo Fernández                                                                                                    22         22/23           121.0                      128.21                      13

  Declan Rice                                                                                                       24         23/24           116.6                      120.34                      17

  Moisés Caicedo                                                                                                    21         23/24           116.0                      119.12                      20

  Gonzalo Higuain                                                                                                   28         16/17           90.0                       117.81                      14

  Jude Bellingham                                                                                                   20         23/24           113.0                      116.62                      15

  Neymar                                                                                                            21         13/14           88.0                       116.42                      42

  Fuente: Elaboración propia con datos de transfermarket.com y                                                                                                                                        
  [Eurostat](https://ec.europa.eu/eurostat/databrowser/view/prc_hicp_midx__custom_13918102/default/table?lang=en)                                                                                     
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Los ingresos por transferencias de futbolistas, junto a la venta de
entradas y las cuotas societarias, son los únicos recursos de los que
participan exclusivamente los clubes de fútbol (Coremberg et al., 2016).
Para el resto de las actividades, la apropiación del VBP por parte de
los clubes es de la siguiente forma: Obtienen 6.3% del merchandising,
prensa y publicidad, el 18% de la transmisión y producción televisiva y
radial y un 36.7% del negocio del rubro otros servicios. En tanto que el
turismo, cultura y alimentos fuera del hogar asociados al fútbol se
generan exclusivamente por sectores asociados (Figura 1).

Coremberg et al. (2016) sostienen que es un mito decir que *el fútbol
local mueve mucho dinero*, debido a que el VBP solo representa el 2.2%
del consumo de las familias. Sin embargo, esta situación no debe servir
para excluir al fútbol de la investigación económica. En Argentina, los
clubes son Sociedades Civiles Sin Fines de Lucro, y, por tanto, no debe
analizarse su alcance como el de una industria más.

La importancia y el interés social del fútbol sobrepasan al consumo en
nuestro país. Los clubes, cumplen el rol de contención social intentando
mantener a los jóvenes fuera del delito y el consumo de drogas,
principalmente, durante su periodo de formación.

  -----------------------------------------------------------------------
  []{#_Ref197281367 .anchor}Figura 1\
  Distribución del negocio del fútbol argentino en el año 2013.
  -----------------------------------------------------------------------
  ![](./image1.png){width="5.083333333333333in" height="3.03125in"}

  Fuente: Coremberg et al. (2016)
  -----------------------------------------------------------------------

Otro dato que refuerza el interés social por el fútbol es el *rating*
televisivo. La final de la Copa del Mundo 2022[^1], ganada por la
Selección Nacional, cosechó 15 puntos más de rating que el debate
presidencial de 2023[^2]. La preponderancia del fútbol en la televisión
es un fenómeno que no se limita a un máximo histórico, sino que es un
fenómeno que se observa año a año, aun en medio de la expansión de otros
medios de entretenimiento.

El mercado de transferencias presenta un atractivo particular al momento
de plantear el recorte dentro del análisis económico del fútbol. Y,
particularmente, la determinación del precio de transferencia por ser
parte integral del análisis económico del mercado laboral del fútbol
(Carmichael, 2006). En términos concretos, el mercado de transferencias
puede considerarse uno de los pocos mercados (si no es el único), en el
cuál las firmas pagan una tarifa por el derecho a contratar un
trabajador con formación especializada, que está prestando sus servicios
en otra firma. La oportunidad para estudiar un mercado con las
características antes mencionadas está limitada al fútbol, debido a que
los principales deportes de conjunto en América del Norte no poseen un
mercado de transferencias desde que se estableció la agencia libre en
1976 (Carmichael, 2006).

El interés por el mercado laboral del fútbol es aún mayor si se tienen
en cuenta las cifras que se pagan por el derecho de contratar a los
jugadores de fútbol (Tabla 1), los salarios de los futbolistas con
respecto a los salarios de otros trabajadores y a la cantidad de años
que el jugador puede prestar sus servicios. Además, esta situación
ocurre, desde hace casi 30 años, en un marco de libertad. Es decir, que
los jugadores no pueden considerarse un insumo que las firmas deportivas
comercian como si fuesen un bien más de la economía, sino que deben
entenderse como un trabajador con sus respectivos derechos. Esta
situación, implica que el club que quiera contratar a un futbolista
necesita de su consentimiento y el del club de origen, y esto abre dos
vías de negociación: la negociación salarial y la negociación por la
transferencia.

El objetivo principal de este trabajo es abordar cuestiones empíricas
hasta ahora descuidadas en la literatura local. Particularmente, conocer
los precios implícitos de las características de los jugadores de fútbol
en el mercado de pases de Argentina, y estimar el impacto del poder de
mercado y de negociación inferido a partir del tamaño de los clubes.
Para ello, será necesario, previamente, revisar la literatura existente
sobre los mercados del deporte en general, del fútbol en particular y
con más especificidad del mercado de transferencia de jugadores.

Para alcanzar estos objetivos, se recurrirá inicialmente al enfoque
cualitativo a través de la revisión bibliográfica y al análisis
descriptivo con el objetivo de analizar las características propias de
los mercados asociados al deporte. Para tal fin, se identificarán los
puntos en común entre el análisis de la teoría económica para los
mercados tradicionales con los planteados por la Economía del Deporte,
destacándose las particularidades que pueden refutar algunas de las
conclusiones heredadas de la teoría. Para ahondar sobre la determinación
de los precios de transferencia, se incorporarán los textos académicos
enfocados a tal fin.

Seguidamente, se utilizará el enfoque cuantitativo para la construcción
de un modelo econométrico, que permita dar cuenta de los factores
determinantes de los precios, sus valores implícitos y la existencia o
no de poder de negociación. Para tal fin, se recopilarán los datos
disponibles en el sitio web *transfermarkt* para todos los jugadores
transferidos del fútbol argentino en el periodo 2021-2024[^3].

El análisis de los datos se realizará a través de un modelo de precios
hedónicos con poder de mercado. Además, siguiendo lo estudiado por
Carmichael, Forrest, y Simmons (1999) sobre las transferencias de precio
cero, se estimará un modelo *Tobit* de variables censuradas.

Otra alternativa estudiada para el tratamiento de este tipo de muestras
es, en analogía con lo estimado por los autores, estimar la probabilidad
de un jugador de ser transferido con precio. En la medida en que algunos
jugadores son más propensos a tener precio de transferencia que otros,
por la razón que sea, una ecuación estimada por MCO sufrirá un sesgo de
selección que conducirá a estimaciones sesgadas e inconsistentes de los
coeficientes. Para ello se puede proponer un procedimiento en dos pasos
de Heckman (1979) para la corrección del sesgo de selección a tener una
tarifa de transferencia en el mercado de pases.

El problema con este tratamiento es que requiere de una variable de
exclusión que afecte la probabilidad de tener precio, pero no al precio
en sí mismo. Esta variable no fue encontrada, y es algo esperable, ya
que, estimar la probabilidad de tener precio es, en parte, una forma de
estimar el precio.

En contrapartida, para estudiar los problemas de autoselección, se
analizará la diferencia entre un modelo truncado para observaciones con
precio, frente un modelo censurado con precio observado cero, y un
modelo Tobit que incorpore la probabilidad de observar el precio. En
este sentido, es importante aclarar que solo se tendrán las
transferencias sin precio, pero con contrato activo. Ya que se considera
que una transferencia sin un contrato que vincule a un jugador con la
institución en la que jugó la última temporada, no requiere una tarifa
de transferencia, y, por tanto, la probabilidad de observar un precio
debería ser cero.

La estructura del Trabajo Final de Maestría constará de tres capítulos.
En el Capítulo 1 exponen los antecedentes de la investigación, y se
desarrollará el marco teórico desde el punto de vista económico y
estadístico. Posteriormente en el Capítulo 2 se describe la metodología
empleada para estudiar el mercado de transferencias del fútbol
argentino, y los datos y las variables utilizados en la investigación.
En el Capítulo 3, se presentan los resultados empíricos con sus
interpretaciones. Para finalmente cerrar el trabajo con las conclusiones
generales de la tesina.

Capítulo 1

# Marco Teórico

## Antecedentes

La literatura económica relacionada con el mercado de transferencias del
fútbol comienza a difundirse en la década de los 90, enfocándose en las
ligas europeas (Carmichael y Thomas, 1993; Speight y Thomas, 1997;
Reilly y Witt, 1995; Carmichael et al., 1999). No obstante, comparado
con la proliferación del resto de las áreas de la Economía del Deporte,
los estudios sobre fútbol son relativamente escasos.

La falta de literatura referida al mercado de transferencias de
futbolistas, especialmente en la etapa inicial del desarrollo de la
Economía del Deporte, puede explicarse a través de dos dificultades. La
primera de ellas es que la mayor parte de la literatura referida a la
Economía del Deporte fue escrita en Estados Unidos, donde el fútbol, no
posee la misma importancia que los deportes conocidos como los cuatro
grandes (béisbol, fútbol americano, hockey sobre hielo y básquetbol).
Estos deportes no poseen un mercado de transferencias, entonces, el
estudio de este tipo de mercado depende exclusivamente de los trabajos
realizados para el fútbol. Esta problemática no se observa en otras
áreas de la Economía del Deporte, como son: el balance competitivo
(Mohamed y Quirk, 1971; Quirk y El-Hodiri, 1974), el principio de
invarianza (Fort y Quirk, Cross-Subsidization, Incentives, and Outcomes
in Professional Team Sports Leagues, 1995; Vrooman, 1995; Krautmann y
Oppenheimer, 1994; Hylan et al., 1996), la determinación de los salarios
(Scully, 1974), los efectos de la salida de la cláusula de reserva
(Holahan, 1978; Hill y Spellman, 1983), la incertidumbre en el
resultado, entre otros.

La segunda dificultad que encontró el desarrollo de la literatura
relacionada con las transferencias de futbolistas es la complejidad
propia del deporte. El aporte de los jugadores durante los partidos es
mayor al capturado por las estadísticas tradicionales (Goles
convertidos, goles recibidos, asistencias, partidos jugados). Otro
inconveniente relacionado con la naturaleza del deporte es que algunas
características de las acciones de juego no son medibles en estadísticas
o, en el caso de que lo sean, su aplicabilidad a este tipo de estudios
se presenta con una excesiva complejidad[^4].

Con el avance de la tecnología, algunas empresas comenzaron a obtener
estadísticas de mayor precisión. Por ejemplo, desde 1996 la empresa Opta
*Sports* recolecta e informa estadísticas de los partidos con un gran
grado de detalle. Además, a partir de ellas, la empresa desarrolla
índices de rendimiento que son tenidos en cuenta por los clubes de
fútbol para valorar los aportes de los jugadores en los partidos. En la
investigación económica, solo se pudo dar cuenta de una única
utilización (Carmichael et al., 2000), a pesar de las buenas críticas y
su gran recepción entre los medios de comunicación y los clubes de
fútbol.

Actualmente, existen sitios web que ofrecen una valoración de los
jugadores, aunque esta no debe entenderse como una estimación del
precio. Aunque hace algunos años, ninguno de ellos establecía a las
estadísticas como un criterio explícito, posteriormente se fue
incorporando el criterio. Por ejemplo, el CIES Football Observatory
(2018) establecía entre sus criterios para los jugadores transferidos en
las cinco grandes ligas: La duración del contrato, el año de
transferencia, el valor en libros, el estado del préstamo, la
nacionalidad y el nivel económico del club comprador (estimado si se
desconoce). Posteriormente ---intentando esta vez predecir precios---
ajustaron su modelo en función del trabajo escrito por su propio
director, Raffaele Poli. Según algunos medios de comunicación, este
modelo habría sido un pedido de la FIFA para fijar precios de
futbolistas[^5] [^6].

En su trabajo, Poli, Besson, y Ravenel (2024) incorporaron a los
regresores que el observatorio utilizaba, variables de rendimiento como
los minutos jugados y los goles convertidos en los últimos 2 años
calendario, los gastos de los compradores (liga y equipo) y los ingresos
de los vendedores, la posición más ocupada por el jugador, los puntos
obtenidos por los clubes, entre otros.

Por otro lado, el sitio web alemán transfermarkt.com, establece
explícitamente los criterios que se utilizan para la valoración de los
futbolistas en su foro de opinión recién desde 2021. Textualmente dice:

> Los valores de mercado de Transfermarkt se calculan teniendo en cuenta
> varios modelos de fijación de precios. Un factor importante es la
> comunidad de Transfermarkt, cuyos miembros discuten y evalúan en
> detalle los valores del mercado de los jugadores. En general, los
> valores de mercado de Transfermarkt no deben equipararse a las
> cantidades realmente pagadas en concepto de traspaso. El objetivo no
> es predecir un precio, sino más bien el valor esperado de un jugador
> en el mercado. Tanto las modalidades individuales de fichajes como el
> contexto general son relevantes para determinar los valores de
> mercado. \[\...\] Asimismo, Transfermarkt no utiliza ningún algoritmo
> para elaborar sus valores, sino que se basa en el criterio de la
> comunidad (Transfermarket, 2024)

La primera investigación económica sobre el mercado de transferencia de
futbolistas es el trabajo de Carmichael y Thomas (1993), donde los
autores expanden al fútbol las contribuciones realizadas a la economía
del deporte en general y al béisbol en particular por Rottenberg (1956).
Para realizar su trabajo, tomaron datos de los jugadores transferidos al
final de la temporada 90-91 del fútbol inglés, utilizando como
principales determinantes del precio, las características de los equipos
involucrados, las características del jugador y su rendimiento. Debido a
que no existían antecedentes sobre mercados de transferencias de
deportistas, Carmichael y Thomas (1993) basaron su investigación de
*cómo* incorporan los equipos profesionales de fútbol, en la negociación
por el salario de los agentes libres del béisbol.

Reilly y Witt (1995) extienden el trabajo de Carmichael y Thomas (1993),
incluyendo la posibilidad de discriminación racial sin encontrar
resultados significativos de discriminación para las transferencias de
la temporada 91-92. Medcalfe (2008), 13 años después, obtiene iguales
conclusiones para las transferencias de la temporada 2001-2002. En este
sentido, no puede dejar de destacarse la fuerte influencia del estudio
del béisbol norteamericano, el cual ha estudiado ampliamente el fenómeno
de la discriminación fundamentado en la historia de dicho deporte
(Gwartney y Haworth, 1974; Raimondo, 1983; Hill y Spellman, 1984;
Christiano, 1986; Nardinelli y Simon, 1990). Otro aporte interesante de
Reilly y Witt (1995), es no tomar la edad como una variable continua,
sino que, a diferencia de sus predecesores, la determinan como variable
binaria en función de determinados rangos de interés.

Por otra parte, Speight y Thomas (1997) estudian el impacto del
arbitraje sobre los precios en el fútbol inglés. Más específicamente, la
influencia del *Football league Appeal Committee* (FLAC), sobre los
precios de las transferencias de jugadores en el período que comprende
las temporadas 85/86 y la 89/90.

Dobson y Gerrard (1999) analizaron las transferencias del fútbol inglés
entre junio de 1990 y agosto de 1996. El objetivo principal era
determinar la tasa de inflación en los precios de las transferencias y
la segmentación del mercado. Adicionalmente, buscaron captar el efecto
que había tenido la ley Bosman en los precios pagados por las
transferencias de los futbolistas. La segmentación del mercado tuvo como
referencia tres tipos de transferencia (horizontal, ascenso, descenso)
para la primera división y para las divisiones menores en conjunto.
Buscaron determinar cuáles son las variables significativas en cada
caso, teniendo en cuenta cuatro conjuntos de variables que ya habían
establecido otros autores: características de los jugadores,
características del club comprador y del club vendedor, y otras
variables de control.

La ley o sentencia Bosman del año 1995, significó un punto de inflexión
en la metodología a aplicar en estos estudios debido a que transformó el
mercado de transferencias en Europa. Uno de los efectos de esta
sentencia fue eliminar el derecho a una tarifa de traspaso, formación o
promoción una vez finalizado el contrato entre el futbolista profesional
mayor a 24 años y el equipo. La influencia de esta sentencia en los
modelos teóricos de determinación de los precios de transferencia tardó
algunos años en manifestarse. Por ejemplo, Szymanski y Smith (1997)
deciden suponer, por primera vez, que el mercado de transferencias de
jugadores puede considerarse competitivo, con restricciones a partir del
periodo conocido como de *libertad de contratación* (1977-78), y sin
restricciones a partir de la sentencia Bosman.

Carmichael et al. (1999) estiman el primer modelo bajo un enfoque
competitivo, utilizando como base el modelo de precios hedónicos. La
justificación para dicho cambio metodológico tenía su base en el
objetivo de la investigación: modelar los efectos de selección de
muestras a través del proceso de dos pasos de Heckman, para corregir el
sesgo muestral encontrado en los modelos anteriores. De esta manera, en
el primer paso, se estima la probabilidad de ser transferido de cada
jugador y en la segunda etapa se estima el modelo de precios de las
transferencias, incluyendo la corrección al sesgo de selección.

La decisión de utilizar un modelo de precios hedónicos no está
fundamentada en la sentencia Bosman, los autores utilizaron los precios
de transferencias de la temporada 94-95 de la liga inglesa, que son
anteriores a la sentencia. La imposibilidad de utilizar un enfoque de
negociación se encuentra en la inexistencia de un club comprador para un
jugador no transferido.

Los autores sostienen que el enfoque de negociación seguido en la
literatura es, de hecho, inconsistente con el modelo de
selección-corrección, ya que el primero contiene variables para las
características de los clubes compradores, como son los ingresos o
ganancias y las asistencias a los estadios. Dichas variables no pueden
incluirse en un modelo de corrección de selección, ya que todas las
variables en la ecuación del precio (excepto el término de corrección de
selección) deben incluirse en la ecuación de probabilidad de
transferencia. Este último contiene información sobre todos los
jugadores, no solo los transferidos, y para aquellos jugadores no
transferidos, no existe información sobre el club comprador (Carmichael
et al., 1999, p. 128).

Está investigación determinó que existen jugadores con una mayor
probabilidad de ser transferidos. Este aumento es más alto para
jugadores más experimentados, que han marcado mayor cantidad de goles y
jugadores a préstamo sin un largo historial de transferencias. Estos
resultados implican que la estimación de ecuaciones de precios hedónicos
requiere corrección por sesgo de selección.

Carmichael et al. (1999), encontraron en las afirmaciones de Szymanski y
Smith (1997), un antecedente para asumir el mercado competitivo. Estos
últimos sostienen que el mercado de transferencias se caracteriza por la
libertad de contratación para los jugadores, muchos compradores y
vendedores (en un mercado internacional en el que el idioma no es una
barrera importante), y una red de información integral (con base
subjetiva) en la que el esfuerzo y el rendimiento de los jugadores son
observados y monitoreados fácilmente. También puede agregarse otro
aporte de Szymanski y Smith (1997) asociado a los mercados competitivos
en el fútbol: las empresas en esta industria tienen poco control sobre
sus principales costos de insumos, los jugadores, porque se negocian en
un mercado, a su criterio, competitivo.

De todas maneras, Carmichael (2006) continuará sosteniendo que el
enfoque de negociación es apropiado en la medida en que el mercado de
transferencias no es verdaderamente competitivo porque se caracteriza
por (a) la incertidumbre que conduce al riesgo y la incompletitud
contractual, y/o (b) la negociación de pequeños números de agentes
(Equipos o jugadores de interés) asociada con la especificidad de la
transacción de forma tal de que la identidad es importante.

Respecto a la existencia de incertidumbre, Carmichael y Thomas (2000)
sostienen que la misma existe debido a la información asimétrica sobre
aspectos de la calidad y el compromiso de un jugador. Además, existe un
riesgo porque no estará claro, antes de una transferencia, qué tan bien
se desempeñará un jugador como parte de una nueva estructura de equipo.
El debate sobre el apartado (b) será discutido en la sección
[1.3](#el-mercado-de-transferencias) de este trabajo.

Por su parte, Tunaru, Clark, y Viney (2005) incorporan el factor
dinámico en la determinación del valor del jugador, intentando responder
a la siguiente pregunta: "¿Cuánto *vale* este jugador en este momento en
el tiempo?". Su enfoque se basa en modelos de opciones reales que, según
los autores, es una de las mejores herramientas teóricas para la toma de
decisiones disponibles para cuando los objetos analizados no se
comercializan en el mercado. Concretamente, mide la incertidumbre que
rodea el rendimiento profesional de un jugador, incluyendo eventuales
lesiones, y la incertidumbre que rodea a los ingresos de un club.
También se incorporan los rendimientos derivados de las actuaciones del
jugador, así como aquellos derivados de la imagen del jugador, la imagen
del club, de la fidelidad de la afición, de la economía en general, etc.

Si bien reconocen que los jugadores se venden y compran en una especie
de mercado, este no se corresponde con el significado financiero de
mercado. Además, sostienen que las transferencias de jugadores de un
club a otro a cambio de una suma de dinero son menos comunes, por lo
menos en comparativa con la cantidad total de jugadores. De todas
maneras, esto no afectaría la metodología propuesta porque, de una forma
u otra, los modelos de opciones reales que muestran se pueden aplicar de
la misma manera, solo que el uso de los resultados difiere ligeramente
según sea el caso.

Ruijg y van Ophem (2015) buscan determinar los precios de los jugadores
para la temporada 2011-2012 de la primera división de la liga inglesa.
Los autores también realizan una corrección por sesgo de selección de la
muestra, pero a diferencia de Carmichael et al. (1999) utilizan la
hipótesis de que la muestra seleccionada de transferencias no es una
muestra aleatoria de todas las transferencias, porque el número relativo
de tasas de transferencia que realmente se hace pública es muy pequeño
(38% para la temporada 2011/12 y el 9% para la 2012/13).

Una de las herramientas utilizadas para la corrección de este problema,
es la clasificación de los equipos involucrados en las transferencias
dentro y fuera del país analizado (Inglaterra). El objetivo es
determinar si la transferencia representa una mejora o un deterioro, ya
que a entender de los autores: "las grandes mejoras van acompañadas de
un nuevo salario alto, un precio alto de transferencia cuando sea
aplicable y una alta productividad." (Ruijg y van Ophem, 2015).
Utilizaron como base el ranking de equipos de la UEFA, pero se
encontraron con la necesidad de adaptarlo debido a que no existía una
correspondencia plena entre un ranking positivo y el pago de salarios
elevados.

Rodríguez, Hassan, y Coad (2019) analizan los factores que influyen en
el valor de mercado de los jugadores de fútbol en las principales ligas
europeas durante la temporada 2015/2016. Para ello, utilizaron un marco
de regresión hedónica e implementaron el Promedio de Modelos Bayesianos
(BMA) a través de la composición de modelos de Monte Carlo con cadenas
de Markov (MC) y el Promedio de Modelos Bayesianos con Variables
Instrumentales (IVBMA) para abordar la incertidumbre del modelo y la
endogeneidad. El objetivo de este enfoque es poder abarcar los 35.000
millones de modelos posibles con las 36 variables seleccionadas.

Los autores determinaron que variables como el rendimiento, la
participación en el equipo nacional, la edad, y la participación en la
selección mayor y sub-21, son determinantes clave del valor de mercado.
Sugieren que la relación de la edad con el valor de mercado es
cuadrática, y esta alcanza su valor máximo alrededor de los 26 años. El
estudio también resalta la importancia de comprender el valor de mercado
de los jugadores como un fenómeno económico significativo en el fútbol
profesional.

Más recientemente, Poli, Besson, y Ravenel (2021) estudiaron los
determinantes de los precios de transferencia de los equipos de las
principales ligas europeas para el periodo de julio de 2012 a noviembre
de 2021, analizando más de 2000 transacciones. El trabajo destaca la
importancia de incorporar los años de contrato restantes del jugador
para mejorar la predictibilidad.

Es importante destacar, que el trabajo de los autores se centra en la
predictibilidad de los precios, y no en los efectos individuales de los
determinantes en los precios. De todas maneras, la significatividad de
los coeficientes es importante en este tipo de análisis. Posteriormente
(Poli et al., 2024), realizaron los últimos avances en modelización
estadística de los factores que los actores del mercado tienen en cuenta
para determinar los precios de transferencia de los jugadores de fútbol
profesional. Ampliaron la muestra a más de 8000 transacciones de
jugadores transferidos por dinero desde clubes de todo el mundo durante
el período que se extiende desde julio de 2014 hasta marzo de 2024. El
trabajo muestra que un modelo estadístico puede explicar hasta el 85% de
las diferencias en las tarifas de transferencia pagadas por los
jugadores. A pesar de los casos específicos y otras posibles
distorsiones, los autores sostienen que el uso de un modelo estadístico
para determinar los precios de transferencia de jugadores es muy
relevante a escala global.

Cómo puede observarse, desde la década de los noventa existe una
proliferación de trabajos teóricos y empíricos para conocer el fenómeno
de determinación de precios en el mercado de fútbol. Sin embargo, tales
estudios han estado focalizados en el mercado de pases del fútbol
europeo, no encontrándose antecedentes para el fútbol latinoamericano en
general, y argentino en particular. Con esta investigación se busca
reducir ese vacío.

## Breve repaso sobre la Economía del Deporte

La investigación sobre la Economía del Deporte, área de estudio se
focaliza en los deportes de equipo y no en las competencias
individuales, inicia con el trabajo realizado por Rottenberg (1956)
sobre el mercado de trabajo del béisbol norteamericano. Para ello,
necesitó previamente revisar algunas de las particularidades de los
mercados que rodean al deporte desde la perspectiva de la teoría
económica tradicional.

Fort (2005) sintetizo este trabajo en sus once puntos principales, a los
cuales denominó las anclas sobre las que se sostiene la Economía del
Deporte. Posteriormente, han sido destacadas por Sloane (2006) en su
evaluación del medio siglo de la Economía del Deporte, reforzando así la
importancia de resaltar estos conceptos al momento de estudiar la
materia.

Las once **Anclas** de la Economía del Deporte son:

1.  El mercado laboral es monopsonista. Esto es así, pues para la época,
    los jugadores de béisbol, y los futbolistas también, eran propiedad
    de los equipos de por vida.

2.  El mercado de productos es monopolístico. Esta es una característica
    específica para el caso específico del béisbol norteamericano, donde
    existen derechos de exclusividad en las ciudades para cada equipo.

3.  Hay clubes ricos y pobres, basados en asistencias en relación con el
    tamaño de la población.

4.  La asistencia del público al estadio es una función de algunas
    variables clave[^7].

5.  La cláusula de reserva no proporciona una distribución equitativa
    del talento.

6.  Las ventajas del sistema *draft*[^8] son en gran medida ilusorias.

7.  La perspectiva de salarios muy altos atrae a un exceso de jugadores,
    llevando a una amplia dispersión salarial.

8.  Los propietarios de los equipos de béisbol son maximizadores
    racionales de ganancias[^9].

9.  Las diferencias en la calidad de los rivales no deben ser "demasiado
    grandes" para producir un producto exitoso (incertidumbre de
    resultado).

10. El mercado libre es tan eficiente como la cláusula de reserva en
    términos de asignación de recursos (el principio de invariancia).

11. La desaparición de la cláusula de reserva no tendría ningún impacto
    en la cantidad de entrenamiento o la calidad del juego.

Respecto al alcance y validez de esta lista, debe tenerse en cuenta que
dichas Anclas constituyen un punto de partida razonable para el modelado
teórico en la Economía del Deporte, lo que no significa que sus
enunciados sean verdaderos. En algunos casos, las anclas operan como
supuestos axiomáticos para el modelado, mientras que otros resultan ser
hipótesis de trabajo plausible de contrastar para algún caso en
particular.

Respecto al alcance y validez de esta lista, debe tenerse en cuenta que
dichas Anclas constituyen un punto de partida razonable para el modelado
teórico en la Economía del Deporte, lo que no significa que sus
enunciados sean verdaderos. En algunos casos, las anclas operan como
supuestos axiomáticos para el modelado, mientras que otros resultan ser
hipótesis de trabajo plausible de contrastar para algún caso en
particular.

Di Marco (1978) clasifica a la Economía del Deporte según su
manifestación sea directa o indirecta en la economía. La primera de
ellas es el aporte que hace la actividad deportiva al producto bruto.
Esta puede realizarse hacia atrás (inversión en capital) o hacia
adelante (comercialización). Además, la actividad deportiva posee un
efecto multiplicador en la economía, por su impacto en los medios de
comunicación, el sector financiero y la inversión en obras. La segunda
manifestación se da, sin ser exhaustivos, a través de posibles mejoras
en el nivel de vida y aumento de la productividad laboral de individuos
que participan en una actividad deportiva.

En lo que respecta a las particularidades de la Economía del Deporte,
Neale (1964) es quien más ha desarrollado dicha cuestión. En su trabajo
titulado *The Peculiar Economics of professional sports*, evidencia
varias singularidades de la Economía del Deporte, que le otorgan un
atractivo especial al estudio de este campo.

Concretamente, a través de un contraejemplo, propone lo que él denomina
la paradoja de Louis-Schmelling[^10], con la que explica por qué en la
Economía del Deporte el máximo beneficio de la industria no se encuentra
en el monopolio, sino que, por el contrario, requiere de algún tipo de
competencia. Esto se debe, en gran parte, a que la esencia del deporte
es la competencia, y en la cual la incertidumbre del resultado deportivo
juega un papel fundamental. Es por esto, que se necesita que exista
competencia de al menos dos contendientes para la existencia de un
mercado deportivo.

Otro aporte de Neale es el concepto de *producto conjunto invertido*. En
la teoría económica, se denomina producto conjunto, al grupo de
productos que se obtienen de un único insumo por medio de un proceso
indivisible. En este caso, el espectáculo deportivo funciona de manera
invertida, porque requiere de al menos dos contendientes que hacen la
función de *insumo* de un producto final único: el encuentro. Neale
sostiene que, en el deporte, "se obtiene un producto indivisible a
partir de procesos separados de dos o más firmas" (Neale, 1964, p. 2).
Esta lógica no niega la individualidad de cada contrincante. En otras
palabras, existe una dualidad producto divisible-indivisible ofrecido
por un equipo. Aunque el consumidor asista al estadio o encienda el
televisor para ver jugar a un equipo en particular, también está viendo
a su rival. No puede deshacerse de él, porque está viendo competir a su
equipo y para ello necesita a su contrincante. El aficionado, por
ejemplo, puede ir el domingo a ver a River Plate, pero también está
viendo el partido River-Platense o River-Vélez. El consumo del rival
viene atado por la naturaleza del deporte. Sin embargo, el rival no es
inerte: algunos despiertan mayor interés que otros, ya sea por la
rivalidad o la dificultad que representan, generando con ello un
resultado económico diferente.

Si bien en estas primeras aproximaciones a la teoría económica del
deporte se hace referencia específica al espectáculo deportivo, existen
otros mercados al margen de este, por ejemplo, los artículos distintivos
que identifican a los equipos. No obstante, la existencia de estos
depende fundamentalmente del espectáculo deportivo.

Lozano y Gallego (2011) clasifican los productos y servicios ofrecidos
por los equipos de fútbol, según el objeto social que representan. Esta
clasificación se presenta en la Figura 2. Desde esta perspectiva, el
*producto conjunto invertido* responde a la participación en
competencias deportivas de carácter profesional (Categoría A), la cual
puede entenderse como aquella que da origen al resto de las otras
categorías (B y C). De esta manera, se puede hablar del espectáculo
deportivo, como el producto principal de este tipo peculiar de mercado.

  -----------------------------------------------------------------------
  []{#_Ref197338036 .anchor}Figura 2\
  Clasificación de los tipos de productos deportivos según su objeto
  social
  -----------------------------------------------------------------------
  ![](./image2.png){width="5.802083333333333in" height="1.875in"}

  Fuente: Lozano (2016)
  -----------------------------------------------------------------------

Di Marco (1978) sostiene que, dentro del análisis microeconómico del
deporte, el aporte Neale (1964) contribuyó a la teoría de la firma en el
contexto de la competencia deportiva y de la competencia en los
mercados. A su entender, uno de los aportes más importantes, es
establecer que los ingresos dependen de la competencia deportiva y no de
la competencia económica, ya que, a mayor colusión económica y mayor
competencia deportiva, se obtendrían mayores ingresos y beneficios. Para
arribar a esta conclusión, debe considerarse a la liga, y no a los
clubes, como la firma que toma decisiones. En palabras de Neale (1964),
la conclusión, entonces, es que la empresa de negocios tal como se
entiende en la ley (y, por lo tanto, en la discusión común) no es la
firma como se entiende en la teoría económica. Más bien, la firma es la
liga. Y más adelante sostiene que una vez realizado este punto, la
conclusión teórica es clara: cada deporte profesional es un monopolio
natural.

La teoría aceptada actualmente, establece al club como la firma y se
basa en los postulados de Rottenberg (1956) para determinar los axiomas
de comportamiento, a excepción del Ancla 8 enunciada anteriormente,
porque al margen de los intereses personales de algún propietario o
gerenciador, el club de fútbol es, esencialmente, maximizador de
utilidad (Sloane P. J., 1969).

Para concluir este breve repaso sobre la Economía del Deporte, se
revisarán los aportes teóricos de Sloane (1971), quien es el primero en
abordar los aspectos económicos que rodean al fútbol. Su principal
aporte consiste en introducir al club de fútbol como un maximizador de
utilidad. Esta función de utilidad $U( \cdot )$, según el autor, depende
del éxito deportivo ($E$), la asistencia de los espectadores ($A$), la
\`\`salud\'\' de la liga ($S$) y los beneficios económicos ($\pi$),
donde estos últimos deben ser no negativos (Rodriguez, 2012).
Formalmente, el objetivo del club vendría dado por

  -----------------------------------------------------------------------
  $$\max{U(E,A,S,\pi)\ \ }\ s.a\ \ \pi \geq 0$$              **1.1**
  ---------------------------------------------------------- ------------

  -----------------------------------------------------------------------

Posteriormente, el supuesto de los beneficios no negativos se ha
flexibilizado permitiendo que sean negativos, siempre y cuando no
comprometan la sostenibilidad del club.

Esta nueva restricción debe ser comprendida en un contexto más amplio.
Porque si bien el objetivo principal del club de fútbol es maximizar
victorias, y estas a su vez incrementan los beneficios por la vía de las
asistencias a los estadios, merchandising y otros ingresos; también la
lucha salarial por contratar mejores jugadores para obtener victorias
produce una disminución de los beneficios que puede volverlos negativos.
En este caso, pueden permitirse temporalmente, siempre y cuando no se
ponga en riesgo la supervivencia de la institución deportiva. Como
sostienen Szymanski y Smith (1997, p. 149), el incremento de la
competencia es un juego de suma negativa.

## El mercado de transferencias

Previo al desarrollo de los modelos específicos de determinación de los
precios, es importante entender cuál es la naturaleza del mercado de
transferencias de jugadores en la Economía del Deporte. Una primera
pregunta que puede surgir en este sentido es ¿Por qué y para qué se
venden y se compran jugadores de fútbol como si fuese un activo más en
propiedad de la firma cuando en ningún otro oficio o profesión se da
este fenómeno?

Para responder a esta pregunta es necesario remontarse a los inicios del
fútbol en Inglaterra, donde los encargados de establecer la normativa de
traspasos de jugadores tomaron un sistema similar a la cláusula de
reserva del béisbol norteamericano: *retain and transfer system*
(sistema de retención y traspasos).

Este sistema, consistía en impedir la movilidad de los jugadores,
otorgando al club apoderado, pleno poder para disponer del futuro del
jugador. Los jugadores, solo podían abandonar el club si la entidad
deportiva los dejaba en libertad de acción. Se requería de dicho
consentimiento, aun con el contrato vencido. Además, los clubes poseían
la libertad de renovar el contrato del jugador en condiciones menos
favorables, o la ventaja de colocarlos en una lista de transferibles sin
goce de sueldo.

Davenport (como se citó en la nota al pie 59 de Cardenal Carro, 1996)
cuenta la experiencia ocurrida en el béisbol:

> \... la Liga de Béisbol nació en Estados Unidos bajo un régimen de
> libertad de mercado de trabajo y en el campeonato de 1869 los
> Cincinnati Red Stockings ganaron los 57 partidos de que constaba, a la
> constante subida de los salarios en la disputa por los deportistas se
> correspondía el completo desinterés del público por la ausencia de una
> verdadera competición; ni un solo Club logró beneficios económicos, y
> solo a partir de 1880, con la introducción de la Cláusula de Reserva
> --- similar al derecho de retención europeo \-\-- se produjo el
> despegue deportivo y financiero de la que ha llegado a ser la mayor
> liga del mundo (Collusive Competition in Major League Baseball: Its
> Theory and Institutional Development, American Economist, 1969); algo
> similar ocurrió en el baloncesto con los Celtics, y la solución que
> adoptó la Liga en este caso fue dispersar los deportistas de ese club
> e introducir las consiguientes restricciones en el mercado de trabajo
> (Berry et al., Labor relations in professional sports, Auburn House
> Publishing Company, Dover, Massachussets, Londres, 1986, p. 154).

En efecto, el objetivo reglamentación es lograr cierto equilibrio en la
competencia para favorecer el atractivo de esta, lo que en el ámbito de
la Economía del Deporte se define como *balance competitivo*. Una
competencia balanceada implica menor grado de previsibilidad en los
resultados, y esto incentiva el interés de los consumidores. Otro efecto
buscado en esta reglamentación es proteger la economía de los equipos
pequeños, dándoles la posibilidad de tener un rédito económico por sus
mejores jugadores, permitiéndoles cancelar deudas, realizar inversiones
y/o reemplazar al jugador vendido.

En el fútbol, el sistema de retención y traspasos se aplicó por primera
vez en Inglaterra, en el año 1893 logrando resultados similares al
béisbol en lo que corresponde a la estabilización de la competencia,
gracias a la rigidez en las plantillas. El despegue económico, es
atribuible a la identificación (fidelización y fanatismo) de los
aficionados que, en cierta medida, comienzan a ser consumidores
exclusivos de una marca (equipo) dentro de un mercado restringido por la
localización. En consecuencia, puede considerarse que los consumidores
son demandantes de un bien *casi* monopólico.

El proceso de inscripción de los jugadores es fundamental para asegurar
la pureza de la competición, al evitar que los jugadores puedan ser
alineados simultáneamente por varios equipos en una misma temporada
(Gerrard, 2002). Así, sólo pueden participar en una competición, los
jugadores inscritos en la correspondiente federación. Además, cada
jugador sólo puede estar inscrito por un único club. A partir de ese
momento, el club es poseedor de los derechos federativos del jugador, lo
que le permitirá alinearlo en las competiciones en las que éste
participe durante el período de vigencia de la inscripción.

En resumen, como sostiene, Davenport (1969) la cláusula de reserva creó
un nuevo mercado: el de los contratos de jugadores. Los clubes se
convirtieron en compradores y vendedores de los servicios de un jugador.
A partir de ese momento, se compran y venden jugadores porque se puede
poseer un derecho de exclusividad sobre el uso de su fuerza de trabajo
mientras el contrato se encuentre en su periodo de vigencia. Este
derecho es una mercancía comerciable.

Cabe aclarar que, dentro del Derecho, existe otra corriente que postula
que el derecho de los clubes es sobre el *pase* o *ficha* del jugador,
siendo esta la que otorga a su vez una serie de prerrogativas, como la
de firmar un contrato con el jugador en las condiciones que libremente
acuerden. Sin embargo, esta postura excluye la situación de los
futbolistas amateur. En cualquier caso, no es el objetivo de este
trabajo resolver un debate propio del derecho, a los efectos económicos,
la situación es la misma.

Una vez presentado el *porqué* de las transferencias de los jugadores de
fútbol, el siguiente paso es definir el *para qué* de las mismas.
Carmichael y Thomas (1993) sostienen al respecto: El objetivo principal
de cualquier mercado de transferencia formal debe ser doble: (1)
facilitar y organizar la adquisición e intercambio de jugadores por los
clubes para permitir la reconstitución de los equipos con el objetivo de
aumentar las fortalezas de juego y mejorar el rendimiento del equipo; y
(2) para facilitar el movimiento de jugadores entre clubes en su
búsqueda de mejores oportunidades, mayores ganancias o mayor
satisfacción laboral.

Esta visión abre camino a dos vías de investigación, la primera es la
referente a la determinación de los precios de los futbolistas
fundamentada en las motivaciones de los equipos de fútbol; la segunda
vía alude a la determinación de los salarios de los jugadores de fútbol,
como componente económico fundamental en la relación entre la
satisfacción del jugador y su poder de negociación respecto a los
equipos. El desarrollo de este trabajo está enfocado a la primera de
estas.

Adicionalmente, se puede mencionar una tercera motivación que podría
considerarse propia del fútbol español o más específicamente de todas
las ligas de fútbol que compartan una reglamentación contable similar.

En dicho país, el Plan General de Contabilidad 2007 y Normas de
Adaptación al PGC 90 a las Sociedades Anónimas Deportivas (SAD) del año
2000 solo reconoce los derechos federativos de los futbolistas
adquiridos a terceros porque se pueden medir con fiabilidad, mientras
que los surgidos de las divisiones inferiores no pueden medirse de
manera fidedigna por carecer de precio de adquisición (Lozano F. M.,
2016). Teniendo en cuenta que la capacidad crediticia de los clubes
depende principalmente de su patrimonio, esto representa una desventaja
para aquellos clubes (SAD para España) que posean un plantel con gran
cantidad de canteranos.

Si bien esta problemática pudo haber sido reducida por la implementación
del *fair play* financiero al fijar topes de incorporación, dicho
crédito puede ser utilizado para otras inversiones (ampliaciones del
estadio, mejoras edilicias, etc.). Por tanto, algunos clubes pueden
adaptarse a dicha normativa, comprando y vendiendo jugadores de similar
calidad (no indispensables) para reflejar el verdadero valor de su
plantel.

Desde el punto de vista formal, la determinación de los precios de los
futbolistas suele modelarse siguiendo dos enfoques
teóricos-metodológicos. El primero es el utilizado por Carmichael y
Thomas (1993) conocido como *bargaining problem* (enfoque de negociación
de dos partes), que consiste en el estudio de la distribución del
excedente que pueden generar dos agentes conjuntamente a través de la
solución de negociación de Nash. Estos modelos tienen más relevancia en
la etapa previa al caso Bosman, debido a que los modelos de negociación
destacan el poder de los equipos compradores y vendedores,
operacionalizado en las características financieras, comerciales y de
rendimiento de los clubes, junto con las características de los
jugadores y sus estadísticas de rendimiento. Es importante destacar que
los efectos de esta favorecieron la movilidad de los jugadores de
fútbol, disminuyendo el poder que los clubes ejercen sobre su libertad
laboral.

El segundo enfoque plantea al mercado de transferencias como un mercado
competitivo, argumentando que a partir de la disminución del poder
monopsónico de los equipos, se consolidó un mercado de muchos
compradores y vendedores en el cual la información del rendimiento y las
características de los jugadores es fácilmente observable. Esta
propuesta la inician Szymanski y Smith (1997) luego de modelar la
estructura del mercado del fútbol inglés. Los autores sostienen, además,
que empíricamente ambos enfoques son difíciles de distinguir.

Cuando el mercado analizado es competitivo, los productos se diferencian
por la calidad y, además, poseen un número considerable de atributos. El
*modelo de precios hedónicos* constituye el modelo más utilizado en la
literatura empírica. El mismo propone que los precios de equilibrio en
un mercado con productos diferenciados pueden expresarse como una
función de los precios implícitos de los atributos o características que
el bien en cuestión posee.

Altman (2013) (fundador de North Yard Analytics) realiza una crítica muy
interesante a la idea de que el mercado de jugadores de fútbol es un
mercado competitivo y eficiente, como postulan Kuper y Szymanski (2012)
en *Soccernomics*. Puntualmente, Altman critica la correspondencia entre
calidad y precio que implicaría asumir tales características
competitivas del mercado. Refiriéndose a lo dicho por Kuper y Szymanski,
sostiene que varias de las declaraciones de los autores son erróneas.
Para los mejores jugadores, que ganan salarios semanales de cientos de
miles de libras, hay muy pocos compradores y vendedores. Cuando solo
tres o cuatro equipos intentan comprar los servicios de tres o cuatro
jugadores principales, lo más probable es que la eficiencia sufra. Esto
parece ser un argumento sólido respecto de seguir en la línea de los
modelos de negociación antes mencionados, y en especial para los
*jugadores top*.

Este debate sobre la correspondencia del supuesto de mercado competitivo
se inicia junto con los estudios del mercado de transferencias.
Carmichael y Thomas (2000) en respuesta a la propuesta de mercado
competitivo de Szymanski y Smith (1997) sostienen un enfoque de
negociación debido a que la situación en la cual los agentes son escasos
y además poseen poder de mercado, es apropiado destacar dichas
características en la determinación del precio y no dejar librado a la
posibilidad de que el precio resultante sea un reflejo de la calidad de
este tipo de jugadores. La negociación de pequeños números entre clubes
es relevante porque ni los clubes ni los jugadores son homogéneos: En un
caso extremo, un club en particular puede tratar de adquirir un jugador
en particular, en cuyo caso la situación es de monopolio bilateral. Una
situación más probable es cuando un jugador de un club en particular es
buscado por varios equipos, en cuyo caso el club vendedor tiene un grado
de poder de monopolio (Carmichael y Thomas, 2000, p. 4).

Además, puede añadirse otra crítica de Altman (2013) al modelo
competitivo de Kuper y Szymanski (2012), los clubes pueden no pagar a
los jugadores solo para ganar juegos. En el pasado, los clubes
supuestamente han traído jugadores a sus filas para vender camisetas (si
eran famosos y estaban al final de su carrera) o para abrir nuevos
mercados. Más adelante sostiene que hay muchas razones para sospechar
que el mercado para los jugadores está lejos de ser eficiente para
traducir el dinero en resultados. Un ejemplo de esto en Argentina es el
intento de Boca Juniors por captar el público japonés incorporando a la
estrella de la *J1 League* Naohiro Takahara en el año 2001.

En este sentido, hay que determinar qué se entiende por eficiencia al
momento de contratar un futbolista. En un entorno de maximización de
victorias, es probable que este tipo de contrataciones no se consideren
eficientes. Sin embargo, en un entorno maximizador de utilidad (Ecuación
1.1), si pueden considerarse eficientes.

Esto quiere decir que el término *eficiencia* empleado económicamente,
puede ser más extenso que el deportivo, porque lo incluye junto con las
expectativas de ingresos futuros. Dicho de otra manera, no se trata solo
de transformar dinero en resultados, sino de dinero en más dinero, sea
por mejores resultados, por la expansión de la demanda, por la venta de
merchandising, etc. Es decir, definir si prevalece la noción de la firma
de Sloane (1971), sobre la de Rottenberg (1956).

Finalmente, se ha definido que para este trabajo se seleccionará el
modelo de precios hedónicos con la incorporación de variables que
capturen el poder de negociación. El objetivo de esta decisión es por la
potencia del enfoque hedónico para modelar mercados con productos
diferenciados vía atributos, como es en el caso de los jugadores de
fútbol, incorporando además variables de poder de negociación por
considerar certeras las críticas respecto a que los mercados no son lo
suficientemente competitivos.

## El modelo de precios hedónicos[^11]

Como se mencionó anteriormente, cuando el mercado analizado es
competitivo, los productos se diferencian por la calidad y, además,
poseen un número considerable de atributos, el modelo más utilizado es
el *modelo de precios hedónicos*. El mismo consiste en usar la variación
sistemática en los precios de los bienes que es atribuida a sus
características para obtener su disposición a pagar.

El principio subyacente del modelo de precios hedónicos consiste en que
cualquier producto (o servicio) representa un conjunto de
características que conforman su nivel de calidad, y es la valoración
implícita de estas características la que permite explicar el precio
final del mismo. Es decir, que los bienes son valorados por la utilidad
que brindan sus atributos.

Si bien existen antecedentes de este tipo de análisis, Lancaster (1966)
fue el primero en presentar formalmente el problema del modelo de
precios hedónicos. Sin embargo, el trabajo de Rosen (1974) es el primero
en formalizar el modelo a partir de la especificación de una la función
hedónica de precios estimable, consistente con el comportamiento de
mercado de firmas maximizadoras de beneficios y consumidores
maximizadores de utilidad. Rosen considera que existen mercados
competitivos implícitos que definen los precios sombra de equilibrio o
precios hedónicos de cada una de las características que componen un
bien. Esto permite a los consumidores evaluar las características
incluidas en su composición cuando deciden comprar el bien. Estos
precios hedónicos o implícitos son aquellos que igualan la oferta y la
demanda de los atributos del bien.

Formalmente, sea $Z$ el conjunto de $n$ atributos
($z_{1},z_{2},\ldots,z_{n}$) que forman al bien analizado ($z$). Y sea
$P(Z)$ la función de precios que se obtiene de igualar la oferta y
demanda de estos atributos. La demanda de estos atributos se deriva de
un proceso de maximización de la utilidad del consumidor representada
por $U\left( x;z_{1},\ldots,z_{n};\alpha \right)$, donde $x$ representa
un bien compuesto normalizado con precio unitario, y $\alpha$ es, según
Costanigro et al. (2012) el parámetro que representa las preferencias
del consumidor, mientras que Desormeaux y Piguillem (2003) definen al
parámetro $\alpha$ como el vector de características observables solo
por el consumidor.

A partir de la función de utilidad, se puede obtener la disposición a
pagar del bien heterogéneo por parte del consumidor, dado un nivel de
ingresos fijos *M*. Esta función debe satisfacer la condición de
maximizar

  -----------------------------------------------------------------------------------------------------
  $\max_{x,Z}U\left( x,z_{1},\ldots,z_{n};\alpha \right)$   *s.a.*   $$M = x + P(Z)$$         **1.2**
  --------------------------------------------------------- -------- ------------------------ ---------

  -----------------------------------------------------------------------------------------------------

Al resolver el problema de la Ecuación 1.2, el consumidor selecciona el
par ($Z,x$) que satisface la condición:

  ---------------------------------------------------------------------------------------------------------------------------------------------------------
  $$\frac{\partial P}{\partial z_{i}} = \frac{\partial U\text{/}\partial z_{i}}{\partial U\text{/}\partial x},\forall i = 1,\ 2,\ \ldots,n$$   **1.3**
  -------------------------------------------------------------------------------------------------------------------------------------------- ------------

  ---------------------------------------------------------------------------------------------------------------------------------------------------------

de forma tal que la tasa marginal de sustitución de cualquiera de las
características del bien heterogéneo, y del bien compuesto *x*, es igual
a la tasa a la cual puede intercambiar cada $z\_ i$ por *x* en el
mercado.

Una representación alternativa de la condición 1.3 es a partir de
definir la función de oferta (*bid function*) del consumidor,
$\theta(Z,M,u,\alpha)$, que indica la disposición a pagar de un
consumidor (WTP del inglés *willingness to pay*) para un nivel fijo de
utilidad ($u$), de ingreso ($M$) y sus preferencias ($\alpha$). En otras
palabras, representa la relación que existe entre la oferta de unidades
monetarias que el consumidor hará por $Z$ cuando una o más de sus
características cambien, *ceteris paribus* la utilidad y el ingreso.
Dado que la diferencia entre el ingreso del consumidor ($M$) y la oferta
de dinero que el consumidor hace por $Z$ (i.e. $\theta$) es igual al
gasto en el bien compuesto ($x$), se puede establecer que

$U\left( M - \theta,z_{1},\ldots,z_{n};\alpha \right) = u$, la cual
indica cómo se modifica la oferta de pago del consumidor en respuesta a
los cambios en $Z$, cuando $M$ y $u$ son constantes. Maximizando la
utilidad, se obtiene la oferta marginal que el consumidor está dispuesto
a hacer por cada $z_{i}$, esto es:

  -------------------------------------------------------------------------------------------------------------------------------------
  $$\frac{\partial\theta}{\partial z_{i}} = \frac{\partial U\text{/}\partial z_{i}}{\partial U\text{/}\partial x} > 0,$$   **1.4**
  ------------------------------------------------------------------------------------------------------------------------ ------------

  -------------------------------------------------------------------------------------------------------------------------------------

Con
$\frac{\partial^{2}\,\theta}{\partial\, x_{i}^{2}} < 0\ ,\forall i = 1,\ 2,\ \ldots,n$

Para los oferentes, se presenta un problema simétrico al de los
consumidores. Se supone, entonces, que tienen una función de costos
$C(N,\ Z;\ \beta)$, donde $N$, representa el número de bienes producidos
con características $Z$ y $\beta$ que es un parámetro que identifica a
cada oferente en función de los precios a los que se enfrenta y su
tecnología. Se asume que la función de costos es creciente y convexa en
$N$ y $Z$. La función de beneficios a maximizar queda determinada por la
diferencia entre los ingresos y costos de cada oferente, formalmente:
$\Pi(N,Z) = Np(Z) - C(N,Z;\beta)$ . Al suponer un mercado competitivo,
con muchos compradores y vendedores, se considera dada la función de
precios $P(Z)$, entonces los oferentes eligen el número de bienes $N$ y
el vector de atributos $Z$ que maximicen su función de beneficios. De
este proceso, se determina que en el óptimo cada productor ofrece bienes
en el mercado, hasta que se igualan los costos marginales de cada
atributo respecto de su precio hedónico, y se ofrecen bienes hasta que
el costo adicional del último bien compuesto se iguala a su valor
$P(Z)$. Formalmente, ambas condiciones vienen dadas por:

  ----------------------------------------------------------------------------------------
  $$\frac{\partial P}{\partial z_{i}} = \frac{\partial C}{\partial z_{i}}$$   **1.5**
  --------------------------------------------------------------------------- ------------
                                                                              

  $$P(Z) = \frac{\partial C}{\partial N}$$                                    **1.6**
  ----------------------------------------------------------------------------------------

Equivalente a la función *bid* de oferta del consumidor, la función de
oferta de la firma

$\phi = \phi(Z,\Pi;\beta)$, indica el precio al cual el oferente está
dispuesto a vender (WTA) el bien diferenciado $Z$, manteniendo fijos los
beneficios a un determinado nivel $\Pi$. A partir de ella, se puede
definir la función de oferta $\Pi = N\phi - C(N,Z;\beta)$. En el óptimo
de esta función, se igualan el precio marginal que la firma está
dispuesta a aceptar por cada característica $z_{i}$, con su respectivo
costo marginal unitario de bienes diferenciados, i.e.

  --------------------------------------------------------------------------------------------------------
  $$\frac{\partial\phi}{\partial z_{i}} = \frac{\partial C\text{/}\partial z_{i}}{N} > 0,$$   **1.7**
  ------------------------------------------------------------------------------------------- ------------

  --------------------------------------------------------------------------------------------------------

Con
$\frac{\partial^{2}\phi}{\partial z_{i}^{2}} > 0,\forall i = 1,\ 2,\ldots,n$

Por el supuesto de mercado competitivo, los consumidores son tomadores
de precios, por tanto, el óptimo se produce en la función de precios
$p(Z)$ de forma tal que

  --------------------------------------------------------------------------------------------------------------
  $$\theta\left( Z^{*},u^{*},M;\alpha \right) = P\left( Z^{*} \right)$$                             **1.8**
  ------------------------------------------------------------------------------------------------- ------------
  $$\frac{\partial\phi}{\partial z_{i}} = \frac{\partial P\left( Z^{*} \right)}{\partial z_{i}}$$   **1.9**

  --------------------------------------------------------------------------------------------------------------

con $i\  = \ 1,\ \ldots,\ n$, donde la función de oferta *bid* del
consumidor es tangente a la función de precios hedónicos para todas las
dimensiones de $Z$.

Para los oferentes, el óptimo tiene condiciones análogas, esto es

  --------------------------------------------------------------------------------------------------------------
  $$\phi\left( Z^{*},\Pi^{*},\beta \right) = P\left( Z^{*} \right)$$                                **1.10**
  ------------------------------------------------------------------------------------------------- ------------
  $$\frac{\partial\phi}{\partial z_{i}} = \frac{\partial P\left( Z^{*} \right)}{\partial z_{i}}$$   **1.11**

  --------------------------------------------------------------------------------------------------------------

Cuando al menos un comprador y un vendedor cumplen la condición de
equilibrio

  ------------------------------------------------------------------------------------------------------------------------------
  $$P\left( Z^{*} \right) = \theta\left( Z^{*},u^{*},M;\alpha \right) = \phi\left( Z^{*},\Pi^{*};\beta \right),$$   **1.12**
  ----------------------------------------------------------------------------------------------------------------- ------------

  ------------------------------------------------------------------------------------------------------------------------------

se produce una venta. Es decir, que cuando se observa una venta, se ha
producido una coincidencia en la disposición marginal a pagar del
consumidor y la disposición marginal a vender del vendedor para todas
las características de un bien $z\_ i$ determinado.

Entonces, al rastrear el lugar geométrico de los puntos de tangencia
entre la oferta del comprador y la curva de oferta del vendedor, y
$P(Z\_ i)$, se representa un conjunto de una familia de funciones de
valor (Figura 3). Dada una distribución estable de preferencias y
tecnología en la población de consumidores y empresas que participan en
un mercado, la función de precio hedónico puede representarse como una
función exclusiva de los atributos del producto; es decir

  -----------------------------------------------------------------------
  $$P(Z) = P\left( z_{1},z_{2},\ldots,z_{n} \right)$$        **1.13**
  ---------------------------------------------------------- ------------

  -----------------------------------------------------------------------

  -----------------------------------------------------------------------
  []{#_Ref197510592 .anchor}Figura 3\
  Modelo de precios hedonicos de equilibrio
  -----------------------------------------------------------------------
  ![](./image3.png){width="4.739583333333333in" height="3.5625in"}

  -----------------------------------------------------------------------

Se puede notar que si especificar una determinada función de utilidad y
de costos, con solo asumir las condiciones de regularidad de dichas
funciones, se obtiene una función regular para $P(Z)$ de equilibrio,
pero sin ninguna forma funcional explícita. Por ende, en la
representación empírica se le debe suponer una cierta relación
funcional, que, comúnmente, es alguna función conocida de una
combinación lineal de los atributos más una perturbación estocástica. En
el capítulo metodológico se especificará dicha función para el
tratamiento empírico de este trabajo.

### Precios hedónicos con poder de negociación

La incorporación de las variables que identifican a los equipos tiene su
fundamento principalmente en Carmichael (2006) quien sostiene que el
enfoque de negociación es apropiado en la medida en que el mercado de
transferencias no es verdaderamente competitivo porque se caracteriza
por: (a) la incertidumbre que conduce al riesgo y la incompletitud
contractual, o (b) la negociación de pequeños números de agentes
asociada con la especificidad de la transacción de forma tal de que la
identidad es importante.

Cuando esto último sucede, las partes que intervienen en la negociación
de un traspaso pueden afectar los precios, lo que invalida los
resultados de los modelos de precios hedónicos estándar (Feenstra,
1995). La consecuencia fundamental de esta imperfección del mercado es
la perdida de eficiencia de los precios de transacción con su
correspondiente perdida de bienestar general.

Harding, Rosenthal, y Sirmans (2003) explican que cuando los mercados
son débiles, es decir, existen pocos compradores y vendedores, hay pocos
sustitutos para el bien heterogéneo, y el excedente no se reduce a cero
debido a la ausencia de ingresos de nuevos compradores y vendedores.
Esto produce que el excedente siga siendo positivo y su distribución
depende del poder de negociación de los agentes involucrados (Figura 4).
Para introducir este poder de negociación al análisis de los bienes
heterogéneos, se adiciona un término $B\_ j$, que incorpora el efecto de
la negociación en una operación $j$ determinada, respecto de su valor de
mercado. Entonces, cuando el término $B\_ j$ tiene valores positivos, el
poder de negociación es favorable al vendedor (al ejercer un peso
alcista sobre el precio), mientras que valores negativos representan un
poder de negociación favorable al comprador (al influenciar con un
descuento sobre el precio).

  -----------------------------------------------------------------------
  []{#_Ref197512534 .anchor}Figura 4\
  Modelo de precios hedónicos de equilibrio con excedente por distribuir
  -----------------------------------------------------------------------
  ![](./image4.emf){width="4.6875in" height="3.504159011373578in"}

  -----------------------------------------------------------------------

Cotteleer, Gardebroek, y Luijt (2008) diferencian las características
personales del poder negociación. Esto se debe a que algunas
características personales pueden influir en el poder de negociación,
mientras que otras afectan directamente a las curvas WTP y WTA, y en
consecuencia a los precios de mercado. Además, suponen que los precios
sombra no interactúan con la forma del mercado ni con las
características de los agentes, produciendo cambios paralelos en la
función de precios hedónicos. Esta posición, también es asumida por
Harding et al. (2003) y en términos más generales por Feenstra (1995),
cuando sostiene que los márgenes precio-costo en modelos de precios
hedónicos bajo competencia imperfecta, no interactúan con otras
variables explicativas del modelo.

Para el caso del poder de mercado, Cotteleer et al. (2008) utilizan las
*proxies* del número de potenciales compradores y vendedores en un
mercado cerrado, con el objetivo de determinar el excedente generado por
la ausencia de competidores. Mientras que Harding et al. (2003)
simplemente sostienen que este excedente existe, sin poner el énfasis en
su determinación. Para este trabajo se descarta determinarlo a través
del poder de mercado, porque según Carmichael y Thomas (2000) existen
dos tipos particulares de transferencias en lo que respecta a poder de
mercado: Un monopolio bilateral cuando un solo equipo se interesa por un
jugador en particular o la existencia de cierto poder monopólico por
parte del vendedor cuando un jugador de un club en particular es buscado
por varios equipos. El número de equipos interesados a priori se
desconoce, y durante el proceso de negociación solo es realmente
conocido por el club vendedor. Entonces, se espera que el efecto del
número de equipos involucrados en una transferencia sea poco relevante y
tenga más peso la identidad de estos.

Se descartan estrategias como las utilizadas por el CIES Football
Observatory (2018), el cual establece una categorización de potenciales
compradores, pero solo para algunas ligas de Europa. Debido a que estos
potenciales compradores no se corresponden con un interés real de esos
clubes, no es correcto tenerlos en cuenta. No es el objetivo principal
de esta investigación medir el poder de mercado, sino el poder de
negociación, por lo cual, se decide seguir el modelo de Harding et al.
(2003), pero incorporando algunos de los aportes de Cotteleer et al.
(2008).

Según Harding et al. (2003), cuando la función de precios hedónicos es
no lineal, las diferencias en el poder de negociación entre compradores
y vendedores pueden afectar implícitamente sobre los precios sombra. Por
tanto, para que exista independencia entre las variables de negociación
y los precios implícitos, es fundamental asumir el supuesto de
linealidad. De esta forma, para una determinada transacción j (i.e. una
cierta transferencia de un jugador) en el mismo intervienen tanto sus
atributos (vía función **¡Error! No se encuentra el origen de la
referencia.**) como un efecto del poder de negociación en dicha
transacción $j$ ($B_{j}$, que puede asumirse que tiene una relación
lineal con el precio de la forma:

  ----------------------------------------------------------------------------------
  $$P_{j}\left( Z_{j},B_{j} \right) = P\left( Z_{j} \right) + B_{j}$$   **1.14**
  --------------------------------------------------------------------- ------------

  ----------------------------------------------------------------------------------

donde $Z_{j}$ es el vector de los atributos del jugador que se está
transfiriendo en la operación j, $P\left( Z_{j} \right)$ es la función
hedónica general sin poder de negociación (Ecuación 1.13) y los $B_{j}$
reflejarán implícitamente el resultado esperado de la negociación entre
vendedores y compradores que intervienen en la transacción j, la que
puede interpretarse como un desvío de su valor de mercado como producto
de una asimetría en el poder de negociación. Por lo tanto, los valores
positivos para $B_{j}$ implican beneficios de negociación superiores al
promedio para el vendedor $j$, y los valores negativos implican
beneficios de negociación superiores al promedio para el comprador $j$.
Como se explicará en la sección metodológica, este poder de negociación
es introducido a partir de variables de los clubes que intervienen en la
compraventa de un determinado jugador como pueden ser el valor del
plantel, la posición en la tabla y la división de la temporada anterior,
entre otras.

Del modelo de Harding et al. se adopta la exclusión del poder de mercado
y se elimina el supuesto de simetría en los efectos de los parámetros de
poder de negociación. En su lugar, a partir de lo establecido por
Cotteleer et al., se permite la asimetría de los coeficientes de los
parámetros de negociación. Este cambio tiene su fundamento en lo
establecido por (Carmichael y Thomas, 1993), quienes sostienen que no es
realista tratar simétricamente a compradores y vendedores en una
situación de negociación de transferencias de futbolistas. Además, para
impedir que la correlación entre los atributos no observados y los
parámetros de negociación arrojen estimadores sesgados, se incorpora de
Cotteleer et al. (2008) el supuesto de independencia entre las
características observadas y no observadas. Estos dos supuestos permiten
facilitar la interpretación de los coeficientes de las características
de negociación obtenidos y asegurar que los estimadores obtenidos son
insesgados.

## Síntesis del capítulo

En el presente capítulo se desarrollaron algunos puntos relacionados con
el tratamiento económico del deporte, tanto a nivel general como del
fútbol en particular. Luego de la revisión de los antecedentes, se
presentaron brevemente las once anclas sobre las que se sentaron las
bases de la Economía del Deporte, introduciendo dos de los conceptos más
importantes de esta área de estudio: el Balance Competitivo y El
Principio de Invarianza. Seguidamente, se revisaron algunas
peculiaridades de la Economía del Deporte, sobre las que la utilización
de la teoría económica general puede producir conclusiones erróneas. Se
pueden destacar tres de estas cuestiones planteadas por Neale (1964): La
paradoja de Louis-Schmelling, el producto conjunto invertido y la
relación entre la competencia económica-competencia deportiva y los
ingresos.

La paradoja de Louis-Schmelling establece que para la firma deportiva la
existencia de competencia es esencial para la existencia del mercado,
por oposición al mercado monopólico como situación ideal para una firma
de un mercado de bienes no deportivos. La relación de los ingresos con
los dos tipos de competencia es intuitiva dentro de la teoría económica,
la excesiva competencia económica entre las firmas produce una caída en
los ingresos, la novedad se encuentra en la incorporación de la
competencia deportiva como generadora de ingresos. Por otro lado, el
producto conjunto invertido, incorpora un nuevo tipo de producción donde
son necesarias al menos dos firmas que compiten entre sí para la
producción de un bien deportivo.

En lo que respecta al fútbol, se han analizado los productos que ofrece
el deporte al mercado y se los ha clasificado según su objeto social, y
se han descrito los tipos de ingresos que estos productos generan en las
entidades deportivas (Lozano y Gallego, 2011). Además, se presentó la
función de utilidad de los clubes de fútbol, representativa de los
objetivos que puede esperarse que tengan las entidades deportivas: éxito
deportivo, asistencia de espectadores, la "salud" de la liga y los
beneficios económicos que eventualmente pueden ser negativos.

El Mercado de transferencias es una institución particular del fútbol
que se caracteriza por comercializar las fichas o pases de los
futbolistas federados. En este capítulo se intentó responder al *porqué
y para qué* de estas transferencias. La posesión de un derecho de
exclusividad del uso de una fuerza de trabajo especializada, el proceso
de inscripción y la necesidad de estabilizar los planteles son un
aspecto fundamental para la existencia del mercado, pero estos aspectos
también están presentes en otros deportes que no poseen un mercado como
mecanismo de asignación de jugadores, aun teniendo una evolución similar
al fútbol en cuanto a sus normas, por lo que puede deducirse que existen
otros factores adicionales. Los objetivos de las transferencias son
facilitar y organizar la adquisición e intercambio de los jugadores para
que los clubes puedan reconstruir sus equipos para mejorar su
rendimiento, y que los jugadores obtengan mayores ganancias y
satisfacción laboral.

Además, se presentó una breve discusión sobre la metodología empleada en
el estudio de estas con el objetivo de fundamentar teóricamente la
selección del modelo empleado en este tipo de estudios. Particularmente,
se presentó el debate entre el modelo de precios hedónicos para los
mercados competitivos (Szymanski y Smith, 1997; Kuper y Szymanski, 2012)
y el enfoque de negociación de dos partes, (Carmichael y Thomas, 1993;
Carmichael, 2006) que estudia la distribución del excedente cuando las
características de los agentes son importantes en la determinación de
los precios.

Finalmente, se abordó el marco teórico de los modelos de precios
hedónicos. Desarrollando los problemas del consumidor y del oferente
hasta presentar el equilibrio de mercado. Dentro de este marco, se
especificó el modelo general de precios hedónicos con poder de
negociación que será abordado en la investigación. Esta decisión se debe
a que las partes de negociación son importantes. En otras palabras, los
agentes influyen en los precios cuando la negociación se da entre pocos
oferentes y demandantes cuando esta circunstancia es característica
propia del tipo de transacción.

Capítulo 2

# Metodología

##  Introducción

En el presente capítulo se expondrán los materiales y métodos utilizados
en la investigación para estimar los precios implícitos de las
características de los jugadores cómo así también el efecto sobre los
precios de las transferencias de variables que aproximan el poder de
negociación de los clubes.

En primer lugar, se presenta la especificación empírica general del
modelo de precios hedónicos presentados de forma general en las
Ecuaciones 1.13 y 1.14. Luego se describirán los datos y las variables
utilizadas que se incluirán como predictores de los precios negociados
en el mercado de pases. Posteriormente, se presentarán los modelos
econométricos definitivos y los métodos de estimación.

## Especificación empírica

Con el objetivo de analizar los determinantes de los precios de
transferencia, es necesario definir los conjuntos de variables
utilizadas en la investigación y la estrategia utilizada para modelar
las mismas. Considerando que tenemos una muestra $N$ de pases de
jugadores en un total $T$ temporadas, la variable dependiente de interés
($P_{it}$) es el precio de transferencia del equivalente al 100% del
pase del futbolista i en la temporada t, con $i = 1\ldots,n_{t}$,
$t = 1,\ldots,T$ y $n_{t} = 1,\ldots,N$. Sean $Z_{it}$ los atributos o
características del jugador i transferido en t, incluyendo su historial
de estadísticas de juegos, vamos a suponer para las Ecuaciones 1.13 y
1.14 el siguiente modelo lineal semilogarítmico

  -----------------------------------------------------------------------------------------------
  $$\ln\left( P_{it} \right) = \alpha + \beta^{T}Z_{it} + B_{it} + \epsilon_{it}$$   **2.1**
  ---------------------------------------------------------------------------------- ------------

  -----------------------------------------------------------------------------------------------

donde $\alpha$ es un término en común (intercepto), $\beta$ es el vector
de precios sombra de los atributos de los jugadores, $B_{it}$ es efecto
del poder de negociación durante el pase $i$ en $T$ y $\epsilon_{it}$ es
un término de perturbación aleatoria que suponemos independiente e
idénticamente distribuido.

La especificación semilogarítmica para modelos hedónicos es quizás la
más utilizada por algunas ventajas metodológicas asociadas a dicha
transformación de la variable respuesta en un modelo lineal. En términos
económicos, permite una interpretación fácil y directa de los parámetros
estimados, en el sentido de que los precios implícitos quedarían
expresados a partir de su representación como cambios porcentuales en el
precio final del pase. En términos estadísticos, la no-negatividad de
los precios y la asimetría en la distribución de estos resulta ser
consistente con una distribución log-normal (Costanigro et al., 2012).

También, siguiendo el criterio del rango de la variable para analizar
transformaciones, al tener valores de una variable que abarcan más de un
orden de magnitud y es estrictamente positiva, es probable que la
sustitución de la variable original por su logaritmo sea útil. Por el
contrario, si el rango de una variable es considerablemente menor que un
orden de magnitud es poco probable que cualquier transformación de esa
variable sea útil (Weisberg, 2005).

Para el efecto del poder de negociación $B$ vamos a suponer un modelo
paramétrico, donde dicho efecto dependerá linealmente de las
características de los vendedores $B^{v}$ y de los compradores $B^{c}$ y
de un efecto aleatorio no observable $e_{B}$, de la forma

  -----------------------------------------------------------------------
  $$B = \gamma^{T}B^{v} + \delta^{T}B^{c} + e_{B},$$         **2.2**
  ---------------------------------------------------------- ------------

  -----------------------------------------------------------------------

siendo $e_{B}$ independiente de $\epsilon_{it}$. Los vectores $\gamma$ y
$\delta$ son los coeficientes que determinan el poder de negociación de
los vendedores y compradores respectivamente.

Por lo tanto, reemplazando 2.2 en 2.1, la especificación empírica vendrá
dada por

  ---------------------------------------------------------------------------------------------------------------------------------------
  $$\ln\left( P_{it} \right) = \alpha + \beta^{T}Z_{it} + \gamma^{T}B_{it}^{v} + \delta^{T}B_{it}^{c} + \varepsilon_{it}$$   **2.3**
  -------------------------------------------------------------------------------------------------------------------------- ------------

  ---------------------------------------------------------------------------------------------------------------------------------------

donde $\varepsilon_{it} \doteq \epsilon_{it} + e_{B}$ .

Debido a que algunas variables pueden presentar las características que
expone Weisberg (2005), como puede ser el valor de mercado del plantel,
en dichos casos se aplicara una especificación log-log.

  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  $$\ln\left( P_{it} \right) = \alpha + \beta^{T}Z_{it} + \lambda_{v}\ln\left( B_{it}^{v} \right) + \lambda_{c}\ln\left( B_{it}^{c} \right) + \varepsilon_{it}$$   **2.4**
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------- ------------

  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

En este caso, $\lambda$ representaría la elasticidad precio-valoración
de mercado del plantel, pues

$$\lambda_{k} = \frac{\partial\ln\left( P_{it} \right)}{\partial\ln\left( B_{it}^{k} \right)},$$

y muestra el cambio porcentual en el precio negociado de un pase ante un
cambio del 1% de la valoración de expertos al plantel.

Cuando la variable dependiente es observada de manera incompleta, ya sea
por censura, truncamiento o cuando sea completamente observada, pero sea
a través de una muestra autoseleccionada, ésta no es representativa de
la población y la estimación por MCO obtiene parámetros sesgados e
inconsistentes. En el mercado de traspasos de jugadores de fútbol, este
fenómeno sucede de la misma forma.

Para suplir el problema de la muestra autoseleccionada, podemos optar
por no extender las conclusiones de los jugadores transferidos a los no
transferidos, y limitar la interpretación de los resultados a este
subconjunto. Una posible solución sería aplicar el modelo de estimación
en dos pasos de Heckman, pero la base de datos *scrapeada* no dispone de
estadísticas de jugadores no transferidos.

Además, como sostienen Carmichael et al. (1999), para enfoque de
negociación como el seleccionado en este trabajo, el enfoque es
inconsistente con el modelo de selección-corrección, ya que el enfoque
de negociación contiene variables para las características de los clubes
compradores. Dichas variables no pueden incluirse en un modelo de
corrección de selección, ya que todas las variables en la ecuación del
precio (excepto el termino de corrección de selección) deben incluirse
en la ecuación de probabilidad de transferencia. Este último contiene
información sobre todos los jugadores, no solo los transferidos, y para
aquellos jugadores no transferidos, no existe información sobre el club
comprador.

En este mismo sentido, se puede plantear el problema análogo para los
jugadores transferidos con y sin precio. Restringir la muestra solo a
aquellas observaciones en las que el precio de transferencia es positivo
y realizar una estimación por mínimos cuadrados ordinarios (MCO) sobre
este subconjunto equivale a estimar un modelo truncado, ya que se
eliminan de la muestra aquellas observaciones con $y_{i} = 0$.

Si la decisión de transferir a un jugador o el precio mínimo aceptado en
una negociación está correlacionada con las características que
determinan el valor de mercado, la estimación por MCO sobre la muestra
truncada puede generar sesgo de selección. No obstante, en contextos
donde el interés está centrado exclusivamente en el mercado de jugadores
efectivamente transferidos con precio, este enfoque permite obtener
estimaciones válidas dentro de este grupo.

Se puede aplicar una estimación Tobit para incorporar a aquellos
jugadores transferidos con precio cero. De esta manera, se puede
corregir la censura de la variable predicha, ajustando por la
probabilidad de la observación del precio. Para realizar dicha
estimación, se decidió excluir los jugadores cuyo contrato está vencido
por considerar que no representan el mismo tipo de transferencia que
aquellas con contrato activo. En este último caso, se asume que el club
dio su consentimiento para que el jugador abandone la institución sin un
pago.

La utilización de la transformación logarítmica para modelar los precios
impide incorporar a los jugadores de precio cero. Esto se puede
solucionar sumando una unidad al precio. Sobre la pertinencia de
realizar este procedimiento, se puede ilustrar con un ejemplo simple que
dicha transformación de la variable es inocua para las observaciones de
precio cero, ya que, para las observaciones con precio se puede ver
fácilmente que el logaritmo natural del valor mínimo 45 249 es
aproximadamente igual al logaritmo natural de 45 250.

Supongamos que la FIFA prohíbe transferir de manera gratuita jugadores
con contrato vigente, pero no establece un precio mínimo. Los clubes
podrían eludir esta restricción sin problemas estableciendo un valor
simbólico de 1 euro que no tendría impacto en su patrimonio. Las
voluntades de los equipos serían las mismas.

A diferencia de las aplicaciones tradicionales donde la variable es
censurada en la cota, en este caso hay sospecha de que la cota sucede en
un valor positivo por encima de cero (Figura 5). No obstante, esto no es
un problema porque no se requiere observar ningún valor de $y^{*}$, solo
conocer el valor de la cota. Formalmente:

$$\ln\left( y_{i} \right) = \left\{ \begin{matrix}
\ln\left( y_{i}^{*} + 1 \right), & \text{si\ }y_{i}^{*} > L \\
0, & \text{si\ }y_{i}^{*} \leq L \\
\end{matrix} \right.\ \quad\text{con\ }L > 0$$

En dichos casos, lo correspondiente es modelar con un límite inferior
cercano y menor al mínimo observado, ya que si se censurará en cero se
estaría indicando que la censura comienza en cero y que es probable
encontrar observaciones entre el mínimo y cero. Esto no parece ser
consistente con lo observado en la Figura 5a donde hay una brecha entre
las observaciones con precio y las sin precio. Según la prueba de
normalidad de Royston (1992), no se puede rechazar que las observaciones
con precio tienen distribución normal (Figura 5b) al menos al 62%, por
lo cual parece bastante improbable que dicha brecha pueda contener
observaciones cercanas a cero.

Carson y Sun (2007) realizando simulaciones de Montecarlo, llegaron a la
conclusión que los umbrales de censura distintos de cero pueden ser una
fuente de sesgo más importante que la no normalidad o la
heterocedasticidad. Recomiendan, además, que para el trabajo aplicado es
conveniente establecer un umbral igual al mínimo de la variable
dependiente no censurada. En muestras grandes, no hay pérdida de
eficiencia en relación con conocer y utilizar el umbral correcto,
siempre que no se esté dispuesto a abandonar el supuesto de que el
umbral de censura es constante o modelarlo explícitamente.

  -----------------------------------------------------------------------
  []{#_Ref197601074 .anchor}Figura 5\
  Histograma del logaritmo del precio de las transferencias de
  futbolistas con contrato para el periodo 2021-2024
  -----------------------------------------------------------------------
  \(a\) Muestra completa

  ![](./image5.png){width="5.110121391076116in"
  height="3.406748687664042in"}

  \(b\) Precios positivos

  ![](./image6.png){width="5.068405511811024in"
  height="3.378938101487314in"}

  Fuente: Elaboración propia con datos de *transfermarkt*.
  -----------------------------------------------------------------------

Asimismo, también recomiendan que para garantizar que las observaciones
que alcanzan el valor mínimo observado se consideren observaciones no
censuradas, es posible que debamos establecer un umbral ligeramente
menor que este. En este sentido, podría sería útil usar la preferencia
por los números redondos para establecer el umbral. Sin embargo, en esta
investigación no es particularmente útil, porque la información de los
precios fue obtenida en euros, mientras que las transferencias en
Argentina, y en la mayoría de los países, se negocia en dólares. De
todas formas, se utilizará como umbral el logaritmo natural del número
redondo más cercano al mínimo.

## Datos y variables

### Datos

Para el análisis empírico, se utilizarán los datos correspondientes a
las dos temporadas anteriores al traspaso de los $N = 1484$ jugadores
transferidos desde y hacia la liga argentina de primera división durante
las siete ventanas de transferencias que van del verano europeo de 2021
al verano europeo de 2024. Los datos provienen del sitio web
*transfermarkt*. Por el análisis de los datos obtenidos de dicho sitio,
se consideran los traspasos de enero a abril, inclusive, como traspasos
de *invierno*, y todos los restantes son de *verano*, debido a las
constantes irregularidades en los calendarios y decisiones de la
Asociación del Fútbol Argentino.

La reputación de *transfermarkt* es el factor principal de su elección,
ya que la misma es de consulta tanto de aficionados, como por personas
intervinientes en las compraventas de jugadores (directores deportivos,
representantes, etc.). Lozano (2016) destaca la fiabilidad de los
precios expuestos en dicho sitio. Ha comparado al azar 5 operaciones
analizadas en estudios de caso en los cuales la información de los
costos de adquisición y de venta final había sido obtenida de manera
confidencial. Cabe destacar, que, para operaciones realizadas en los
países sudamericanos, dicha fiabilidad puede ser menor, ya que la
información disponible habitualmente es menor.

Hay que recordar que para garantizar que la excesiva competencia
económica perjudique el atractivo de la competencia deportiva, el
mercado de pases requiere que los jugadores sean transferidos en
momentos determinados y conocidos por todos, afectando lo menos posible
la competencia deportiva y logrando la mayor estabilidad posible en los
planteles. Esto se logra organizando los fichajes en dos momentos, el
inicio de la temporada y en un intervalo intermedio que permita recurrir
al mercado frente a imprevisto o a resultados desfavorables.

Por convención, debido a que se incluyen traspasos con el resto del
mundo y porque así se encuentran clasificadas en la página que es fuente
de información, se consideran las estaciones de traspaso, verano e
invierno, según las estaciones del hemisferio norte. Sin importar que,
en el fútbol sudamericano, las temporadas corresponden al año
calendario, es decir, la mayor parte de los traspasos suceden luego del
mercado de invierno europeo, en lugar del verano.

En lo que respecta a la distribución temporal de los datos, los datos se
fragmentaron en siete ventanas de transferencia. Para la temporada 2021
hay 376 traspasos, 226 de invierno y 150 de verano; para la 2022 son 352
traspasos, 266 de invierno y 126 de verano; para la 2023, 511 traspasos,
351 de invierno y 160 de verano; y para finalmente, para la temporada
2024, solo se contabilizaron los 206 traspasos de verano, ya que la
ventana de invierno todavía se encuentra abierta (Figura 6).

De los 1484 traspasos documentados inicialmente, 1116 corresponden a
jugadores con contrato activo confirmado, según se detalla en la Tabla
2. Puede observarse una tendencia creciente en la cantidad de
transferencias con contrato verificado hasta 2023, seguida por una caída
en 2024, explicada porque solo se incluye la ventana de verano de dicha
temporada. La concentración de operaciones en el mercado de invierno es
sistemática en todas las temporadas completas (2021--2023), lo que
sugiere un patrón institucional o estratégico por parte de los clubes.

  -----------------------------------------------------------------------
  []{#_Ref197614977 .anchor}Figura 6\
  Distribución de las transferencias por cuatrimestre
  -----------------------------------------------------------------------
  ![](./image7.png){width="6.102777777777778in"
  height="2.8222222222222224in"}

  Fuente: Elaboración propia con datos de *transfermarkt*.
  -----------------------------------------------------------------------

Por otro lado, la Tabla 3 muestra que solo una fracción de estas
transferencias tiene un precio positivo registrado (453 casos). Aunque
el número absoluto de transferencias con precio aumenta en 2023, el peso
relativo respecto del total de operaciones con contrato verificado sigue
siendo bajo (aproximadamente el 40%). La mayor presencia de traspasos
pagos en la ventana de invierno también se mantiene, aunque la
diferencia con el mercado de verano es menos pronunciada que en el total
de transferencias.

  --------------------------------------------------------------------------------------
  []{#_Ref197615001                                                            
  .anchor}**Tabla                                                              
  2**\                                                                         
  *Transferencias con                                                          
  contrato verificado                                                          
  por temporada y                                                              
  ventana*                                                                     
  ------------------- ---------------- ----------- -------------- ------------ ---------
                      **Temporada**    **Total**   **Invierno**   **Verano**   

                      **2021**         242         155            87           

                      **2022**         274         171            103          

                      **2023**         403         258            145          

                      **2024**         197         --             197          

                      Total            1116        584            532          

  Fuente: Elaboración                                                          
  propia con datos de                                                          
  *transfermarkt*.                                                             
  --------------------------------------------------------------------------------------

  --------------------------------------------------------------------------------------
  []{#_Ref197615051                                                            
  .anchor}**Tabla 3\                                                           
  ***Transferencias                                                            
  con precio positivo                                                          
  por temporada y                                                              
  ventana*                                                                     
  ------------------- ---------------- ----------- -------------- ------------ ---------
                      **Temporada**    **Total**   **Invierno**   **Verano**   

                      **2021**         94          59             35           

                      **2022**         107         67             40           

                      **2023**         175         98             77           

                      **2024**         77          --             77           

                      **Total**        **453**     **224**        **229**      

  Fuente: Elaboración                                                          
  propia con datos de                                                          
  *transfermarkt*.                                                             
  --------------------------------------------------------------------------------------

El criterio utilizado para definir las temporadas de las estadísticas de
rendimiento es la de los torneos completos disputados en una temporada.
Esto es, se consideran todos los torneos disputados por el jugador en la
temporada anterior al traspaso, siempre y cuando estos torneos no tengan
partidos jugados más allá de una temporada antes de la apertura de la
ventana de transferencia.

Es importante destacar, que, debido a la nomenclatura utilizada por la
web, los equipos vendedores del norte poseen temporadas del tipo
$t/t + 1$ (julio a junio), mientras que los equipos del sur $t$ (enero a
diciembre). Debido a la técnica de *scraping* utilizada, cuando el
jugador juega entre hemisferios, se corre el riesgo de sumar una
temporada y media. En concreto, la regla seguida es que, si es traspaso
de verano, se excluyen los torneos con partidos previos al 30 de junio
del año anterior. Para los traspasos de invierno, los torneos con
partidos anteriores al 31 de diciembre de la temporada anterior. De esta
forma, se acorta a, como máximo, una temporada las estadísticas
consideradas[^12].

A partir de las reglas de scraping establecidas para la fecha de fin de
contrato, se detectaron 9 jugadores con precio positivo y sin contrato
vigente que se analizaron individualmente. En la Tabla 4 se expone la
información de dichas observaciones.

  ----------------------------------------------------------------------------------------------------------------------
  []{#_Ref197616403                                                                                 
  .anchor}Tabla 4\                                                                                  
  *Observaciones                                                                                    
  obtenidas con                                                                                     
  scraping con precio                                                                               
  positivo sin                                                                                      
  contrato vigente.                                                                                 
  (Precios                                                                                          
  corrientes)*                                                                                      
  ------------------- ------------- -------------- --------------- ------------------- ------------ --------------------
  **Nombre**          **Precio**    **Vendedor**   **Comprador**   **Fecha de**        **Fecha de** **observaciones**

                      **(euros)**                                  **transferencia**   **fin de     
                                                                                       contrato**   

  Jhonatan Candia     285 000       Arsenal FC     CA Huracán      7/5/2021            30/6/2021    La tarifa es pagada
                                                                                                    al jugador, se
                                                                                                    considera salario

  Nicolás Giménez     2 050 000     CA Talleres    FC Baniyas      7/1/2021            30/6/2021    Según
                                                                                                    mercadodepases.com
                                                                                                    el jugador antes de
                                                                                                    ir a préstamo renovó
                                                                                                    hasta diciembre de
                                                                                                    2022

  Tomás Cuello        2 250 000     Club Atlético  Club Athletico  23/2/2022           31/12/2021   Según
                                    Tucumán        Paranaense                                       mercadodepases.com
                                                                                                    el jugador antes de
                                                                                                    ir a préstamo renovó
                                                                                                    hasta diciembre de
                                                                                                    2022

  Emiliano González   450 000       Club           Club Almagro    1/1/2023            31/12/2022   Según
                                    Estudiantes de                                                  mercadodepases.com
                                    La Plata                                                        el jugador antes de
                                                                                                    ir a préstamo renovó
                                                                                                    hasta diciembre de
                                                                                                    2024

  Juan Brunetta       2 350 000     CD Godoy Cruz  Santos Laguna   7/1/2023            30/6/2023    Según
                                    Antonio Tomba                                                   mercadodepases.com
                                                                                                    el club de destino
                                                                                                    ejerció la opción de
                                                                                                    compra media
                                                                                                    temporada antes

  Gastón Martirena    2 360 000     Liverpool FC   Racing Club     31/7/2023           31/7/2023    No hay información
                                    Montevideo                                                      de renovación, ni de
                                                                                                    motivo por el pago
                                                                                                    del pase con
                                                                                                    contrato vencido

  Diego Herazo        836 000       Deportes       CA San Lorenzo  2/12/2024           31/12/2023   No hay información
                                    Tolima         de Almagro                                       de renovación, pero
                                                                                                    si se sabe que el
                                                                                                    equipo vendedor lo
                                                                                                    compro antes de
                                                                                                    venderlo

  Rodrigo Piñeiro     2 320 000     Unión Española CA Vélez        28/1/2024           31/12/2023   El jugador firmo con
                                                   Sarsfield                                        el club de origen un
                                                                                                    contrato por 3 años
                                                                                                    el año anterior.

  Aníbal Leguizamón   363 000       CS Emelec      CA Belgrano     22/8/2024           30/6/2024    El 12 de agosto de
                                                                                                    2024 renueva hasta
                                                                                                    2027 con el club
                                                                                                    vendedor

  Fuente: elaboración                                                                               
  propia con datos de                                                                               
  *transfermarkt*.                                                                                  
  ----------------------------------------------------------------------------------------------------------------------

Para el caso de Jhonatan Candia, se detectó que la tarifa de
transferencia se abonó al jugador que ya tenía el pase en su poder, por
lo que se corrigió el valor a 0 por considerarlo parte del salario del
jugador. Respecto a Juan Brunetta, el club comprador ejerció la opción
de compra durante la ventana anterior, aunque se sospecha que para ser
cedido tuvo que renovar su contrato previamente. De todas formas, se
corrigió la fecha de transferencia.

Para los casos restantes, se encontraron deficiencias en el método de
*scraping* que no permite capturar la fecha real de finalización de
contrato cuando la renovación es muy cercana a la transferencia. En
todos los casos, excepto los de Gastón Martirena y Diego Herazo, se
encontró una fecha de fin de contrato válida, por lo que se reemplazaron
los valores. Para estos dos jugadores, se encontraron indicios de un
contrato vigente, pero no la fecha, por lo que se eliminó la fecha de
fin de contrato, y las variables que se construyen a partir de ella,
pero se mantuvo el indicador de contrato vigente.

### Variables

Las variables utilizadas podemos clasificarlas en 4 grupos: a) las
monetarias, b) los atributos del jugador, dados por las características
personales del mismo y sus estadísticas de rendimiento; c) Las
características de los clubes, que son tomadas a los fines de captar las
potenciales asimetrías en el poder de negociación; y d) las temporales,
para controlar por la ventana o la temporada en que se realiza el pase y
la distancia en ventanas de transferencia entre el vencimiento del
contrato y la transferencia.

A modo de síntesis, en la Tabla 5 se expone una breve descripción de las
variables utilizadas en este trabajo. En lo que sigue se presentará una
descripción más detallada de cada una de ellas.

  ------------------------------------------------------------------------------------
  []{#_Ref197682009                                                       
  .anchor}Tabla 5*\                                                       
  Definición de las                                                       
  Variables*                                                              
  ----------------------- ----------------------------------------------- ------------
  **Variable**            **Descripción**                                 **Signo
                                                                          esperado**

  **Variable                                                              
  dependiente**                                                           

  lprecio                 Logaritmo del precio declarado de la            n/d
                          transferencia deflactado a junio de 2021 más 1  
                          (equivalente al 100% del pase).                 

  **Variables                                                             
  independientes**                                                        

  edad                    Edad del jugador al momento de la transferencia \- (-)
                          (en años).                                      

  altura                  Altura del jugador (en centímetros).            \+

  comunitario             Dummy: 1 si el jugador posee ciudadanía         \+
                          comunitaria.                                    

  extranjero              Dummy: 1 si el jugador no es argentino.         ?

  delantero               Dummy: 1 si la posición principal del jugador   ?
                          es delantero.                                   

  mediocampista           Dummy: 1 si la posición principal del jugador   ?
                          es mediocampista.                               

  portero                 Dummy: 1 si la posición principal del jugador   ?
                          es arquero.                                     

  pj                      Partidos jugados en la temporada anterior.      \+

  goles                   Goles convertidos en la temporada anterior.     \+

  asis                    Asistencias realizadas en la temporada          \+
                          anterior.                                       

  amonestac               Tarjetas amarillas recibidas en la temporada    \-
                          anterior.                                       

  expuls                  Expulsiones en la temporada anterior.           \-

  goles_concedidos        Goles concedidos por el equipo en la temporada  \-
                          anterior (relevante para arqueros).             

  vallas_inv              Cantidad de vallas invictas logradas en la      \+
                          temporada anterior.                             

  rest_ventana_transfer   Cantidad de ventanas de transferencia restantes \+
                          hasta que venza el contrato del jugador con el  
                          club.                                           

  temporada               Variable continua: indica el año de la          \+
                          temporada.                                      

  verano                  Dummy: 1 si la transferencia ocurrió en la      ?
                          ventana de verano europeo.                      

  lvalor_plantel_c        Logaritmo del valor total del equipo comprador. \+

  lvalor_plantel_v        Logaritmo del valor total del equipo vendedor.  \+
  ------------------------------------------------------------------------------------

#### Variables Monetarias {#variables-monetarias .unnumbered}

La variable respuesta de interés es el precio del pase ($P$) en su
transformación logarítmica. Se utilizará el logarítmo del precio más 1
con el objetivo de incluir las transferencias de precio cero en los
modelos censurados y Tobit.

La variable precio $P$, representa la cantidad de dinero (en euros)
declarada en el sitio web *transfermarkt* por la cual los equipos de
fútbol efectivamente realizan la transferencia o el equivalente al 100%
cuando el porcentaje del pase negociado sea menor deflactado por el
índice de precios al consumidor armonizado de Eurostat[^13]. El monto
del pase se desconoce hasta el momento en el cual se realiza el pago,
por lo cual un jugador no posee precio hasta que es transferido.

En la base de datos, el valor *Libre* fue codificado como 0, mientras
que las transferencias de precio desconocido fueron codificadas como
valor faltante (*missing value*). Sin embargo, puede ser posible que
este campo represente una omisión informativa en lugar de una
transferencia gratuita. El presente análisis trata estos casos como
ceros reales, aceptando esta posible fuente de error de clasificación.

En la Tabla 6 se presentan los precios promedio deflactados de las
transferencias (P), desagregados por posición del jugador y tipo de
transferencia. La última columna *Total* muestra el promedio general del
precio por posición, calculado utilizando todas las transferencias
observadas para esa posición, sin importar el tipo de operación. De
manera análoga, la fila *Total* al pie de la tabla reporta el precio
promedio general por tipo de transferencia, considerando todas las
observaciones correspondientes a ese tipo, sin importar la posición. Es
decir, tanto las filas como las columnas de totales están ponderadas por
la cantidad de observaciones reales en cada grupo, y no representan un
promedio aritmético simple entre las celdas anteriores.

  ---------------------------------------------------------------------------------------------------------------
  []{#_Ref197683245                                                                                   
  .anchor}Tabla 6\                                                                                    
  *Precios promedio                                                                                   
  de las                                                                                              
  transferencias en                                                                                   
  el fútbol argentino                                                                                 
  por posición y tipo                                                                                 
  de transferencia en                                                                                 
  el periodo                                                                                          
  2021-2024 (en euros                                                                                 
  constantes de junio                                                                                 
  2021)*                                                                                              
  ------------------- --------------- ------------------- ----------- ------------------- ----------- -----------
                      **Posición**    **Importaciones**   **Local**   **Exportaciones**   **Total**   

                      **Portero**     818 442             534 430     2 270 665           901 747     

                      **Defensa**     1 132 993           793 411     2 976 407           1 660 167   

                      **Medio campo** 1 484 135           979 687     5 172 873           2 827 603   

                      **Delantero**   1 334 572           1 281 750   3 774 841           2 158 740   

                      **Total**       1 306 125           988 626     3 967 178           2 1472 88   

  Fuente: Elaboración                                                                                 
  propia con datos de                                                                                 
  transfermarkt                                                                                       
  ---------------------------------------------------------------------------------------------------------------

Esta forma de cálculo refleja con mayor precisión la estructura del
mercado, ya que toma en cuenta la frecuencia real de los distintos tipos
de transferencias y posiciones. De la tabla se desprende que las
exportaciones tienen, en promedio, valores notablemente más altos que
las importaciones y operaciones locales, lo que da cuenta del perfil
fuertemente exportador del fútbol argentino. Asimismo, las posiciones
ofensivas ---especialmente mediocampistas y delanteros--- tienden a
alcanzar precios promedio más elevados, aunque esto varía según el tipo
de operación.

#### Características del jugador {#características-del-jugador .unnumbered}

Las características propias del jugador tenidas en cuenta son cinco: La
edad (Variable *edad*), la estatura en centímetros (Variable *altura*),
si es comunitario, es decir, si no cuenta como extranjero en los clubes
miembros de la UEFA (Variable *comunitario*), si es extranjero (Variable
*extranjero*) para indicar que el jugador no posee nacionalidad
argentina, y la posición del jugador. Esta última se divide
tradicionalmente en cuatro posibilidades: portero, defensor,
mediocampista y delantero.

Según Carmichael y Thomas (1993), las características de los jugadores
que los equipos se disponen a contratar incluyen una mezcla de factores
intrínsecos y externos tanto de la carrera reciente como de la cosecha a
más largo plazo, que incluyen habilidad, experiencia, forma, condición
física, atractivo para el público, potencial y habilidad para el equipo.
Estas características deben medirse principalmente a través de variables
*proxy*. Por ejemplo, la relación entre la edad y el precio se basa en
la experiencia y el potencial. Mientras que la experiencia aumenta con
la edad, se espera que la capacidad y el potencial disminuyan después de
un punto. Como tal, se puede suponer que la relación general no es
lineal, lo que sugiere una forma cuadrática para la variable de edad
(ibid.). Sin embargo, dado el perfil exportador de la liga argentina,
también se puede esperar que predomine el efecto negativo de la pérdida
de potencial.

La presencia de la estatura (Variable *altura*) tiene por objetivo medir
el componente que aporta la capacidad física del jugador al precio. Más
específicamente, se busca determinar si, *ceteris paribus*, un jugador
con mayor potencia física tiene mayor valor. Fundamentado principalmente
en el aporte a los duelos aéreos. Se descarta el peso en este sentido,
por tener alta correlación en la altura. Al no encontrarse disponible la
velocidad máxima de los jugadores, esta variable fue descartada.

Respecto de las variables que hacen a la nacionalidad del jugador, se
exponen dos: comunitario y extranjero. La inclusión de estas variables
fue motivada por la existencia de cupos que limitan el número de
jugadores que pueden incorporarse con estas características. La variable
*extranjero*, entendida como que no posee la nacionalidad argentina,
afecta al mercado local. Mientras que la variable *comunitario* está
referida a su potencial libre acceso al mercado europeo, el cual, se
supone, es el de mayor prestigio.

En líneas generales, no se espera ningún signo para la posición de los
jugadores, debido a que se espera que las estadísticas de rendimiento
eliminen los efectos de la posición. Sin embargo, si se observa la Tabla
6, se puede decir que los medios y delanteros se negocian a precios más
altos que defensores y porteros.

A modo de resumen, se expone en la Tabla 7 las características de los
jugadores transferidos con precio por posición para el periodo de la
muestra.

  -------------------------------------------------------------------------------------------
  []{#_Ref197947296                                                             
  .anchor}Tabla 7\                                                              
  *Promedio de                                                                  
  características de                                                            
  los jugadores                                                                 
  transferidos con                                                              
  contrato vigente                                                              
  por posición en el                                                            
  período 2021-2024*                                                            
  ------------------- ------------ ------ ---------- ----------- -------------- -------------
  Categoría           Posición     N      Edad       Estatura    Comunitarios   Extranjeros
                                          (años)     (cm)                       

  Sin precio          Portero      22     26.82      188.90      31.82%         13.64%

                      Defensa      128    24.84      180.72      24.22%         22.66%

                      Medio campo  129    24.44      176.58      29.46%         17.50%

                      Delantero    174    24.87      178.10      17.24%         24.71%

  Con precio          Portero      60     30.15      186.82      21.67%         15.00%

                      Defensa      199    28.4       181.29      26.63%         26.13%

                      Medio campo  188    28.71      175.95      23.94%         23.94%

                      Delantero    216    28.51      179.39      24.70%         22.22%

  Total               Portero      82     29.26      187.16      24.39%         14.63%

                      Defensa      327    26.79      181.60      25.69%         24.77%

                      Medio campo  317    26.97      176.21      26.18%         21.14%

                      Delantero    390    26.88      178.77      21.30%         23.33%
  -------------------------------------------------------------------------------------------

Fuente: Elaboración propia con datos de *transfermarkt*

**Rendimiento del jugador en la temporada anterior a la transferencia**

Para las estadísticas de rendimiento, se tendrán en cuenta las
principales variables que tradicionalmente se consideran más importantes
para las posiciones: los partidos jugados (variable *pj*), los goles
convertidos (*goles*), las asistencias (*asis*) entendidas como los
pases realizados previos a anotar el gol, las tarjetas amarillas y las
expulsiones (*amonestac* y *expuls*), y para los porteros, los goles
concedidos y las vallas invictas (*goles*\_*conced* y *vallas*\_*inv*).

Siguiendo a Carmichael y Thomas (1993), se asume que el número de
apariciones en la liga durante la temporada anterior a la transferencia
refleja la forma y, posiblemente, el estado físico general. En este
caso, a diferencia de lo propuesto por los autores, se incorporan todos
los partidos jugados y no solo los partidos de liga. De esta forma se
intenta capturar el efecto de jugar otras competencias además de la
liga. De esta forma, se evaluará cada variable como un conjunto que, se
supone, mayor a medida que las características que representan vayan
mejorando[^14].

Podría considerarse que una forma más correcta de evaluar el efecto del
rendimiento en las competencias internacionales sobre el precio es tomar
una variable para la participación del equipo e incorporar las
estadísticas de rendimiento para cada competencia. Se omite para esta
investigación.

El número de goles marcados por el jugador en la temporada anterior
(variable *goles*) se utiliza como un indicador clave de su rendimiento.
Los goles son la estadística más fácil de recopilar y la que más
claramente puede asociarse al aporte individual del jugador. Otras
métricas, como balones recuperados, tiros fallados o atajadas, requieren
un mayor esfuerzo de recolección y procesamiento de datos.

Además, incluir todas estas variables en un modelo de regresión lineal
podría reducir la potencia estadística y generar una pérdida de grados
de libertad, especialmente considerando que el número de transferencias
con precio oscila entre 150 y 250 por ventana de transferencia. A esto
se suma que los coeficientes estimados pueden no ser estables en el
tiempo debido a cambios en el mercado de fichajes, como la inflación en
los precios, la evolución en la valoración de ciertas estadísticas o
nuevas regulaciones. Esto dificulta la identificación de los efectos de
una variable si su impacto no es lo suficientemente grande. No obstante,
estas métricas pueden resultar útiles en modelos predictivos más
avanzados, como Lasso, Ridge o el uso de componentes principales.

En la Tabla 8 se expone un resumen estadístico de las variables de
rendimiento. A partir de ella, se establece que el precio umbral para el
modelo Tobit es cercano a 45 000. También se puede observar que, si se
transfieren sin precio jugadores con buenas métricas, las medias y
medianas de las estadísticas siguen cierta tendencia a ser mejores en
las transferencias con precio.

  ---------------------------------------------------------------------------------------------------------
  []{#_Ref197950312                                                                          
  .anchor}Tabla 8\                                                                           
  *Resumen                                                                                   
  estadístico de las                                                                         
  variables de                                                                               
  rendimiento en el                                                                          
  período 2021-2024*                                                                         
  ------------------- --------------- -------------- ------------- -------------- ---------- --------------
  Categoría           Variable        Media          Mediana       SD             Mín        Máx

  Con precio          precio          2 436 141.28   1 270 000.0   3 654 380.93   53 000.0   44 250 000.0

                      pj              28.0           30.0          12.78          1.0        58.0

                      goles           3.62           2.0           4.54           0.0        23.0

                      asis            2.11           1.0           2.43           0.0        13.0

                      amonestac       4.41           4.0           3.57           0.0        19.0

                      expuls          0.30           0.0           0.62           0.0        4.0

                      goles_conced    1.8            0.0           6.12           0.0        62.0

                      vallas_invict   0.29           0.0           1.78           0.0        19.0

  Sin precio          precio          0.0            0.0           0.0            0.0        0.0

                      pj              22.31          22.0          11.58          1.0        58.0

                      goles           2.0            2.0           3.5            0.0        24.0

                      asis            1.19           1.0           1.81           0.0        11.0

                      amonestac       3.65           4.0           3.36           0.0        22.0

                      expuls          0.26           0.0           0.53           0.0        3.0

                      goles_conced    1.98           0.0           8.36           0.0        57.0

                      vallas_invict   0.58           0.0           2.57           0.0        21.0

  Total               precio          988 863.80     0.0           2 616 506.60   0.0        44 250 000.0

                      pj              24.66          25.0          12.41          1.0        58.0

                      goles           2.67           2.0           3.82           0.0        24.0

                      asis            1.56           1.0           2.13           0.0        13.0

                      amonestac       3.97           4.0           3.47           0.0        22.0

                      expuls          0.28           0.0           0.57           0.0        4.0

                      goles_conced    1.61           0.0           7.53           0.0        62.0

                      vallas_invict   0.46           0.0           2.28           0.0        21.0
  ---------------------------------------------------------------------------------------------------------

Fuente: Elaboración propia con datos de *transfermarkt*

#### Características de los clubes {#características-de-los-clubes .unnumbered}

Para operacionalizar las características de los clubes que representan
el poder de mercado, se va a utilizar el logaritmo del valor del plantel
de los compradores y vendedores informado por *transfermarkt* para la
temporada anterior. Esta decisión está fundamentada en que el valor del
plantel esta correlacionada un conjunto de características de los
clubes. El rezago temporal, se explica por la temporalidad de la
transferencia. Es concreto, se busca caracterizar a los clubes al
momento de la transferencia, y dado que las estadísticas de los clubes
se recopilan al final de la temporada, son las estadísticas previas más
cercanas que se disponen.

Para eliminar el efecto de la inflación, se deflactan los valores al mes
de junio de la temporada anterior al traspaso. Esto se decide debido a
que, por lo que se pudo corroborar, las ultimas actualizaciones de los
planteles se realizan al de final de temporada europea.

A partir de una regresión MCO *Pooleada* (POLS), se puede comprobar que
la variabilidad del valor del plantel, en su transformación logarítmica,
puede ser explicada en más del 80% por la variabilidad de un conjunto de
otras variables referentes al club. Estas variables son: el tamaño del
plantel (*len_plantel*), la posición en la liga (*league_position*), los
partidos de liga disputados (*partidos_jugados*), la cantidad de puntos
(*puntos*), los goles convertidos (*goles_favor*), los goles recibidos
(*goles_contra*), la división (*div*), la nacionalidad (*no_arg*) o
control por nacionalidad) (Tabla 14 del Anexo 2).

#### El momento de la transferencia {#el-momento-de-la-transferencia .unnumbered}

Finalmente, se dispone de dos variables destinadas a medir el momento de
la operación: el año de traspaso normalizado a 2021 (Variable
*tempbase*) y la estación del mercado (Variable *verano*) para indicar
verano o invierno europeo.

La primera variable intenta captar la existencia de inflación en los
precios con el paso del tiempo, tomando como base el año 2021. Para el
caso de la segunda variable, se busca capturar si existe algún efecto de
estacionalidad.

De la combinación de estas variables, se pueden ordenar consecutivamente
las ventanas de transferencias. Esto permite asignarles a las fechas de
vencimiento de contrato, un momento en relación con las ventanas de
transferencia disponibles para vender al jugador (Variable
*n_transfer_window*). Esto es importante debido a que la fecha de
vencimiento de contrato está ligada a la posibilidad de obtener un
precio por la transferencia. Además, se espera que la proximidad con la
fecha de vencimiento de contrato reduzca el poder de negociación de los
clubes.

### Modelos econométricos

De las especificaciones econométricas semilogarítmicas generales
(**2.1**)-(**2.4**) se ajustarán diferentes modelos econométricos
alternativos a fin de conocer los precios implícitos (precios sombra) de
los diferentes atributos de los jugadores y de los equipos negociadores
de la transferencia.

#### Modelos sin censura {#modelos-sin-censura .unnumbered}

En primer lugar, consideraremos los siguientes tres modelos más simples:

-   **Modelo 1**: Este es el modelo hedónico más simple, donde se ajusta
    la regresión (4.1) con
    \$$Z_{it} = \left( J_{i},R_{i,t - 1},T \right)$, donde \$$J_{i}$\$
    es el vector de características del jugador \$$i$\$,
    \$$R_{i,t - 1}$\$ es el vector de estadísticas de rendimiento del
    jugador en la temporada previa y \$$T$\$ son las variables
    temporales. Para el caso de la edad, se incluye el efecto
    cuadrático. Para este modelo \$$B_{it} = 0$\$, i.e. no se tienen en
    cuenta las características de los equipos que compran y venden.

-   **Modelo 2**: Este incorpora al Modelo 1 algunas características
    generales de los clubes. Específicamente, se incluyen el valor de
    mercado de los clubes compradores y vendedores. Además, se eliminan
    transformaciones no significativas (efecto cuadrático de la edad) y
    características personales no significativas.

-   **Modelo 3**: En este modelo, además de las características
    generales, se eliminan las estadísticas de rendimiento no
    significativas o con efecto opuesto al esperado, que no tienen
    interpretación teórica.

-   **Modelo 4**: En este modelo, en vez de incorporar las
    características de los clubes por medio de variables, se incluyen
    efectos específicos para el club comprador y vendedor. De esta
    manera, se tienen un modelo más saturado, con el fin de detectar si
    existen factores no observables de los equipos que cambien
    sustancialmente el modelo hedónico planteado.

Dado que en todos los modelos la regresión se estima con la
transformación logarítmica de la variable respuesta (*precio*), los
coeficientes son fácilmente interpretables en términos de su precio
implícito o como impacto marginal porcentual de cada atributo sobre el
precio. Tal interpretación es más directa si el predictor es una
variable continua (multiplicando por 100 el valor del coeficiente
respectivo). Pero si se quiere obtener el efecto de una forma más
precisa, se debe aplicar la fórmula
$\left( e^{\beta} - 1 \right)\text{.}100\%$.

Sin embargo, si la variable es categórica, siguiendo a Kennedy (1981),
debe aplicarse la siguiente formula sobre los parámetros estimados a fin
de estimar el impacto porcentual de dicha variable

$g^{*}(c) = \left( e^{\widehat{c} - \frac{1}{2}\widehat{V}\left( \widehat{c} \right)} - 1 \right) \times 100$

Donde \$ $\widehat{c}$ \$ es el coeficiente estimado de la variable y
$\widehat{V}\left( \widehat{c} \right)$ la varianza estimada. De esta
manera el valor $g^{*}(c)$ mide el impacto porcentual de dicha variable
sobre el precio del pase.

Este tipo de correcciones se evitarán cuando no haya grandes diferencias
entre la aproximación lineal y el efecto preciso.

#### Modelo Censurado {#modelo-censurado .unnumbered}

-   **Modelo Tobit**: Este modelo se aplica exclusivamente sobre el
    Modelo 3, incorporando la censura en los valores de transferencia.
    Dado que algunos jugadores no tienen un precio de transferencia
    observado, se utiliza un modelo Tobit para corregir el sesgo de
    selección en la variable dependiente. Se mantiene la especificación
    del Modelo 3, con las características generales de los clubes y las
    estadísticas de rendimiento relevantes, pero ajustando la estimación
    para reflejar que los valores de transferencia solo son observables
    por encima de un umbral de censura. Se excluyen las transferencias a
    precio cero sin contrato vigente (*rest_transfer_window* \> 0).

-   **Modelo Censurado**: En este modelo, se estiman los precios de
    transferencia mediante mínimos cuadrados ordinarios (MCO),
    incorporando tanto las observaciones con valores positivos como
    aquellas en las que el precio registrado es cero y tienen contrato
    vigente. A diferencia del modelo Tobit, que trata la censura como un
    fenómeno estructural del mercado, aquí se asume que un valor nulo de
    transferencia es un resultado válido y observable. El modelo
    mantiene la especificación del Modelo 3, incluyendo las
    características de los clubes y las estadísticas de rendimiento
    relevantes, sin excluir las transacciones en las que no se registra
    un precio de transferencia. Al tratarse de una regresión lineal sin
    ajuste por censura, los coeficientes estimados reflejan el impacto
    promedio sobre el logaritmo del precio, sin considerar
    explícitamente la probabilidad de que una observación pertenezca al
    subconjunto de valores positivos.

Respecto de las interpretaciones de los coeficientes obtenidos, vale
aclarar que los coeficientes de pendiente de MCO, son estimaciones
directas del cambio marginal. Para hacer que el coeficiente Tobit, sea
comparable se debe multiplicar por un factor de ajuste. Existen dos
métodos comunes para calcular un factor de ajuste para obtener efectos
parciales, al menos para variables explicativas continuas: el efecto
parcial al promedio, EPA y el efecto parcial promedio, EPP, el cual se
prefiere en la mayoría de los casos. Por desgracia, para las variables
explicativas discretas, comparar las estimaciones de MCO y de Tobit no
es tan fácil (Wooldridge, 2010).

Capítulo 3

# Resultados y discusión

## Introducción

En este capítulo se muestran los resultados de los diferentes modelos
estimados para determinar los condicionantes del precio de las
transferencias de futbolistas en el mercado argentino.

En primer lugar, se exponen los resultados de modelos que solo incluyen
las observaciones con precio positivo, abarcando modelos con y sin
variables de poder de negociación. Posteriormente, se agregarán las
observaciones con precio cero para realizar los modelos con y sin
corrección por probabilidad de observar el precio.

## Modelos hedónicos con Precios Positivos

En esta sección se presentan los resultados de diferentes modelos
hedónicos en los que no se tienen en cuenta las observaciones de precio
cero. Se exponen cuatro modelos, comenzando por el más simple, donde el
precio es modelado en función de características propias del jugador y
su rendimiento, para luego incluir las valoraciones de los clubes
compradores y vendedores como *proxy* del poder de negociación.
Finalmente, se incorporan los efectos fijos de comprador y vendedor para
saturar el modelo y observar la significatividad de las variables
excluyendo el efecto de la identidad de los clubes.

Para cada uno de los modelos estimados la prueba de Breusch-Pagan
rechaza la hipótesis nula de homocedasticidad, lo que da indicios de
errores heterocedásticos, por lo que los desvíos estándar de los
parámetros estimados deberían corregirse a fines de poder realizar una
inferencia correcta. Para tal propósito, se reportan los errores
estándar robustos de Huber-White (Cameron y Trivedi, 2010).

Como se especificó en el capítulo anterior, el Modelo 1 de la Tabla 9
solo se tienen en cuenta las características del jugador, las
estadísticas de rendimiento y las variables temporales (Factor
inflacionario, mercado de verano y ventanas de transferencia restantes).
En este modelo, la edad tiene un coeficiente negativo y significativo al
5%, y su término cuadrático es positivo y significativo al 10%. El
mínimo estimado se encuentra a los 37.4 años. Dado que solo 12
observaciones superan esta edad, la significancia del término cuadrático
podría estar capturando comportamientos atípicos, como operaciones de
repatriación de jugadores de alto valor en etapas avanzadas de su
carrera, o una posible desaceleración del efecto negativo de la edad. La
pérdida de significancia del término cuadrático al incorporar variables
de los equipos (Modelo 2) sugiere que la primera hipótesis es más
plausible.

La altura, al igual que en buena parte de la literatura, no muestra
significancia estadística. La nacionalidad argentina del jugador tampoco
es significativa, pero la posesión de pasaporte comunitario sí lo es,
con un efecto positivo, de al rededor del 40%, significativo al 1%. Esto
podría deberse a que el pasaporte comunitario habilita al jugador a ser
transferido a ligas europeas donde los precios son, en promedio, más
altos. Por lo tanto, esta variable podría estar captando la probabilidad
de ser comprado por un equipo europeo. Esta idea se refuerza en el
Modelo 2, donde la variable pierde significatividad cuando se incluyen
los valores de los planteles de los equipos. Sobre la posición de los
jugadores, en este modelo, no hay evidencia significativa de que afecten
al precio.

Respecto a las estadísticas de rendimiento de los jugadores, la cantidad
de partidos jugados es estadísticamente significativa al 5%, impactando
positivamente sobre el precio como es esperado mejorando en un 1.6% por
cada partido jugado. Los goles también se muestran significativos, pero
al 10% con un efecto estimado de una mejora aproximada del 3.2% en el
precio por cada gol convertido.

El resto de las variables de rendimiento no son significativas al 10%, o
en el caso de serlo, como puede ser en los casos de las expulsiones, los
goles concedidos y las vallas invictas, ambas ultimas exclusivas para
los porteros, lo son con el signo contrario al esperado, sin que esto
tenga una explicación razonable. Podría esperarse que sea la ausencia de
la inclusión de variables de equipos, pero como se verá más adelante,
esto no corrige los coeficientes, aunque si disminuye la
significatividad, pudiendo esto ser un indicio que para incluirlos se
deba mejorar la especificación del modelo. Una explicación alternativa
para las variables de los porteros es que la muestra de esta posición no
sea lo suficientemente grande para capturar correctamente el
coeficiente, o que se requiera incorporar estadísticas de rendimiento
más específicas como las atajadas o goles evitados.

La variable de ventanas de transferencia restantes tiene un efecto
positivo y significativo de acuerdo con lo esperado. Cada ventana de
transferencia adicional le otorga al equipo vendedor un poder de
negociación que le permite, en promedio, mejorar el precio
aproximadamente en un 20% (Si se corrige el coeficiente obtenido el
efecto llega al 22.5%). Por otro lado, el factor inflacionario temporal
tiene un coeficiente positivo del 4.8%, pero no alcanza significancia
estadística. La estacionalidad asociada al mercado de verano tampoco
muestra efectos significativos.

Cuando se incorporan las variables que identifican a los mercados
*lvalor_plantel_v* y *lvalor_plantel_c* en el Modelo 2, se observa una
importante mejora en la bondad de ajuste del modelo, el R-cuadrado
aumenta aproximadamente en 0.29. Ambas variables son estadísticamente
significativas al 1%. Se observa que, la elasticidad precio para los
equipos compradores en de 0.53% de aumento en el precio de transferencia
por cada 1% de aumento en el valor del plantel, mientras que para los
vendedores es de 0.31%. Dado que se está controlando por variables
relacionadas a la performance y calidad de los jugadores, tal efecto
podría estar respondiendo a un efecto del mayor poder de negociación de
los vendedores. En contraposición, los equipos de mayor valor tienden a
pagar precios por encima del rendimiento y las características
personales, sugiriendo que la urgencia por ganar se impone al poderío
económico.

De estos coeficientes, también se puede comprender la crítica de los
equipos más débiles a los grandes sobre que les pagan los jugadores a un
precio más bajo. No lo dicen desde una perspectiva absoluta, que sería
representado por un coeficiente negativo en el logaritmo del plantel del
comprador, sino desde una perspectiva relativa. Es decir, si ellos, los
vendedores, fuesen *más grandes*, obtendrían un mayor precio. O, desde
el otro punto de vista, ante dos jugadores de iguales características y
rendimiento, es más barato comprarlo a un equipo de menor valor.

Comparando con el Modelo 1, se observan cambios sustanciales en los
coeficientes asociados a las características de los jugadores. La edad
deja de ser significativa en términos cuadráticos, por lo que se
prefiere continuar testeando con una versión más parsimoniosa sin
efectos cuadráticos. Asimismo, la variable *comunitario* pierde
significancia, posiblemente debido a la inclusión del valor de los
planteles, que captura el efecto de los equipos europeos, destino de los
jugadores comunitarios, generalmente con plantillas de mayor valor.

En contraposición con esto, las posiciones de los jugadores ganan
significatividad. Si se toma como categoría base los defensores, y
aplicando la corrección de Kennedy (1981) los mediocampistas se
negocian, en promedio, en un 27%, mientras que los delanteros en un 29%.
Los porteros, no tienen un coeficiente estadísticamente significativo,
muy probablemente asociado a su alta varianza, explicada por la escasez
de su participación en la conformación de los planteles, y más aún, en
los mercados.

Respecto a las estadísticas de rendimiento, no se observan cambios
significativos, solo la mejora de la significatividad de los goles del
10 al 5%. También se mantienen las inconsistencias de las estadísticas
de los porteros y las expulsiones (*off*).

Para cerrar el análisis de este modelo, se puede destacar la mejora en
la significatividad del factor inflacionario y el aumento en su efecto,
pasando de no ser significativa, serlo al 1% con un efecto del 18% anual
en términos reales. Por otro lado, las ventanas restantes de contrato de
los jugadores, tiene un efecto menor, más cercano al 14%, pero con la
misma significancia. Esto puede deberse a que estaban capturando parte
del poder de negociación de los equipos de mayor valor, es decir, es
probable que los equipos más grandes, tengan jugadores con contratos más
largos (mayor riego y costo).

  ------------------------------------------------------------------------------------------------------
  []{#_Ref198034319                                                                                   
  .anchor}Tabla 9\                                                                                    
  *Parámetros Estimados                                                                               
  de Modelos Simples de                                                                               
  Precios Hedónicos:                                                                                  
  variable dependiente                                                                                
  lprecio*                                                                                            
  ----------------------- -------- --------- -------- --------- -------- --------- -------- --------- --
  Variable                Modelo 1           Modelo 2           Modelo 3           Modelo 4           

  edad                    -0.449   (0.176)   -0.169   (0.117)   -0.108   (0.014)   -0.092   (0.022)   
                          \*\*                                  \*\*\*             \*\*\*             

  edad²                   0.006 \* (0.003)   0.001    (0.002)                                         

  altura                  0.008    (0.008)                                                            

  comunitario             0.337    (0.120)   0.146    (0.089)                                         
                          \*\*\*                                                                      

  extranjero              -0.155   (0.121)                                                            

  delantero               0.106    (0.154)   0.258    (0.113)   0.263    (0.105)   0.117    (0.170)   
                                             \*\*               \*\*                                  

  mediocampista           0.199    (0.137)   0.241    (0.101)   0.243    (0.098)   0.034    (0.169)   
                                             \*\*               \*\*                                  

  portero                 -0.467   (0.546)   -0.127   (0.449)   0.257    (0.205)   0.419    (0.292)   

  pj                      0.016    (0.006)   0.010    (0.004)   0.012    (0.003)   0.005    (0.006)   
                          \*\*               \*\*               \*\*\*                                

  goles                   0.032 \* (0.017)   0.033    (0.014)   0.028    (0.013)   0.040    (0.020)   
                                             \*\*               \*\*               \*\*               

  asis                    0.019    (0.026)   0.002    (0.019)                                         

  amonestac               -0.004   (0.018)   0.001    (0.013)                                         

  expuls                  0.133 \* (0.075)   0.110 \* (0.060)                                         

  goles_conced            0.038    (0.019)   0.026 \* (0.014)                                         
                          \*\*                                                                        

  vallas_inv              -0.070   (0.036)   -0.037   (0.037)                                         
                          \*                                                                          

  rest_ventana_transfer   0.202    (0.027)   0.143    (0.021)   0.138    (0.021)   0.092    (0.034)   
                          \*\*\*             \*\*\*             \*\*\*             \*\*\*             

  temporada               0.048    (0.059)   0.181    (0.044)   0.164    (0.040)   0.016    (0.057)   
                                             \*\*\*             \*\*\*                                

  verano                  0.005    (0.117)   -0.124   (0.088)                                         

  lvalor_plantel_c                           0.531    (0.045)   0.542    (0.044)                      
                                             \*\*\*             \*\*\*                                

  lvalor_plantel_v                           0.307    (0.040)   0.307    (0.040)                      
                                             \*\*\*             \*\*\*                                

  Ef. Comprador           No                 No                 No                 Sí                 

  Ef. Vendedor            No                 No                 No                 Sí                 

  N                       390                388                388                390                

  R²                      0.338              0.627              0.617              0.918              

  df_m                    18                 18                 10                 115                
  ------------------------------------------------------------------------------------------------------

Errores estándar entre paréntesis

\* p\<0.1, \*\* p\<0.05, \*\*\* p\<0.01

En el Modelo 3 se elimina el efecto cuadrático de la edad por perder
significatividad al incorporar las variables que identifican a los
equipos (Modelo 2). Además, la prueba de Ramsey RESET para variables
omitidas no rechaza la hipótesis de que el modelo no tiene variables
omitidas (p-valor = 0.91), lo que apoya la validez del modelo sin el
término cuadrático. La misma prueba para la versión del modelo 1 sin el
término cuadrático de la edad, tampoco rechaza la hipótesis de que el
modelo no tiene variables omitidas.

Sin embargo, se prefiere exponer el modelo con el término cuadrático
porque la hipótesis de la teoría se corresponde con un efecto
cuadrático, aunque se haya obtenido en sentido inverso a lo indicado. En
concreto, las estimaciones indican que la edad tiene un efecto negativo
lineal en el precio de los jugadores transferidos desde y hacia el
fútbol argentino.

En líneas generales, este modelo aporta simpleza con una pérdida de
explicatividad general mínima. Se ha conservado: la edad, la posición,
los partidos jugados (*pj*), goles convertidos (*goles*), las ventanas
de transferencia restantes (*rest_ventana_transfer*), el factor
inflacionario (*temporada*) y los valores de los planteles en logaritmos
(*lvalor_plantel_v y lvalor_plantel_c*).

En cuanto a los efectos de estas variables, la edad tiene un impacto
negativo en el precio de los jugadores. Cada año adicional reduce el
valor del pase en aproximadamente 10%, lo que confirma la expectativa
teórica de que el mercado valora menos a los jugadores a medida que
envejecen. Al no haber un efecto cuadrático, pareciera estar indicando
que, una vez llegados a la primera división, el efecto del incremento de
experiencia ganado en el fútbol local no compensa la pérdida de valor
del paso del tiempo.

El rendimiento reciente también juega un papel clave en la valoración.
Cada partido adicional disputado en la temporada anterior incrementa el
precio en 1.2%, mientras que cada gol convertido lo eleva en 2.8%. Ambos
efectos son estadísticamente significativos, lo que refuerza la relación
positiva entre el desempeño y el valor de mercado.

Los resultados indican que la posición en el campo influye
significativamente en el precio de los jugadores. En comparación con los
defensores (categoría base), los delanteros tienen un precio
aproximadamente un 29% mayor, mientras que los mediocampistas muestran
un incremento del 27%. En tanto, los arqueros presentan un coeficiente
contrario al esperado, pero no significativo, su impacto es similar al
de los mediocampistas en el precio de transferencia (27%)[^15].

El número de ventanas de transferencia restantes tiene un impacto
importante: por cada ventana adicional disponible, el precio del jugador
aumenta en 13.8%. Este resultado, altamente significativo, sugiere que
un vínculo más largo con el jugador favorece la obtención de una
contraprestación más alta de los jugadores. O, si se analiza en sentido
contrario, una urgencia por vender por estar finalizando el vínculo del
club vendedor con el jugador reduce su poder de negociación casi un 14%
por cada ventana perdida.

Esta afirmación debe analizarse con cuidado, porque establecer
correlación, no establece causalidad. No se pretende decir que si los
clubes pretenden sacar más dinero por sus jugadores deben renovar
constantemente sus contratos, sino que el fenómeno de reducción de años
de contrato restante está asociado a menores precio de venta. Es decir,
la explicación más probable a esta asociación es que la posibilidad de
que los clubes no reciban ninguna compensación por la salida de un
jugador los predispone a recibir menos dinero. También la reducción de
los años de contrato restantes puede entenderse como la expresión de
voluntad de alguna de las partes por no continuar con el vínculo, lo que
vuelve más barata su ruptura.

El factor temporal también muestra un efecto positivo, con un incremento
promedio del 16% por cada temporada. Esto podría reflejar tanto la
inflación en el mercado de fichajes como tendencias de valorización en
el fútbol argentino.

Por último, los valores de los planteles de los equipos comprador y
vendedor son determinantes en el precio de los traspasos. Un aumento del
1% en el valor del plantel del club comprador está asociado con un
incremento del 0.54% en el precio de compra, mientras que un aumento del
1% en el valor del plantel del club vendedor eleva el precio del
traspaso en 0.31%. Estos efectos, ambos altamente significativos,
indican que los equipos con mayor poder económico suelen realizar
fichajes más costosos, tanto por su capacidad de pago como por la
valorización de los jugadores en clubes más prestigiosos.

Por su parte, el Modelo 4 incorpora los efectos fijos para clubes
compradores y vendedores. Con la incorporación de estos, se tiene un
modelo casi saturado donde el 92% de la variabilidad es explicada por
las variables y factores incluidos en el mismo. En contrapartida, la
cantidad de variables utilizadas deja al modelo con muchos menos grados
de libertad.

La inclusión de efectos fijos para los clubes compradores y vendedores
reduce notablemente la cantidad de variables que resultan
significativas, lo que sugiere que una parte importante de la variación
en los precios está explicada por las características propias de los
equipos involucrados. En este escenario, solo tres variables mantienen
su significancia: la edad del jugador, los goles convertidos en la
temporada previa y las ventanas de transferencia restantes. Cada gol
adicional se asocia con un aumento cercano al 4% en el precio, mientras
que cada ventana disponible suma alrededor de un 13.8%. La edad, por su
parte, conserva un efecto negativo, con una caída del 9% por cada año
adicional.

Ahora bien, el hecho de que el resto de las variables pierdan
significancia no implica necesariamente que no sean relevantes. En
modelos con muchos efectos fijos ---como este--- es común que solo
prevalezcan los impactos más fuertes o persistentes. Es posible que
parte del efecto de algunas variables esté siendo absorbido por las
diferencias estructurales entre los clubes, lo que hace más difícil
detectar su influencia estadística de forma aislada.

## Censura, truncamiento y estimación Tobit

A partir de los regresores estimados en el Modelo 3, se estimaron las
regresiones censuradas y con corrección por probabilidad de Tobin
(*Tobit*). En este contexto, los modelos para precios positivos pueden
considerarse los modelos truncados de un modelo más amplio de todos los
jugadores transferidos.

Como se explicó en el capítulo anterior, para conocer los efectos
marginales, se pueden incluir las observaciones de precio cero de dos
formas: la primera es incorporar las observaciones a la regresión MCO en
lo que se conoce como modelo censurado, pero sin ningún tipo de
corrección. La segunda, es corregir esta censura por la probabilidad de
observar el precio a partir de una estimación Tobit. Se estableció como
límite inferior el logaritmo natural de 45 000. En la Tabla 10 se
exponen los parámetros obtenidos por los distintos métodos de
estimación. Entre las principales diferencias con los modelos con
precio, se puede destacar que la prueba de Breusch--Pagan/Cook--Weisberg
para heteroscedasticidad del Modelo MCO Censurado no rechaza la
hipótesis de varianza constante al 25% de significancia.

  --------------------------------------------------------------------------
  []{#_Ref198041508                                                
  .anchor}Tabla 10\                                                
  *Parámetros Estimados                                            
  del Modelo Censurado y                                           
  del modelo Tobit:                                                
  variable dependiente                                             
  lprecio*                                                         
  ------------------------ --------------- --------- ------------- ---------
  **Variable**             **Censurado**             **Tobit**     

  edad                     -0.657 \*\*\*   (0.044)   -0.181 \*\*\* (0.011)

  delantero                -0.262          (0.490)   -0.011        (0.112)

  mediocampo               0.167           (0.452)   0.084         (0.105)

  portero                  1.847 \*\*      (0.768)   0.606 \*\*\*  (0.218)

  pj                       0.064 \*\*\*    (0.016)   0.019 \*\*\*  (0.004)

  goles                    0.275 \*\*\*    (0.058)   0.069 \*\*\*  (0.013)

  lvalor_plantel_c         2.442 \*\*\*    (0.172)   0.660 \*\*\*  (0.040)

  lvalor_plantel_v         -0.008          (0.179)   0.112 \*\*\*  (0.041)

  rest_transfer_window     0.553 \*\*\*    (0.114)   0.136 \*\*\*  (0.024)

  season                   0.228           (0.170)   0.074 \*      (0.040)

  Varianza del error σ²                              2.593 \*\*\*  (0.202)

  N                        769                       769           

  Proporción sin precio    0.495                     0.495         

  R²                       0.560                                   

  Log-likelihood                                     -915.799      

  df_m                     10                        10            
  --------------------------------------------------------------------------

Errores estándar entre paréntesis

\* p\<0.1, \*\* p\<0.05, \*\*\* p\<0.01

  ------------------------------------------------------------------------------------------
  []{#_Ref198045226                                                                 
  .anchor}Tabla 11\                                                                 
  *Efectos Marginales                                                               
  de los Modelos                                                                    
  Truncado (MCO),                                                                   
  Censurado (MCO) y                                                                 
  Censurado Tobit:                                                                  
  Variable                                                                          
  Dependiente                                                                       
  lprecio.*                                                                         
  ------------------- ---------------------- ------------ ------------- ----------- --------
                      Variable               MCO Truncado MCO Censurado Tobit       

                      edad                   -0.108       -0.657 \*\*\* -0.181      
                                             \*\*\*                     \*\*\*      

                      delantero              0.263 \*\*   -0.262        -0.011      

                      mediocampo             0.243 \*\*   0.167         0.084       

                      portero                0.257        1.847 \*\*    0.606       
                                                                        \*\*\*      

                      pj                     0.012 \*\*\* 0.064 \*\*\*  0.019       
                                                                        \*\*\*      

                      goles                  0.028 \*\*   0.275 \*\*\*  0.069       
                                                                        \*\*\*      

                      lvalor_plantel_c       0.542 \*\*\* 2.442 \*\*\*  0.660       
                                                                        \*\*\*      

                      lvalor_plantel_v       0.307 \*\*\* -0.008        0.112       
                                                                        \*\*\*      

                      rest_transfer_window   0.138 \*\*\* 0.553 \*\*\*  0.136       
                                                                        \*\*\*      

                      season                 0.164 \*\*\* 0.228         0.074 \*    
  ------------------------------------------------------------------------------------------

Errores estándar entre paréntesis

\* p\<0.1, \*\* p\<0.05, \*\*\* p\<0.01

Se calcularon los efectos marginales (parciales promedio[^16])
condicionados a la probabilidad de que la variable latente sea mayor al
umbral de censura (10.7) (Tabla 11). Las interpretaciones de las
variables de posición deben tomarse como indicativas, debido a que no
está claro que el software estadístico siga el método recomendado por
Wooldridge (2010) para las variables binarias.

En líneas generales, se puede observar que los efectos parciales del
modelo Tobit son similares a los del modelo MCO Truncado (Modelo MCO 3)
pero con la significatividad del Modelo Censurado. La principal
diferencia en los coeficientes del modelo con corrección de Tobit se
encuentra en la elasticidad del equipo vendedor y en los goles. Estos
últimos, casi triplican su efecto porcentual, pasa del 3 al 7%, mientras
que la elasticidad-precio respecto del valor del equipo vendedor, hay
que aclarar, que, si bien sigue siendo significativo, su efecto se
reduce a un tercio, bajando de una elasticidad de 0.3 al 0.1.

Sobre las características de los jugadores, el efecto negativo de la
edad crece del 10% al 18%, mientras que las posiciones pierden
significatividad. Si bien los porteros son significativos, por la baja
proporción que representan en la muestra y la diferencia en las
funciones en el campo de juego, debe considerarse si es correcto
incluirlo en la regresión. La interpretación del coeficiente estaría
indicando que en promedio los porteros se negocian un 60% por encima de
los defensores controlando por el resto de las variables.

Los partidos jugados y la elasticidad-precio del comprador se mantienen
similares. Cada partido jugado pasa de tener un efecto del 1.2% a casi
el 2%, mientras que la elasticidad-precio del comprador sube de 0.54 a
0.66. Vale destacar que en el modelo MCO Censurado, el efecto del club
comprador es notoriamente más elevado, concentrando todo el efecto de la
negociación. Si bien este resultado puede parecer una curiosidad
empírica tiene un poco más de sentido si se lo analiza junto a los
resultados de un modelo *probit* que intente explicar la probabilidad de
que el jugador tenga precio (Tabla 13 - Anexo 1).

En este sentido, Wooldridge (2010) sugiere que una forma de evaluar de
manera informal si el modelo Tobit es adecuado es estimar un modelo
probit donde el resultado binario, sea igual a uno si la variable es
observable y cero si está censurada. El modelo Tobit será válido si los
coeficientes del probit son *cercanos* a los coeficientes del Tobit
dividido la estimación del error ($\widehat{\sigma}$). Éstas nunca serán
idénticas debido al error de muestreo. Pero se pueden buscar ciertos
signos problemáticos.

En la Tabla 12 se exponen los resultados. No se observan grandes
diferencias a excepción de las variables de poder de negociación. Para
entender porque sucede esto con las variables de negociación, es
importante clarificar que se está estimando en cada modelo.

En el Modelo Truncado por MCO, se están calculando el efecto del valor
del plantel de los equipos en la transferencia de un jugador con precio,
mientras que en el modelo Tobit se está calculando el poder de
negociación de toda la muestra, jugadores con o sin precio ajustado por
la probabilidad de tener precio.

En este último caso, conviene recordar que la decisión de desprenderse
de un jugador con contrato sin recibir un pago a cambio muy
probablemente sea una decisión que el club *vendedor* toma por su cuenta
y que, en todo caso, es producto de una negociación con el jugador y no
con el club de destino. En concreto, si el club de origen detecta el
interés de algún otro club, lo más probable es que intente obtener una
contra prestación por el jugador. Como el liberar jugadores con contrato
es una situación que puede ocurrir con clubes de todos los valores,
estos no inciden en la probabilidad de tener precio, pero este tipo de
salida, si puede ser un indicador de un jugador que no rinde o es
problemático, y, por tanto, es esperable que, en general, vayan a clubes
menos prestigiosos. Por el contrario, los jugadores con precio tendrán
por destino equipos de mayor valor, inflando artificialmente su efecto
en la negociación por incorporar en el análisis una negociación que
nunca ocurre.

  ---------------------------------------------------------------------------
  []{#_Ref198045529                                          
  .anchor}Tabla 12\                                          
  *Comparación de coeficientes                               
  Probit vs. Tobit                                           
  estandarizados*                                            
  ---------------------------- -------------- -------------- ----------------
  **Variable**                 **Tobit/σ**    **Probit**     **Diferencia**

  edad                         -0.194 \*\*\*  -0.220 \*\*\*  -0.026

  Delantero                    -0.012         -0.219         -0.207

  Medio campo                  0.091          0.030          -0.061

  Portero                      0.602 \*\*\*   0.700 \*\*     0.098

  pj                           0.020 \*\*\*   0.016 \*\*\*   -0.004

  goles                        0.074 \*\*\*   0.121 \*\*\*   0.047

  lvalor_plantel_c             0.710 \*\*\*   0.827 \*\*\*   0.117

  lvalor_plantel_v             0.121 \*\*\*   -0.028         -0.149

  rest_transfer_window         0.147 \*\*\*   0.169 \*\*\*   0.022

  season                       0.080 \*       0.074          -0.006
  ---------------------------------------------------------------------------

p \< 0.1, \*\* p \< 0.05, \*\*\* p \< 0.01

σ = √2.593554 ≈ 1.6104 (desvío estándar del modelo Tobit)

Finalmente, en cuanto a la temporalidad de las transferencias, el tiempo
de contrato restante se ha mostrado casi inalterado, permaneciendo
iguales las interpretaciones que en los modelos MCO, mientras que el
factor inflacionario deja de ser significativo. Muy probablemente,
agregar una muestra del mismo taño que la original que por definición es
inerte al factor inflacionario, perjudique en lugar de mejorar su
efecto. Es decir, para analizar que sucede con los precios con el paso
del tiempo, hay que mirar solo las observaciones con precio.

# Conclusiones {#conclusiones .Titulo-1-sin-numeracion}

En este trabajo se han abordado varios aspectos de la Economía del
Deporte, tanto teóricos como empíricos, con el fin de comprender los
determinantes del precio de los pases en el mercado del fútbol
argentino. En lo que respecta a lo teórico lo más destacable consistió
en identificar las particularidades de la Economía del Deporte,
entendiéndola como una rama de la Economía en la que, por la esencia
competitiva de su objeto de estudio, algunos conceptos de la economía
tradicional pueden no ser aplicables.

La teoría, también, permitió responder dos de las preguntas que se
plantean en este trabajo ¿Por qué y para qué se comercian jugadores de
fútbol? Los clubes comercian derechos sobres los jugadores de fútbol,
porque tienen derechos de exclusividad sobre el uso de la fuerza de
trabajo de los futbolistas, establecido con el objetivo de proteger el
patrimonio de los clubes y sostener el interés del público mediante la
estabilización de los planteles. Estos activos, a su vez, son
comercializables porque sirven para ganar partidos y para la generación
directa e indirecta de ingresos de las firmas.

En lo que respecta al abordaje empírico de esta investigación, en
términos generales los modelos hedónicos explican una buena parte de la
variabilidad del precio, sobre todo en aquellos que incorporan el valor
del plantel de los clubes. Sin embargo, aún queda mucho por explicar a
través de la modelización. Características contractuales como el salario
y la cláusula de rescisión son destacadas por la teoría como relevantes
para la determinación de los precios. Sin embargo, dicha información
resulta inaccesibles para todas las principales ligas del mundo, a
excepción de Alemania. Estos cambios solo pueden venir a través de
regulaciones de organismos superiores, ya que es esperable que tanto
clubes como jugadores quieren mantener dicha información en secreto.
Otras variables de interés que pueden adicionarse a los modelos son las
propensiones a las lesiones, la fama del jugador, las convocatorias a la
selección nacional, etc.

Entre lo más destacable de esta investigación se encuentra la
importancia de incorporar en la determinación de los precios las
variables que identifican a los clubes. Además, esto debe hacerse de
forma asimétrica, porque como sostienen Carmichael y Thomas (1993) no es
realista tomar a compradores y vendedores de manera simétrica al momento
de negociar. En este sentido, los resultados indican que los clubes
pagan más mientras más grandes son, pero también consiguen mejores
precios de venta. Esta circunstancia es coincidente con la hipótesis de
Ruijg y van Ophem (2015) donde los jugadores, a medida que van
demostrando mejor rendimiento, se van incorporando a equipos con mayor
nivel de competencia (de mayores ingresos, ergo, más grandes) en los
cuales se manejan precios de transferencia más altos. Estar en una
categoría superior implica pagos en promedio más elevados, y al mismo
tiempo mayores posibilidades de reventa a precios más altos,
principalmente por el acceso a compradores potenciales más grandes y no
tanto por la grandeza propia del club que vende.

Si se quiere analizar el poder de negociación en función de los signos
de las elasticidades, se puede decir que, *ceteris paribus* el
rendimiento y las características intrínsecas del jugador, los equipos
vendedores tienen poder de negociación, porque poseen el rendimiento que
se demanda para la competencia deportiva, mientras que los compradores
pagan más mientras más valor de plantel tienen, i.e., mientras asuman
roles más importantes en la competencia deportiva, más gastaran. La
competencia deportiva afecta al comprador.

Entonces, contrariamente a lo que se pueda imaginar en la teoría
económica general y el sentido común, los clubes más grandes, en valor
de mercado informado por *transfermarkt*, cuando compran, tienen un
poder negativo de negociación, es decir, en lugar de bajar lograr el
precio, lo incrementan. Por otro lado, los vendedores sí tienen un poder
de negociación positivo. La estrategia de quedarse con un porcentaje del
pase de una futura venta, puede ser una buena estrategia para captar
ingresos de la venta cuando venden a un club más grande. Sin embargo,
esta operación incorpora el riesgo de que el jugador no rinda de igual
manera en una competencia más exigente.

En relación con las características personales del Jugador, la edad ha
sido la más determinante respecto del precio, pero a diferencia de las
investigaciones precedentes centradas en el fútbol inglés o europeo, en
lugar de ser una relación cuadrática cóncava, es lineal decreciente. Una
posible explicación a este fenómeno es que los clubes argentinos no
valoren tanto la experiencia como el potencial, o quizás exista cierta
discrepancia respecto de los equipos europeos sobre cuando finaliza el
periodo de formación del jugador, particularmente al momento de vender.

Por otro lado, la posición del jugador resulta significativa, pero solo
en los casos de mediocampistas y delanteros. Para el caso de los
porteros, la escasez de observaciones en su posición y lo diferente de
su rol en el juego puede sugerir la conveniencia de excluirlos del
modelo. También se requiere un mayor trabajo en la construcción de
variables de rendimiento que logren aislar correctamente el aporte de
cada posición, con el objetivo de poder controlar los resultados.

Solo las estadísticas de rendimiento más reconocidas, partidos jugados y
goles, han demostrado tener una cierta influencia en los precios. Se ha
corroborado que los jugadores con más partidos jugados tienen una
valoración mayor en el mercado. Mientras que los goles, también
resultaron ser significativos con el signo esperado. Los goles aportan
al precio. El resto de las variables de rendimiento no han encontrado
evidencia suficiente de significatividad. Se sugiere para próximos
estudios la incorporación de variables de rendimiento más específicas,
combinadas con métodos de estimación que penalicen menos la
incorporación de variables.

Ampliar la muestra incluyendo las transferencias sin precio declarado
puede ser una estrategia útil para estimar con mayor precisión el efecto
de las características personales, el rendimiento y el tiempo de
contrato restante sobre el precio. Sin embargo, no resulta igualmente
adecuada para captar el poder de negociación de los clubes. Tal como se
observa en el modelo probit estimado para analizar la probabilidad de
contar con un precio registrado, las variables de interés muestran un
comportamiento consistente, con excepción de aquellas vinculadas al
poder de negociación, cuya influencia resulta más difícil de identificar
bajo esta especificación.

En este sentido, si el objetivo del análisis es estudiar el impacto de
las características individuales o del desempeño sobre el valor de
transferencia, tiene sentido incorporar las observaciones sin precio.
Pero si el foco está puesto en los determinantes del poder de
negociación, es preferible restringir el análisis a las transferencias
con precio positivo, donde dicho efecto puede identificarse con mayor
claridad. Además, según lo obtenido en esta investigación y en línea con
los que el sentido común podría indicar, el precio es el ámbito donde
sucede la negociación entre los clubes.

# Bibliografía {#bibliografía .Titulo-1-sin-numeracion}

Altman, D. (2013). *What's wrong with Soccernomics?* Tech. rep., North
Yard Analytics.
https://web.archive.org/web/20241105151412/http://danielaltman.com/data/soccernomics-nya.pdf

Cameron, A., & Trivedi, P. (January de 2010). *Microeconometrics Using
Stata* (Vol. 5).

Cardenal Carro, M. (1996). *Deporte y derecho profesional: Las
relaciones laborales en el deporte.* Murcia, España: Ed. EDITUM.

Carmichael, F. (January de 2006). The player transfer system in soccer.
En *Handbook on the Economics of Sport* (págs. 668-676). Ed. Edward
Elgar. https://doi.org/10.4337/9781847204073.00086

Carmichael, F., & Thomas, D. (1993). Bargaining in the transfer market:
theory and evidence. *Applied Economics, 25*, 1467-1476.

Carmichael, F., & Thomas, D. (March de 2000). Institutional Responses to
Uncertainty: Evidence from the Transfer Market. *Economic Issues Journal
Articles, 5*, 1-20.
https://ideas.repec.org/a/eis/articl/100carmichael.html

Carmichael, F., Forrest, D., & Simmons, R. (1999). The Labour Market in
Association Football: Who Gets Transferred and for How Much? *Bulletin
of Economic Research, 51*, 125-150.
https://doi.org/10.1111/1467-8586.00075

Carmichael, F., Thomas, D., & Ward, R. (2000). Team performance: the
case of English Premiership football. *Managerial and Decision
Economics, 21*, 31-45.
https://doi.org/10.1002/1099-1468(200001/02)21:1\<31::AID-MDE963\>3.0.CO;2-Q

Carson, R. T., & Sun, Y. (2007). The Tobit model with a non-zero
threshold. *The Econometrics Journal, 10*, 488--502.

Christiano, K. J. (June de 1986). Salary Discrimination in Major League
Baseball: The Effect of Race. *Sociology of Sport Journal, 3*, 144-153.
https://doi.org/10.1123/ssj.3.2.144

CIES Football Observatory. (2018). Scientific assessment of football
players' transfer value. *Scientific assessment of football players'
transfer value*.
https://web.archive.org/web/20180128035934/http://www.football-observatory.com/IMG/pdf/note01en.pdf

Coremberg, A., Sanguinetti, J., & Wierny, M. (2016). El fútbol en la
economía Argentina. Números sin pasiones. *Sports Economics &
Managament, 6*, 46-68.

Costanigro, M., Mccluskey, J. J., Lusk, J. L., Roosen, J., & Shogren, J.
F. (November de 2012). Hedonic Price Analysis in Food Markets. *Hedonic
Price Analysis in Food Markets*. Oxford University Press.
https://www.oxfordhandbooks.com/view/10.1093/oxfordhb/9780199569441.001.0001/oxfordhb-9780199569441-e-7

Cotteleer, G., Gardebroek, C., & Luijt, J. (2008). Market power in a
GIS-based hedonic price model of local farmland markets. *Land
Economics, 84*, 573--592.

Davenport, D. S. (1969). Collusive Competition in Major League Baseball
its Theory and Institutional Development. *The American Economist, 13*,
6-30. https://doi.org/10.1177/056943456901300201

Deloitte & Touche. (2024). *Deloitte Football Money League 2024.* Tech.
rep., Deloitte & Touche.
https://www2.deloitte.com/content/dam/Deloitte/fi/Documents/about-deloitte/deloitte-uk-annual-review-of-football-finance.pdf

Desormeaux, D., & Piguillem, F. (2003). Precios hedónicos e índices de
precios de viviendas. *Documento de trabajo*, 1--33.
https://www.researchgate.net/profile/Facundo-Piguillem/publication/237743314_Precios_Hedonicos_e_Indices_de_Precios_de_Viviendas/links/55b3d3cd08aed621de01107b/Precios-Hedonicos-e-Indices-de-Precios-de-Viviendas.pdf

Di Marco, L. E. (1978). Teoría económica y deporte. *Anales de la
Asociación Argentina de Economía Política*, 371-366.
https://bd.aaep.org.ar/anales/works/works1978/dimarco.pdf

Dobson, S. J., & Gerrard, B. (October de 1999). The Determination of
Player Transfer Fees in English Professional Soccer. *Journal of Sport
Management, 13*, 259-279. https://doi.org/10.1123/jsm.13.4.259

Feenstra, R. C. (1995). Exact hedonic price indexes. *Review of
Economics and Statistics, 77*, 634-653.

Fort, R. (2005). The Golden Anniversary of "The Baseball Players' Labor
Market". *Journal of Sports Economics, 6*, 347-358.
https://doi.org/10.1177/1527002505281226

Fort, R., & Quirk, J. (1995). Cross-Subsidization, Incentives, and
Outcomes in Professional Team Sports Leagues. *Journal of Economic
Literature, 33*, 1265--1299. http://www.jstor.org/stable/2729122

Gerrard, B. (2002). The muscle drain, coubertobin‐type taxes and the
international transfer system in association football. *European Sport
Management Quarterly, 2*, 47-56.
https://doi.org/10.1080/16184740208721911

Gwartney, J., & Haworth, C. (July de 1974). Employer Costs and
Discrimination: The Case of Baseball. *Journal of Political Economy,
82*, 873-881.
https://ideas.repec.org/a/ucp/jpolec/v82y1974i4p873-81.html

Harding, J. P., Rosenthal, S. S., & Sirmans, C. F. (2003). Estimating
bargaining power in the market for existing homes. *Review of Economics
and statistics, 85*, 178--188.

Heckman, J. J. (1979). Sample Selection Bias as a Specification Error.
*Econometrica, 47*, 153--161. Retrieved 3 de February de 2025, from
http://www.jstor.org/stable/1912352

Hill, J. R., & Spellman, W. (1983). Professional Baseball: The Reserve
Clause and Salary Structure. *Industrial Relations: A Journal of Economy
and Society, 22*, 1-19.
https://doi.org/10.1111/j.1468-232X.1983.tb00249.x

Hill, J. R., & Spellman, W. (1984). Pay Discrimination in Baseball: Data
from the Seventies. *Industrial Relations: A Journal of Economy and
Society, 23*, 103-112.
https://doi.org/10.1111/j.1468-232X.1984.tb00879.x

Holahan, W. L. (1978). The Long-Run Effects of Abolishing the Baseball
Player Reserve System. *The Journal of Legal Studies, 7*, 129--137.
http://www.jstor.org/stable/724067

Hylan, T. R., Lage, M. J., & Treglia, M. (1996). The Coase theorem, free
agency, and major league baseball: A panel study of pitcher mobility
from 1961 to 1992. *Southern Economic Journal*, 1029--1042.

Kennedy, P. (1981). Estimation with Correctly Interpreted Dummy
Variables in Semilogarithmic Equations \[The Interpretation of Dummy
Variables in Semilogarithmic Equations\]. *American Economic Review,
71*. https://EconPapers.repec.org/RePEc:aea:aecrev:v:71:y:1981:i:4:p:801

Krautmann, A. C., & Oppenheimer, M. (1994). Free agency and the
allocation of labor in Major League Baseball. *Managerial and Decision
Economics, 15*, 459--469.

Kuper, S., & Szymanski, S. (2012). *Soccernomics.* New York: Ed.
HarperCollins Publishers.

Lancaster, K. J. (1966). A New Approach to Consumer Theory. *Journal of
Political Economy, 74*, 132--157.

Lozano, F. M. (2016). *Discusion crı́tica sobre valoración y revelación
contable de "los derechos de traspaso" de los jugadores profesionales en
las SAD clubes de futbol.* Ph.D. dissertation, Universidad de Sevilla.

Lozano, F. M., & Gallego, A. C. (November de 2011). Deficits of
accounting in the valuation of rights to exploit the performance of
professional players in football clubs. A case study. *Metrika:
International Journal for Theoretical and Applied Statistics, 22*,
335-357. https://doi.org/10.1007/s00187-008-0047-2

Medcalfe, S. (2008). English league transfer prices: is there a racial
dimension? A re-examination with new data. *Applied Economics Letters,
15*, 865-867. https://doi.org/10.1080/13504850600949178

Mohamed, E.-H., & Quirk, J. (1971). An Economic Model of a Professional
Sports League. *Journal of Political Economy, 79*, 1302-19.
https://EconPapers.repec.org/RePEc:ucp:jpolec:v:79:y:1971:i:6:p:1302-19

Nardinelli, C., & Simon, C. (August de 1990). Customer Racial
Discrimination in the Market for Memorabilia: The Case of Baseball\*.
*The Quarterly Journal of Economics, 105*, 575-595.
https://doi.org/10.2307/2937891

Neale, W. C. (1964). The Peculiar Economics of Professional Sports: A
Contribution to the Theory of the Firm in Sporting Competition and in
Market Competition. *The Quarterly Journal of Economics, 78*, 1--14.
http://www.jstor.org/stable/1880543

Poli, R., Besson, R., & Ravenel, L. (2021). Econometric approach to
assessing the transfer fees and values of professional football players.
*Economies, 10*, 4.

Poli, R., Besson, R., & Ravenel, L. (2024). Statistical Modeling of
Football Players' Transfer Fees Worldwide. *International Journal of
Financial Studies, 12*. https://doi.org/10.3390/ijfs12030093

Quirk, J. P., & El-Hodiri, M. M. (1974). *The economic theory of a
professional sports league.* Brookings Institution.

Raimondo, H. J. (01 de June de 1983). Free agents impact on the labor
market for baseball players. *Journal of Labor Research, 4*, 183--193.
https://doi.org/10.1007/BF02685176

Reilly, B., & Witt, R. (1995). English league transfer prices: is there
a racial dimension? *Applied Economics Letters, 2*, 220-222.
https://EconPapers.repec.org/RePEc:taf:apeclt:v:2:y:1995:i:7:p:220-222

Rodríguez, M. S., Hassan, A. R., & Coad, A. (2019). Uncovering Value
Drivers of High Performance Soccer Players. *Journal of Sports
Economics, 20*, 819-849. https://doi.org/10.1177/1527002518808344

Rodriguez, P. (2012). La economía del deporte. *Estudios de Economía
Aplicada, 30*, 387-417.
https://www.redalyc.org/articulo.oa?id=30124481001

Rosen, S. (1974). Hedonic prices and implicit markets: product
differentiation in pure competition. *Journal of political economy, 82*,
34-55.

Rottenberg, S. (1956). The Baseball Player\'s Labor Market. *Journal of
Political Economy, 64*, 1467-1476.

Royston, P. (February de 1992). Comment on sg3.4 and an Improved
D\'Agostino Test. *Stata Technical Bulletin, 1*.

Ruijg, J., & van Ophem, H. (2015). Determinants of football transfers.
*Applied Economics Letters, 22*, 12-19.
https://doi.org/10.1080/13504851.2014.892192

Scully, G. W. (1974). Pay and Performance in Major League Baseball.
*American Economic Review, 64*, 915-30.
https://EconPapers.repec.org/RePEc:aea:aecrev:v:64:y:1974:i:6:p:915-30

Sloane, P. (1971). The Economics of Professional Football: The Football
Club as a Utility Maximiser. *Scottish Journal of Political Economy,
18*, 121-46.
https://EconPapers.repec.org/RePEc:bla:scotjp:v:18:y:1971:i:2:p:121-46

Sloane, P. J. (1969). The Labour Market In ProfessionalL Football.
*British Journal of Industrial Relations, 7*, 181-199.
https://doi.org/10.1111/j.1467-8543.1969.tb00560.x

Sloane, P. J. (June de 2006). *Rottenberg and the Economics of Sport
after 50 Years: An Evaluation.* IZA Discussion Papers, Institute for the
Study of Labor (IZA). https://ideas.repec.org/p/iza/izadps/dp2175.html

Speight, A., & Thomas, D. (1997). Arbitrator Decision-Making in the
Transfer Market: an Empirical Analysis. *Scottish Journal of Political
Economy, 44*, 198-215. https://doi.org/10.1111/1467-9485.00053

Szymanski, S., & Smith, R. (1997). The English Football Industry:
profit, performance and industrial structure. *International Review of
Applied Economics, 11*, 135-153.
https://EconPapers.repec.org/RePEc:taf:irapec:v:11:y:1997:i:1:p:135-153

Taylor, L. (February de 2003). Hedonics. En *A Primer on Nonmarket
Valuation* (págs. 331-393). Springer Netherlands.

Transfermarket. (2024). *Definición de valor de mercado*. Retrieved 26
de November de 2024, from Definición de valor de mercado:
https://web.archive.org/web/20241126183939/https://www.transfermarkt.es/definicion-de-valor-de-mercado/thread/forum/407/thread_id/2975

Tunaru, R., Clark, E., & Viney, H. (2005). An option pricing framework
for valuation of football players. *Review of financial economics, 14*,
281--295.

Vrooman, J. (1995). A general theory of professional sports leagues.
*southern economic journal*, 971--990.

Weisberg, S. (2005). *Applied linear regression* (Vol. 528). Ed. John
Wiley & Sons.

Wooldridge, J. M. (2010). Introducción a la Econometrı́a: Un enfoque
moderno 4\\textordfeminine Edición. *Michigan State Universitty*.

**\
**

# Anexo 1 {#anexo-1 .Titulo-1-sin-numeracion}

+-----------+-------------------------+---------+---+----------------+
| []{#_Ref  |                         |         |   |                |
| 197959854 |                         |         |   |                |
| .anc      |                         |         |   |                |
| hor}Tabla |                         |         |   |                |
| 13\       |                         |         |   |                |
| *E        |                         |         |   |                |
| stimación |                         |         |   |                |
| Probit    |                         |         |   |                |
| sobre la  |                         |         |   |                |
| pro       |                         |         |   |                |
| babilidad |                         |         |   |                |
| de tener  |                         |         |   |                |
| precio:   |                         |         |   |                |
| Variable  |                         |         |   |                |
| De        |                         |         |   |                |
| pendiente |                         |         |   |                |
| tie       |                         |         |   |                |
| ne_precio |                         |         |   |                |
| (1 =      |                         |         |   |                |
| precio \> |                         |         |   |                |
| 0)*       |                         |         |   |                |
+===========+=========================+=========+===+================+
|           | **Modelo Probit**       |         |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | **(Variable             |         |   |                |
|           | dependiente:            |         |   |                |
|           | tiene_precio)**         |         |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | Variable                | Coef    |   |                |
|           |                         | iciente |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | edad                    | 0.220   | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
+-----------+-------------------------+---------+---+----------------+
|           | Delantero               | 0.220   |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | Medio campo             | 0.330   |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | Portero                 | 0.701   | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
+-----------+-------------------------+---------+---+----------------+
|           | pj                      | 0.160   | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
+-----------+-------------------------+---------+---+----------------+
|           | goles                   | 0.122   | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
+-----------+-------------------------+---------+---+----------------+
|           | lvalor_plantel_to       | 0.830   | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
+-----------+-------------------------+---------+---+----------------+
|           | lvalor_plantel_from     | 0.270   |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | rest_transfer_window    | 0.168   | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
+-----------+-------------------------+---------+---+----------------+
|           | season                  | 0.310   |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | N                       | > 769   |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | Log Likelihood          | -       |   |                |
|           |                         | 261.273 |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | LR \$\\chi\^2\$(10)     | 543.450 | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
|           |                         |         | \ |                |
|           |                         |         | * |                |
+-----------+-------------------------+---------+---+----------------+
|           | Pseudo R\$\^2\$         | 0.510   |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | Iteraciones             | > 5     |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | \* p\<0.1, \*\*         |         |   |                |
|           | p\<0.05, \*\*\* p\<0.01 |         |   |                |
+-----------+-------------------------+---------+---+----------------+
|           | El modelo convergió en  |         |   |                |
|           | 5 iteraciones           |         |   |                |
+-----------+-------------------------+---------+---+----------------+

**\
**

# Anexo 2 {#anexo-2 .Titulo-1-sin-numeracion}

  -----------------------------------------------------------------------------------------------------
  []{#_Ref197966963                                                                           
  .anchor}Tabla 14\                                                                           
  *Regresión del                                                                              
  valor de clubes en                                                                          
  el período                                                                                  
  2020-2023*                                                                                  
  ------------------- ------------------- ---------------- -------- ---------------- -------- ---------
                                          \(1\)                     \(2\)                     

                                          lvalor_plantel            lvalor_plantel            

                      len_plantel         0.042            \*\*\*   0.035            \*\*\*   

                                          (0.004)                   (0.003)                   

                      league_position     0.042            \*\*\*   0.200                     

                                          (0.010)                   (0.005)                   

                      puntos              0.160                     0.000                     

                                          (0.011)                   (0.006)                   

                      partidos_jugados    0.170                     0.025            \*\*     

                                          (0.017)                   (0.010)                   

                      goles_favor         0.035            \*\*\*   0.027            \*\*\*   

                                          (0.008)                   (0.004)                   

                      goles_contra        0.600                     0.015            \*\*\*   

                                          (0.008)                   (0.004)                   

                      2021.season         0.218            \*\*     0.450                     

                                          (0.100)                   (0.059)                   

                      2022.season         0.800                     0.103            \*       

                                          (0.097)                   (0.057)                   

                      2023.season         0.150                     0.119            \*       

                                          (0.110)                   (0.062)                   

                      2.div               -1.228           \*\*\*   -1.351           \*\*\*   

                                          (0.082)                   (0.054)                   

                      3.div               -2.329           \*\*\*   -3.317           \*\*\*   

                                          (0.302)                   (0.169)                   

                      no_arg              0.154                                               

                                          (0.097)                                             

                       Control por        No                        Si                        
                      nacionalidad                                                            

                      N                   777                       746                       

                      R2                  0.461                     0.864                     

                      Breush-Pagan        \-                        0.129                     

                      Errores robustos    Si                        No                        

                      Errores estándar                                                        
                      entre paréntesis                                                        

                      \* p\<0.1, \*\*                                                         
                      p\<0.05, \*\*\*                                                         
                      p\<0.01                                                                 
  -----------------------------------------------------------------------------------------------------

**\
**

  -----------------------------------------------------------------------------
  []{#_Toc198472217                                               
  .anchor}Tabla 15\                                               
  *Efectos fijos de                                               
  la nacionalidad del                                             
  equipo en el                                                    
  logaritmo del valor                                             
  del plantel                                                     
  respecto de                                                     
  Argentina. Modelo                                               
  (2)*                                                            
  ------------------- ------------- --------- --------- --------- -------------
  País                Coeficiente   SD        t         p-valor   

  Inglaterra          3.078 \*\*\*  0.164     18.810    0.000     

  Alemania            2.262 \*\*\*  0.231     9.800     0.000     

  Francia             2.232 \*\*\*  0.312     7.150     0.000     

  Italia              2.200 \*\*\*  0.128     17.230    0.000     

  España              2.049 \*\*\*  0.116     17.630    0.000     

  Países Bajos        1.210 \*\*\*  0.384     3.150     0.002     

  Portugal            1.200 \*\*\*  0.144     8.350     0.000     

  Corea del Sur       1.056 \*\*    0.526     2.010     0.045     

  Bélgica             1.019 \*\*\*  0.201     5.070     0.000     

  Austria             0.955 \*      0.571     1.670     0.095     

  Rusia               0.943 \*\*\*  0.164     5.750     0.000     

  México              0.881 \*\*\*  0.097     9.060     0.000     

  Estados Unidos      0.857 \*\*\*  0.163     5.250     0.000     

  Suiza               0.846 \*\*    0.378     2.240     0.026     

  Brasil              0.718 \*\*\*  0.109     6.600     0.000     

  Turquía             0.553 \*\*\*  0.179     3.100     0.002     

  Japón               0.286         0.379     0.760     0.450     

  Escocia             0.242         0.529     0.460     0.647     

  Grecia              0.062         0.130     0.480     0.632     

  Polonia             0.049         0.528     0.090     0.926     

  Arabia Saudita      -0.018        0.206     -0.090    0.929     

  Hungría             -0.283        0.376     -0.750    0.451     

  Israel              -0.311        0.375     -0.830    0.408     

  Colombia            -0.311 \*\*\* 0.104     -3.000    0.003     

  Ucrania             -0.315        0.312     -1.010    0.313     

  Croacia             -0.350        0.528     -0.660    0.508     

  Ecuador             -0.377 \*\*\* 0.140     -2.700    0.007     

  Chile               -0.396 \*\*\* 0.089     -4.440    0.000     

  Emiratos Árabes     -0.398 \*     0.208     -1.910    0.057     
  Unidos                                                          

  Rumania             -0.432 \*     0.239     -1.800    0.072     

  Irán                -0.518        0.524     -0.990    0.324     

  Sudáfrica           -0.563        0.524     -1.070    0.283     

  Serbia              -0.638 \*     0.376     -1.700    0.090     

  Chipre              -0.663 \*\*\* 0.242     -2.730    0.006     

  República Checa     -0.725        0.525     -1.380    0.168     

  Perú                -0.808 \*\*\* 0.137     -5.900    0.000     

  China               -0.822        0.528     -1.560    0.120     

  Bulgaria            -0.833 \*\*   0.374     -2.230    0.026     

  Uruguay             -0.907 \*\*\* 0.102     -8.900    0.000     

  Azerbaiyán          -0.920 \*\*   0.376     -2.450    0.015     

  Eslovenia           -0.924 \*     0.529     -1.750    0.081     

  Uzbekistán          -0.960 \*     0.525     -1.830    0.068     

  Albania             -0.985 \*     0.530     -1.860    0.063     

  Paraguay            -1.008 \*\*\* 0.143     -7.030    0.000     

  Australia           -1.091 \*\*   0.528     -2.070    0.039     

  Georgia             -1.111 \*\*   0.530     -2.090    0.037     

  India               -1.326 \*\*   0.532     -2.490    0.013     

  Malta               -1.350 \*\*   0.526     -2.570    0.010     

  Catar               -1.551 \*\*\* 0.382     -4.060    0.000     

  Venezuela           -1.679 \*\*\* 0.241     -6.960    0.000     

  Bosnia y            -1.684 \*\*\* 0.526     -3.200    0.001     
  Herzegovina                                                     

  Honduras            -1.718 \*\*\* 0.527     -3.260    0.001     

  Bolivia             -1.826 \*\*\* 0.183     -10.000   0.000     

  Costa Rica          -1.926 \*\*\* 0.377     -5.110    0.000     

  Lituania            -1.940 \*\*\* 0.526     -3.690    0.000     

  Malasia             -2.779 \*\*\* 0.553     -5.030    0.000     

  Panamá              -2.783 \*\*\* 0.534     -5.210    0.000     

  Luxemburgo          -2.918 \*\*\* 0.530     -5.510    0.000     
  -----------------------------------------------------------------------------

Errores estándar en paréntesis. p \< 0.1, \*\* p \< 0.05, \*\*\* p \<
0.01

[^1]: https://web.archive.org/web/20230323195123/https://www.infobae.com/teleshow/2022/12/19/hubo-record-de-audiencia-televisiva-en-argentina-y-francia-por-la-final-de-la-copa-del-mundo/

[^2]: https://web.archive.org/web/20240422162613/https://www.perfil.com/noticias/modo-fontevecchia/el-debate-presidencial-en-numeros-pico-de-485-puntos-de-rating-modof.phtml

[^3]: Para un análisis sobre la obtención de la información pueden
    revisarse los scripts usados para scrapear la web en
    <https://github.com/jfjvallaris/Master-Tesis>

[^4]: Un ejemplo inspirado en un
    [tuit](https://x.com/Vdot_Spain/status/1861737518640558256) de Jesús
    Lagos, fundador de scoutanalyst.com, puede referirse a la búsqueda
    de un portero que *saca bien con los pies*. Un grupo de personas que
    *saben de fútbol*, podrían determinar cuál es el mejor o, al menos,
    quienes lo hacen. Sin embargo, para operacionalizarlo en
    estadísticas medibles, habría que localizar, entre otras, porteros
    que han sido capaces de dar un pase de media distancia sobre un
    jugador que ha podido recibir y conducir, generando un pase o una
    conducción progresiva. Esto, sin entrar en la discusión de sí, el
    éxito o fracaso de la acción es producto del portero o del receptor
    del pase.

[^5]: <https://web.archive.org/web/20220425224158/https://elpais.com/deportes/2018/09/21/actualidad/1537541325_316666.html>

[^6]: <https://web.archive.org/web/20241102041931/https://indicepolitico.com/algoritmo-para-calcular-el-precio-de-los-futbolistas/>

[^7]: Estas variables son: El nivel general de ingresos, el precio de
    las entradas en relación con los precios de los sustitutos
    recreativos, y la bondad de los sustitutos, el tamaño de la
    población del territorio en el que el equipo tiene su estadio, el
    tamaño y la conveniencia de la ubicación del estadio, y la posición
    media del equipo durante la temporada en la competición de su liga.
    Es una función negativa de la bondad de los sustitutos del tiempo
    libre en la zona y de la dispersión de porcentajes de juegos ganados
    por los equipos de la liga.

[^8]: Este sistema de *draft* no debe confundirse con el sistema
    empleado en el básquet o el draft para agentes libres del béisbol
    que comenzó en 1965. Este sistema del béisbol, que podría
    denominarse *draft de ligas menores*, consistía en que cualquier
    jugador, en un nivel particular, por un período específico de años,
    podía ser seleccionado para jugar en una liga superior a cambio de
    un precio mínimo establecido. Por supuesto, ante la pérdida de
    jugadores a ese precio, los equipos simplemente los vendieron a
    mayor valor y solo quedaron disponibles jugadores que valían el
    precio del *draft*, por lo que casi nadie fue seleccionado (Fort,
    2005)

[^9]: Esta circunstancia no aplicaría a todos los deportes y países. Por
    ejemplo, para el fútbol argentino, los clubes son Asociaciones
    Civiles Sin Fines de Lucro, por lo cual no tienen propietarios en el
    sentido que los define Rottenberg (1956)

[^10]: En referencia a los boxeadores Joe Louis y Max Schmeling, que
    protagonizaron en los años 30 uno histórico enfrentamiento
    deportivo.

[^11]: El desarrollo de esta sección está basado en Costanigro et al.
    (2012), Desormeaux y Piguillem (2003) y Taylor (2003) conjuntamente.

[^12]: Para una mayor comprensión de las reglas utilizadas, se puede
    revisar el código de Python en
    <https://github.com/jfjvallaris/Master-Tesis/blob/main/scrapper/tmkt_rendimiento.py>

[^13]: https://ec.europa.eu/eurostat/databrowser/product/page/PRC_HICP_MIDX

[^14]: En determinadas circunstancias, puede existir alguna
    compensación. Por ejemplo, cuando se cuida un jugador haciendo que
    descanse un partido de liga donde se espera que obtenga un
    rendimiento mayor, en favor de tenerlo disponible al 100% para un
    partido internacional, en el cual sus estadísticas de rendimiento
    esperadas son inferiores al enfrentarse a rivales más difíciles. A
    los efectos estadísticos, ambos partidos suman uno y los
    rendimientos se tienen en cuenta como si fuesen iguales, pero se
    espera que un gol en una semifinal de la Copa Libertadores sea más
    valorable que el tercer gol contra un rival de media tabla en la
    liga local.

[^15]: Estos efectos se calcularon aplicando la corrección de Kennedy,
    que ajusta por la varianza de los coeficientes estimados para evitar
    sesgos en la interpretación de los impactos porcentuales.

[^16]: Notar que, para el caso de una variable transformada en
    logaritmos, el promedio de las observaciones equivale al logaritmo
    de la media geométrica de la variable en nivel. Por lo cual, al
    calcular los efectos marginales porcentuales promedio, nos referimos
    a la media geométrica y no a la media aritmética. Formalmente:

    $$\frac{\sum_{i = 1}^{n}{ln(x_{i})}}{n} = ln\left( \left( \prod_{i = 1}^{n}{x_{i})} \right)^{(^{\frac{1}{n}})} \right)$$
