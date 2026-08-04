# **- Enfatizar el aporte del modelo Tobit y la discusión sobre el poder de negociación.** 

_Para realizar, no hay respuesta. Aunque esta desenfatizado intencionalmente_ 

# **- Preparar respuestas sobre las limitaciones del scrapping y la representatividad de los datos.** 

# **_Con los controles adecuados, las limitaciones no las considero muy diferentes a las de cualquier método de obtención de datos primarios._** 

_Los resultados del scrapping fueron controlados con tomas de muestras aleatorias y comparación, comprobación manual de missing values y rearmado de lógica, completado búsqueda manual._ 

# **- Destacar la relevancia del estudio para políticas deportivas y económicas.** 

_Realmente no creo que todo trabajo deba proponer políticas. En este caso más allá de la política existente de fair play financiero, los derechos de formación y la venta por porcentaje menos al 100% o partes de futuras ventas no considero que haya que intervenir en el mercado_ 

# **- ¿Puede existir un problema de medición? Los precios del sitio son en general el valor esperado y no el pago efectivamente pagado. Si este problema de medición tiene algún patrón (por ejemplo, en los valores más altos), tal vez pueda haber una posibilidad de sesgo.** 

_Habría que conversar sobre la diferencia entre precio y valor en el sitio web._ 

# **- En línea con lo anterior, podría existir un sesgo de clasificación significativo si una parte de los valores faltantes no son transferencias gratuitas, sino que son, en realidad, precios positivos desconocidos.** 

_Es poco probable, pero posible. La información que circula más rápido es la de los pases con precio y la menor proporción. Podrían existir casos, pero no se pudo corroborar. De existir, lo más probable es que sean de precios bajos o jugadores poco conocidos_ 

**- La omisión del modelo Heckman puede ser un problema (si bien se aclara por qué no se lo uso), ya que el modelo Tobit es más restrictivo: asume que los factores que afectan la probabilidad de tener un precio positivo son los mismos que afectan el nivel del precio. Si hay variables (no observadas o no incluidas) que influyen en la decisión de transferir a un jugador libre (precio = 0) de forma distinta a las que influyen en el precio, el modelo Tobit podría tener parámetros sesgados e inconsistentes.** 

_En este caso me gusta invertir la carga de la prueba porque a diferencia de una ecuación de mincer donde heckman ajusta porque hay factores que inciden en la_ 

_probabilidad de ingresar al mercado de trabajo y el salario 0 indica esa condición. En este caso, la modelización de sicha situación sería comparable a creer que todos los jugadores están potencialmente en venta y aquellos_ **_no transferidos_** _tienen precio cero. De hecho, es lo que realiza Carmichael (1999)._ 

_La situación del modelo aplicado en esta investigación es más comparable al precio 0 que aportan los trabajadores voluntarios que no reciben contraprestación en el mercado, pero si realizan actividades para otros._ 

**- El periodo de estudio (2021-2024) es de alta inflación en Argentina. Si bien se deflactan las variables monetarias con el Índice de Precios al Consumidor Armonizado de Eurostat (HICP), se podría criticar si un índice europeo es el deflactor más apropiado para las transacciones que involucran clubes argentinos, donde las negociaciones están muy influenciadas por el riesgo cambiario y la dinámica de la inflación local (o la cotización del dólar en mercados no oficiales).** 

_Acepto la crítica, pero al ser jugadores comercializados internacionalmente, pensaría más en controlar por ITCRM al dólar blue y mantener el deflactor internacional en lugar de deflactar por precios locales._ 

_De hecho, en pruebas posteriores cambiando el control temporal por dummies de ventanas de transferencias, el ITCRM blue se vuelve significativo con el signo esperado._ 

**- Se puede argumentar que el modelo hedónico clásico, que asume un equilibrio competitivo o una negociación entre agentes racionales maximizadores de beneficios, podría no capturar completamente las externalidades sociales, la aversión al riesgo de descenso, o la presión de la hinchada que influyen en las decisiones de transferencia de un club que busca maximizar su utilidad deportiva/social. El trabajo se basa en el modelo de precios hedónicos y el concepto de poder de negociación, pero también establece correctamente que los clubes argentinos son Sociedades Civiles Sin Fines de Lucro y, por lo tanto, son maximizadores de utilidad (que incluye el éxito deportivo, la asistencia y beneficios económicos no negativos) y no** . **solo maximizadores de beneficios** 

_La pregunta que me surge de leer este tópico y el próximo, es si dichos factores no inciden en el poder de negociación y hasta que punto son independientes tanto del poder de negociación como de la valorización de los planteles. De hecho asumir las asimetrías comprador-vendedor permite que las curvas WTP-WTA del modelo de negociación absorba estas particularidades. El costo de esta ventaja es una mayor dependencia del supuesto de independencia de las características no observadas (P. 578)._ 

**- Se puede pensar que el valor del plantel del club vendedor (lvalor_plantel_v) no solo mide "poder de negociación", sino también la calidad promedio del jugador o el efecto vitrina del club. Los jugadores de un plantel más valioso son inherentemente percibidos como de mayor calidad y, por lo tanto, obtendrían precios más altos, independientemente del poder de negociación. La tesis debe ser muy clara en cómo su modelo aísla el efecto del poder de negociación del simple efecto de la calidad implícita del club.** 

_Contestado arriba_ 

**- El trabajo encuentra un efecto lineal negativo de la edad en el precio de los jugadores, sin un efecto cuadrático (es decir, el precio no aumenta con la experiencia para luego caer). Esto es contrario a la mayoría de la literatura internacional que sugiere un pico de valor alrededor de los 25-26 años. Si este resultado es una característica real del mercado argentino o una limitación del modelo debe discutirse. ¿Es posible que la experiencia de los jugadores de 21-26 años en la liga argentina no esté siendo capturada adecuadamente por las variables de rendimiento usadas (pj y goles), o que la fuga de talento joven al exterior impida que los precios más altos de los jugadores en su prime se observen en el mercado local?** 

_Considero que ambas interpretaciones son parte del fenómeno, es más un debate filosófico. Desde lo empírico, ambos efectos (lineal y cuadrático) pueden modelarse, pero para un país de perfil exportador de talento, considero más esperable que el potencial domine a la experiencia._ 

_La pregunta central es ¿Cuánto pueden valorar los clubes del fútbol argentino la ganancia de rendimiento por experiencia con premios 46 veces menores que el fútbol brasilero? Los ingresos determinan cuanto se pueden expresar las valoraciones._ 

**-La tesis utiliza "rest_ventana_transfer" (ventanas de transferencia restantes) como variable clave. Aunque es un proxy útil, la variable estándar en la literatura es el tiempo restante del contrato en años/meses. Se puede criticar que la "ventana de transferencia" es una medida más gruesa que podría no reflejar con precisión la urgencia o el apalancamiento del club vendedor, que se maximiza con menos de un año de contrato** . 

_La variable es solo una forma alternativa de trabajar en semestres. Debido a que, si bien los contratos si vencen cada seie meses, las transferencias suelen realizarse en periodos diferentes, pero dos veces al año._ 

_Además, el Reglamento sobre el Estatuto y la Transferencia de Jugadores que reglamente las transferencia, utiliza como medida de tiempo los periodos de_ 

_<u>inscripción, lo cual puede ser un indicio de que las decisiones se toman en dicha</u> unidad de tiempo_ 

