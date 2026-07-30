# Validación de la hipótesis sobre concentración de ceros y pendientes

Revisión econométrica encargada el 2026-07-29. Material accesorio: no forma
parte de la tesis ni de la presentación.

## La hipótesis evaluada

Que el cambio de pendiente al incluir los ceros sin corregir (MCO Censurado
frente a MCO Truncado) informa **dónde se concentran los ceros** en el espacio
de los regresores, y que el probit sería la evidencia de esa concentración.

Ejemplos originales:

- **edad**: la pendiente negativa se empina (−0.108 → −0.657) porque los ceros
  se concentran en edades altas.
- **lvalor_plantel_c**: se empina (0.542 → 2.442) porque los ceros se
  concentran en compradores de bajo valor.
- **lvalor_plantel_v**: la relación se destruye (0.307 → −0.008) porque los
  ceros se reparten entre vendedores de cualquier valor.

## Veredicto

**Las conclusiones sustantivas son correctas. El argumento que las sostiene, no.**

---

## 1. Dónde están los ceros: se verifica directo

Fracción de transferencias sin precio por quintil del regresor:

| edad | frac. cero | | lvalor plantel **comprador** | frac. cero | | lvalor plantel **vendedor** | frac. cero |
|---|---|---|---|---|---|---|---|
| 18–23 | 0.159 | | q1 (12.7–15.9) | 0.903 | | q1 | 0.526 |
| 23–25 | 0.362 | | q2 | 0.721 | | q2 | 0.597 |
| 25–28 | 0.464 | | q3 | 0.417 | | q3 | 0.542 |
| 28–31 | 0.714 | | q4 | 0.292 | | q4 | 0.445 |
| 31–39 | 0.846 | | q5 (17.7–20.7) | 0.139 | | q5 | 0.366 |

`rest_ventana`: 0.649 / 0.339 / 0.118.

**Las tres afirmaciones se sostienen**: concentración monótona en edades altas,
concentración en compradores pobres, y reparto comparativamente plano entre
vendedores.

Efectos parciales promedio del probit sobre P(precio > 0):

| Variable | APE sobre P | p |
|---|---|---|
| lvalor_plantel_c | +0.158 | <0.001 |
| portero | +0.134 | 0.011 |
| edad | −0.042 | <0.001 |
| rest_ventana | +0.032 | <0.001 |
| goles | +0.023 | <0.001 |
| pj | +0.003 | 0.008 |
| lvalor_plantel_v | −0.005 | 0.654 |
| temporada | +0.014 | 0.246 |
| delantero | −0.042 | 0.226 |
| mediocampista | +0.006 | 0.858 |

Lectura: un año más de edad reduce 4.2 puntos porcentuales la probabilidad de
que la transferencia tenga precio; 10% más de valor del plantel comprador la
aumenta 1.6 puntos.

## 2. Por qué el MCO censurado no prueba nada

Existe una **identidad algebraica exacta**. Con $w$ = log-precio,
$D = \mathbb{1}\{precio>0\}$, $y = D\,w$ (los ceros codificados en 0), y
$\bar w_+$ = media del log-precio entre las positivas:

$$\hat\beta_{cens} = \underbrace{\bar w_+ \cdot \hat\beta_{MPL}}_{\text{margen extensivo}} + \underbrace{\hat\beta_{resid}}_{\text{margen intensivo}}$$

En estos datos $\bar w_+ = 13.976$. Descomposición verificada (la suma reproduce
el coeficiente censurado dígito a dígito):

| Variable | β censurado | extensivo | intensivo | share extensivo |
|---|---|---|---|---|
| edad | −0.6572 | −0.6459 | −0.0114 | 0.98 |
| portero | 1.8473 | 1.8203 | 0.0270 | 0.99 |
| pj | 0.0639 | 0.0639 | −0.0001 | 1.00 |
| goles | 0.2745 | 0.2715 | 0.0030 | 0.99 |
| lvalor_plantel_c | 2.4423 | 2.2802 | 0.1620 | 0.93 |
| rest_ventana | 0.5525 | 0.4728 | 0.0797 | 0.86 |
| temporada | 0.2276 | 0.1177 | 0.1099 | 0.52 |
| delantero | −0.2619 | −0.4178 | 0.1559 | 1.60 |
| mediocampista | 0.1672 | −0.0163 | 0.1834 | −0.10 |
| lvalor_plantel_v | −0.0079 | −0.1504 | 0.1424 | 18.9 |

Consecuencias:

- Para las seis variables que "se empinan", entre 86% y 100% del coeficiente
  censurado **es** el margen extensivo multiplicado por 13.976.
- El factor "4 a 10x" **es** ese 13.976, que a su vez sale de haber codificado
  los ceros en $\ln(1)=0$ mientras la cota está en 10.714.

Recodificando los ceros:

| Codificación | media \|censurado/truncado\| |
|---|---|
| ln(1) = 0 (la usada) | 4.00 |
| ln(45.000) = 10.714 (la cota) | 1.17 |
| ln(45.249) = 10.720 (mínimo observado) | 1.16 |

Con los ceros en la cota, los coeficientes se acercan a la predicción de
Greene (1981), $\beta_{Tobit} \cdot P$: edad −0.162 vs −0.158; pj 0.0149 vs
0.0165; goles 0.0664 vs 0.0597; portero 0.452 vs 0.490.

**La magnitud del empinamiento no contiene información sustantiva sobre los
ceros: refleja la constante de codificación.**

## 3. La comparación con el probit es circular

- corr($\hat\beta_{cens}$, $13.976 \cdot \hat\beta_{MPL}$) = **0.9969**
- corr($\hat\beta_{cens}$, $\hat\beta_{trunc}$) = 0.704

El MCO censurado es una imagen casi afín del modelo de probabilidad lineal, y
el probit estima ese mismo margen extensivo con otro link. "El probit confirma
lo que muestra el censurado" equivale a "el MPL confirma una reescala del MPL".

La regularidad observada (probit significativo → inflación 4–10x) es
estadísticamente detectable —Spearman entre |t| del probit y el ratio de
coeficientes = 0.745, p = 0.013, n = 10— pero está **implicada por el álgebra**,
no es corroboración.

Contraejemplos: **delantero** invierte el signo (+0.263 → −0.262) con probit no
significativo (p = 0.226); **mediocampista** tiene share extensivo −0.10, es
casi puro margen intensivo.

## 4. Por qué McDonald y Moffitt (1980) no sirve de ancla

Su descomposición del efecto marginal Tobit tiene ambos términos proporcionales
a $\beta_j$, de modo que el share extensivo es **un escalar común a todos los
regresores**. Calculado sobre este Tobit: 0.8005 para las diez variables, con
desvío entre regresores de 6.4e−16.

La estructura de índice único **prohíbe por construcción** la heterogeneidad
entre variables que la hipótesis quiere documentar. Invocarla para sostener
"los ceros se concentran en edades altas pero no en vendedores caros" es
autocontradictorio.

## 5. El caso `lvalor_plantel_v` se refuta con su propia lógica

Si el mecanismo fuera "los ceros se reparten y por eso no aportan pendiente",
el término extensivo sería ≈ 0 y quedaría $\hat\beta_{cens} \approx P \cdot
\hat\beta_{trunc} \approx 0.15$, positivo y apreciable.

Lo que ocurre es una **cancelación**: extensivo = −0.1504, intensivo = +0.1424,
suma = −0.0079. El coeficiente se anula por compensación entre dos términos no
nulos de signo opuesto.

## 6. Nota adicional no solicitada (fuera de alcance)

El revisor corrió el test de Cragg (1971), que compara el Tobit contra el
modelo de dos partes en el que está anidado: LR = 464.4, gl = 11, p ≈ 1.3e−92.
La fuente del rechazo es la escala: σ del Tobit = 1.611 contra σ dentro de las
positivas = 0.727.

**Sin acción.** El test de Cragg no figura en Wooldridge, *Introducción a la
econometría*, 4ª ed. (sección 17.2 plantea la comparación de coeficientes, no
una condición de descarte), así que queda fuera de la bibliografía obligatoria
del trabajo.

## Recomendaciones

1. **Sacar** la lectura del cambio de pendiente del censurado como evidencia
   sobre la ubicación de los ceros.
2. **Conservar** el MCO censurado con la función que ya tiene: mostrar qué pasa
   si se ignora la censura.
3. **Reemplazar** la evidencia por la tabla de fracción de ceros por quintil y
   los APE del probit, que dicen lo mismo y son directos.
4. Si se quiere mantener el argumento formal, enunciarlo con la identidad
   exacta: el coeficiente del censurado es, en un 86–100% según la variable, el
   efecto sobre la probabilidad de observar precio multiplicado por 13.98.

## Referencias

- Greene, W. (1981). "On the Asymptotic Bias of the Ordinary Least Squares
  Estimator of the Tobit Model". *Econometrica* 49(2): 505–513.
- McDonald, J. y Moffitt, R. (1980). "The Uses of Tobit Analysis".
  *Review of Economics and Statistics* 62(2): 318–321.
- Cragg, J. (1971). "Some Statistical Models for Limited Dependent Variables".
  *Econometrica* 39(5): 829–844.
- Carson, R. y Sun, Y. (2007). "The Tobit model with a non-zero threshold".
  *Econometrics Journal* 10(3): 488–502.
