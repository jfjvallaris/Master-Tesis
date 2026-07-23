# Estado de la presentación (Presentacion.qmd)

Última actualización: 2026-07-23

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
| A1 | Metodología: estrategia de modelos anidados (1→4) | ⬜ Pendiente | Definir si va sola (auto-animate) o junto con A2/A3 en panel-tabset |
| A2 | Metodología: el problema de la censura (umbral L>0) | ⬜ Pendiente | — |
| A3 | Metodología: Tobit vs. Censurado vs. Truncado | ⬜ Pendiente | — |
| B1 | Tabla 9 completa (scrolleable) | ⬜ Pendiente | Decidido: `.scrollable: true`, sin dividir en tabs |
| B2 | Forest plot Modelo 3 | ⬜ Pendiente | Usa `df_mco_data` (ya cargado en chunk setup2) |
| B3 | Dumbbell Modelo 1 vs. Modelo 3 | ⬜ Pendiente | Prioridad media |
| C1 | Tabla 10/11 completas (scrolleable) | ⬜ Pendiente | Mismo criterio que B1 |
| C2 | Dumbbell MCO Truncado / Censurado / Tobit | ⬜ Pendiente | Usa `df_tobit` (ya cargado en chunk setup) |
| C3 | Tabla 12 + comparación Probit/Tobit | ⬜ Pendiente | — |
| D1 | Conclusiones: respuestas a preguntas de investigación | ⬜ Pendiente | Retomar diseño de slide "Problema de investigación" |
| D2 | Conclusiones: poder de negociación asimétrico | ⬜ Pendiente | — |
| D3 | Conclusiones: edad (lineal vs. cuadrática) | ⬜ Pendiente | — |
| D4 | Conclusiones: limitaciones | ⬜ Pendiente | — |
| D5 | Conclusiones: agenda futura | ⬜ Pendiente | — |
| D6 | Slide de cierre | ⬜ Pendiente | — |

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

## Pendiente de decidir
- Conteo de "slides lógicas" vs. "físicas" para el chequeo de tiempos de
  exposición (bloques `r-stack`/`auto-animate` cuentan como una sola
  unidad temática). Se resuelve con ensayo cronometrado real.
