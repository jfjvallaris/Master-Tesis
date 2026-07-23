// GRAFICOS FINALES

**Logaritmo del precio de la transaccion como funcion del logaritmo del valor

set scheme white_tableau

*Histograma de transferencias con contratos
hist logprecio_d if rest > 0 , bin(18)  xtitle(ln(precio+1)) ytitle(Frecuencia) freq

hist logprecio_d if rest > 0 & transfer_fee > 0, freq bin(10) kdensity kdenopts(lc(red)) normal xtitle(ln(precio+1)) ytitle(Frecuencia) legend(on order(1 "" 2 "Kdensity" 3 "Distribución normal" )rows(1) pos(6)) 



*Efecto global
twoway (scatter lprecio lvalor) (lfit lprecio lvalor) , xtitle(logaritmo del valor) legend(order(1 "logaritmo del precio" 2 "ajuste lineal" ) rows(1) pos(6) span) 
//scheme(white_tableau)

*separando menores de 200k
// twoway (scatter lprecio lvalor if market_value < 200000, mcolor(gs12)) (scatter lprecio lvalor if market_value >= 200000) (lfit lprecio lvalor if market_value < 200000) (lfit lprecio lvalor if market_value >= 200000) ,  xtitle(logaritmo del valor) legend(order(1 "logaritmo del precio" 2 "logaritmo del precio" 3 "ajuste lineal" 4 "ajuste lineal" ) rows(2) pos(6) span)

*Me gusta pero no se para que
// predict xb_raw, xb
// scatter xb_raw logprecio   , yline(10.82) xline(10.82)

