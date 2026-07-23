// Datos y Variables

//Tabla 2.3
sort carg varg posic
by carg varg posic: summarize transfer_fee_defl if transfer_fee > 0
sort  posic
by   posic: summarize transfer_fee_defl if transfer_fee > 0
sort carg varg
by carg varg : summarize transfer_fee_defl if transfer_fee > 0
summarize transfer_fee_defl if transfer_fee > 0


// Tabla 2.5
tabstat  transfer_fee_d pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant if rest>0, by(tiene_precio) stat(mean p50 sd min max)  format


// Clubes
use "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Presentacion\df_clubes.dta"

// gen lvalor_plantel = log(valor_plantel)
// gen len_plantel = round(valor_plantel / mean_plantel, 1.0)
// encode division, gen(div)
// encode nationality , gen(nac)
// gen no_arg = nac!=4

eststo C1: reg lvalor_plantel len_plantel  league_position puntos partidos_jugados goles_favor goles_contra i.season i.div no_arg ,r 

eststo C2: reg lvalor_plantel len_plantel  league_position puntos partidos_jugados goles_favor goles_contra i.season i.div ib4.nac 


esttab C1 C2 , star(* 0.1 ** 0.05 *** 0.01) b(%6,3f) drop(_cons)  mtitle("lvalor_plantel" "lvalor_plantel" )  r2  scalars(df_m) se tex // wide 


// Alternativa con panel - No lo uso porque no quiero explicar panel
// xtset  club_code season
//
// xtreg lvalor_plantel len_plantel  league_position puntos partidos_jugados goles_favor goles_contra i.div
// 
// xtreg lvalor_plantel len_plantel  league_position puntos partidos_jugados goles_favor goles_contra i.div i.nac  
  
 // Puede ser ,be ya que nos interesa la variabilidad entre individuos
 
 

// Regresiones

// use "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Py\df_full.dta"
use "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Presentacion\df_full_deflact.dta"

// Regresiones deflactadas a precios de junio 2021
//Primer tabla - Analisis de MCO
*La variable ln_ratio_last_from es interesante pero analizar la construccion de last primero

eststo MCO1: reg logprecio_d c.age##c.age altura comunitario extranjer ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant rest_transfer_window season i.verano if transfer_fee_d > 0, r

eststo MCO2: reg logprecio_d c.age##c.age comuni ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant  lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season i.verano if transfer_fee_d > 0, r

eststo MCO3: reg logprecio_d age ib1.posic pj_ant goles_ant lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season  if transfer_fee_d > 0, r

eststo MCO4: reg logprecio_d age ib1.posic pj_ant goles_ant rest_transfer_window season i.club_vendedor i.club_comprador if transfer_fee_d > 0 , r

esttab MCO1 MCO2 MCO3 MCO4 , star(* 0.1 ** 0.05 *** 0.01) b(%6,3f) drop(_cons)  mtitle("Modelo 1" "Modelo 2" "Modelo 3" "Modelo 4")  r2  scalars(df_m) se wide 
// tex


eststo Censura: reg logprecio_d age ib1.posic pj_ant goles_ant   lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season if rest_transfer_window > 0 

eststo Tobit: tobit logprecio_d age ib1.posic pj_ant goles_ant   lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season if rest_transfer_window > 0, ll(ln(45000))
eststo Tobit_emg: margins, dydx(*) predict(ystar(ln(45000),.)) post

esttab MCO3 Censura Tobit_emg  , star(* 0.1 ** 0.05 *** 0.01) b(%6,3f) drop(_cons)  mtitle("MCO Truncado" "MCO Censurado" "Tobit" )  r2  scalars(df_m) se wide 
// tex


est table MCO3  Censura Tobit_emg, star(0.1 0.05 0.01) b(%6,3f) stats(N) drop(_cons) 




eststo Probit: probit tiene_precio age ib1.posic pj_ant goles_ant lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season if rest_transfer_window > 0

est table Probit, star(0.1 0.05 0.01) b(%6,3f) stats(N) drop(_cons) 


**Precios corrientes
//Primer tabla - Analisis de MCO
*La variable ln_ratio_last_from es interesante pero analizar la construccion de last primero

eststo MCO1: reg lprecio c.age##c.age altura comunitario extranjer ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant    rest_transfer_window season i.verano, r

eststo MCO2: reg lprecio c.age##c.age comuni ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant  lvalor_plantel_to lvalor_plantel_from rest_transfer_window season i.verano, r

eststo MCO3: reg lprecio age ib1.posic pj_ant goles_ant lvalor_plantel_to lvalor_plantel_from rest_transfer_window season , r

eststo MCO4: reg lprecio age ib1.posic pj_ant goles_ant rest_transfer_window season i.club_vendedor i.club_comprador , r


esttab MCO1 MCO2 MCO3 MCO4 , star(* 0.1 ** 0.05 *** 0.01) b(%6,3f) drop(_cons)  mtitle("Modelo 1" "Modelo 2" "Modelo 3" "Modelo 4")  r2  scalars(df_m) se wide 
// tex





// eststo MCO3: reg lprecio age  pj_ant goles_ant lvalor_plantel_to  lvalor_plantel_from  rest_transfer_market season i.verano , r

// Comparando alternativas de modelización
// eststo MCO: reg lprecio age  pj_ant goles_ant lvalor_plantel_to  lvalor_plantel_from  rest_transfer_market n_transfer_date , r

eststo Censura: reg logprecio age ib1.posic pj_ant goles_ant   lvalor_plantel_to lvalor_plantel_from rest_transfer_window season if rest_transfer_window > 0 

eststo Tobit: tobit logprecio age ib1.posic pj_ant goles_ant   lvalor_plantel_to lvalor_plantel_from rest_transfer_window season if rest_transfer_window > 0, ll(ln(50000))
eststo Tobit_emg: margins, dydx(*) predict(ystar(ln(50000),.)) post

esttab MCO3 Censura Tobit_emg  , star(* 0.1 ** 0.05 *** 0.01) b(%6,3f) drop(_cons)  mtitle("MCO Truncado" "MCO Censurado" "Tobit" )  r2  scalars(df_m) se wide  tex


est table MCO3 Tobit_emg Censura, star(0.1 0.05 0.01) b(%6,3f) stats(N) drop(_cons) 


eststo Probit: probit tiene_precio age ib1.posic pj_ant goles_ant   lvalor_plantel_to lvalor_plantel_from rest_transfer_window season if rest_transfer_window > 0

est table Probit, star(0.1 0.05 0.01) b(%6,3f) stats(N) drop(_cons) 


** Interesante para analizar
// ssc install reghdfe
// ssc install ftools
// reghdfe lprecio age  pj_ant goles_ant lvalor_plantel_to lvalor_plantel_from rest_transfer_window season, absorb(nation_to nation_from) vce(cluster club_comprador club_vendedor)


* Interesante analizar el modelo completo
// tobit logprecio c.age altura comunitario extranjer ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant  lvalor_plantel_to lvalor_plantel_from rest_transfer_window season i.verano, ll(ln(50000))
// margins, dydx(*) predict(ystar(ln(50000),.)) post

*Modelo con limite variable
// keep if rest_transfer_window > 0
// save temp_transfer_data, replace
// use temp_transfer_data, clear
// egen L_i_real = min(transfer_fee) if transfer_fee > 0, by(club_vendedor)
// replace L_i_real = 0 if L_i_real == .  
// gen L_i = log(L_i_real + 1)
// gen y_lower = L_i
// gen y_upper = logprecio
// replace y_upper = y_lower if transfer_fee == 0
// list club_vendedor transfer_fee y_lower y_upper if y_lower > y_upper
// intreg y_lower y_upper age ib1.posic pj_ant goles_ant cedido_last  lvalor_plantel_to lvalor_plantel_from season i.club_vendedor i.club_comprador, robust
// intreg y_lower y_upper age ib1.posic pj_ant goles_ant cedido_last  lvalor_plantel_to lvalor_plantel_from season, robust
// regress y_lower lvalor_plantel_from
// regress y_upper lvalor_plantel_from

// eststo VarInst: ivregress 2sls lprecio age  pj_ant goles_ant lvalor_plantel_to  rest_transfer_window n_transfer_window  (lvalor = age pj_ant goles_ant lvalor_plantel_from rest_transfer_window n_transfer_window), first

// eststo VarInst2: ivregress 2sls lprecio age  pj_ant goles_ant lvalor_plantel_to       goles_contra_to  rest_transfer_market n_transfer_date  (lvalor = age pj_ant goles_ant lvalor_plantel_from rest_transfer_market n_transfer_date) if market_value > 200000 , first


//Test endogeneidad reg3 

eststo VarInstFrom: ivregress 2sls lprecio age ib1.posic pj_ant goles_ant  cedido_last lvalor_plantel_to  rest_transfer_window season  (lvalor_plantel_from =   league_position_from puntos_from partidos_jugados_from goles_favor_from goles_contra_from i.div_from ib4.nation_from season), first
estat endog

eststo VarInstTo: ivregress 2sls lprecio age ib1.posic pj_ant goles_ant  cedido_last  lvalor_plantel_from rest_transfer_window season  (lvalor_plantel_to = len_plantel_to  league_position_to puntos_to partidos_jugados_to goles_favor_to goles_contra_to i.div_to ib4.nation_to season), first
estat endog
