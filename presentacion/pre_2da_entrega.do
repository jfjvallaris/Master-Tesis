save "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Py\df_full_deflact.dta"
reg lprecio c.age##c.age altura comunitario extranjer ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant    rest_transfer_window season i.verano, r
gen lvalor_plantel_to_defl = ln( valor_plantel_to_defl )
gen lvalor_plantel_from_defl = ln( valor_plantel_from_defl )
reg lprecio c.age##c.age altura comunitario extranjer ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant  rest_transfer_window season i.verano if transfer_fee > 0, r
reg logprecio_d c.age##c.age altura comunitario extranjer ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant  rest_transfer_window season i.verano if transfer_fee > 0, r
reg lprecio c.age##c.age comuni ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant  lvalor_plantel_to lvalor_plantel_from rest_transfer_window season i.verano, r
reg logprecio_d c.age##c.age comuni ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant  lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season i.verano if transfer_fee > 0, r
 reg lprecio age ib1.posic pj_ant goles_ant   lvalor_plantel_to lvalor_plantel_from rest_transfer_window season , r
 reg logprecio_d age ib1.posic pj_ant goles_ant   lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season if transfer_fee > 0 , r
tobit logprecio age ib1.posic pj_ant goles_ant   lvalor_plantel_to lvalor_plantel_from rest_transfer_window season if rest_transfer_window > 0, ll(ln(50000))
eststo Tobit_emg: margins, dydx(*) predict(ystar(ln(50000),.)) post
tobit logprecio_d age ib1.posic pj_ant goles_ant   lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season if rest_transfer_window > 0, ll(ln(50000))
eststo Tobit_emg: margins, dydx(*) predict(ystar(ln(50000),.)) post
tobit logprecio age ib1.posic pj_ant goles_ant   lvalor_plantel_to lvalor_plantel_from rest_transfer_window season if rest_transfer_window > 0, ll(ln(50000))
eststo Tobit_emg: margins, dydx(*) predict(ystar(ln(50000),.)) post
tobit logprecio_d age ib1.posic pj_ant goles_ant   lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season if rest_transfer_window > 0, ll(ln(50000))
eststo Tobit_emg_d: margins, dydx(*) predict(ystar(ln(50000),.)) post
est table  Tobit_emg Tobit_emg_d, star(0.1 0.05 0.01) b(%6,3f) stats(N) drop(_cons)
est table  Tobit_emg Tobit_emg_d, star(0.1 0.05 0.01) b(%6,3f) stats(N)
save "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Py\df_full_deflact.dta"
save "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Py\df_full_deflact.dta", replace
by carg varg, sort : summarize transfer_fee_d
tabstat  transfer_fee pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant if rest>0, by(tiene_precio) stat(mean sd min max)  format
tabstat  transfer_fee_d pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant if rest>0, by(tiene_precio) stat(mean sd min max)  format
tabstat  transfer_fee_d pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant if rest>0, by(tiene_precio) stat(mean sd min max)  format
format id %9.0f
format  %9.0f
format * %9.0f
format transfer_fee_defl valor_plantel_to_defl valor_plantel_from_defl %9.0f
tabstat  transfer_fee_d pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant if rest>0, by(tiene_precio) stat(mean sd min max)  format
tabstat  transfer_fee_d pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant if rest>0, by(tiene_precio) stat(mean p50 sd min max)  format
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
graph export "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Py\graficos\hist_transfer.eps", as(eps) name("Graph") preview(off) replace
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
graph export "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Py\graficos\hist_transfer.eps", as(eps) name("Graph") preview(off) replace
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
graph export "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Py\graficos\hist_transfer_con_precio.eps", as(eps) name("Graph") preview(off) replace
sktest lprecio
sktest logprecio_d if transfer_fee_d>0
sum logprecio_d if transfer_fee_d>0
sum transfer_fee_d if transfer_fee_d>0
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
use "D:\Maestria en Economia Aplicada\Material\_Mi tesis\Py\df_full_deflact.dta", clear
sort carg varg posic
by carg varg posic: summarize transfer_fee_defl
by carg varg posic: summarize transfer_fee_defl if transfer_fee > 0
by   posic: summarize transfer_fee_defl if transfer_fee > 0
sort  posic
by   posic: summarize transfer_fee_defl if transfer_fee > 0
sort carg varg
by carg varg : summarize transfer_fee_defl
by carg varg : summarize transfer_fee_defl if transfer_fee > 0
sort carg varg posic
by carg varg posic: summarize transfer_fee_defl if transfer_fee > 0
sort  posic
by   posic: summarize transfer_fee_defl if transfer_fee > 0
sort carg varg
by carg varg : summarize transfer_fee_defl if transfer_fee > 0
summarize transfer_fee_defl
summarize transfer_fee_defl if transfer_fee > 0
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
count if age > 37
reg logprecio_d age ib1.posic pj_ant goles_ant lvalor_plantel_to lvalor_plantel_from rest_transfer_window season  if transfer_fee_d > 0, r
ovtest
reg logprecio_d c.age##c.age altura comunitario extranjer ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant rest_transfer_window season i.verano if transfer_fee_d > 0, r
ovtest
reg logprecio_d age ib1.posic pj_ant goles_ant lvalor_plantel_to lvalor_plantel_from rest_transfer_window season  if transfer_fee_d > 0
hettest
reg logprecio_d c.age##c.age altura comunitario extranjer ib1.posic pj_ant goles_ant asis_ant ycard_ant off_ant goles_conced_ant vallas_inv_ant rest_transfer_window season i.verano if transfer_fee_d > 0
hettest
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
reg logprecio_d age ib1.posic pj_ant goles_ant   lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season if rest_transfer_window > 0
hettest
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
reg logprecio_d age ib1.posic pj_ant goles_ant lvalor_plantel_to_d lvalor_plantel_from_d rest_transfer_window season  if transfer_fee_d > 0, r
ovtest
reg rest_ lvalor_from_d
reg rest_ lvalor_plantel_from_d
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"
do "C:\Users\TERMIN~1\AppData\Local\Temp\STD6634_000000.tmp"