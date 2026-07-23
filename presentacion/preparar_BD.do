// Actual


gen pj_ant = Internacional_pj_ant+ Local_pj_ant
gen goles_ant = Internacional_Goals_ant + Local_Goals_ant
gen pj_ant2 = Internacional_pj_ant2+ Local_pj_ant2
gen goles_ant2 = Internacional_Goals_ant2 + Local_Goals_ant2

replace Local_Assists_ant = 0 if posic==4 
replace Internacional_Assists_ant = 0 if posic==4

gen asis_ant = Internacional_Assists_ant+ Local_Assists_ant
gen ycard_ant = Local_YellowCards_ant + Internacional_YellowCards_ant
gen off_ant = Local_RedCards_ant + Internacional_RedCards_ant + Internacional_Doubleyellow_ant+ Local_Doubleyellow_ant

gen goles_conced_ant = Internacional_GoalsConceded_ant + Local_GoalsConceded_ant  
gen vallas_inv_ant = Local_CleanSheets_ant + Internacional_CleanSheets_ant

gen lprecio = log( transfer_fee)
gen logprecio = log( transfer_fee+1)
gen lvalor = log(market_value)
gen lvalor_plantel_to = log( valor_plantel_to)
gen lvalor_plantel_from = log( valor_plantel_from)


// drop cedido_last
*Revisar todo lo referente a last y current
// gen lvalor_plantel_last = log( valor_plantel_last)
// gen ratio_last_from =  valor_plantel_last / valor_plantel_from //mejor
// gen ratio_ln_last_from =  lvalor_plantel_last / lvalor_plantel_from

encode posicion , generate(posic)
encode club_from , generate(club_vendedor)
encode club_to , generate(club_comprador)
encode division_to , generate(div_to)
encode division_from , generate(div_from)
encode nationality_to , generate(nation_to)
encode nationality_from , generate(nation_from)
encode season_part, gen(verano)

gen pj2_ant = pj_ant^2
gen goles2_ant = goles_ant^2
gen len_plantel_from = round(valor_plantel_from/ mean_plantel_from, 0)
gen len_plantel_to = round(valor_plantel_to/ mean_plantel_to, 0)
gen tiene_precio = transfer_fee > 0

rename n_transfer_date n_transfer_window
label variable n_transfer_window "n_transfer_window"
rename rest_transfer_market rest_transfer_window
label variable rest_transfer_window "rest_transfer_window"


// Precios deflactados a precios de junio 2021





// gen lprecio = log( transfer_fee)
// gen logprecio = log( transfer_fee+1)
// gen lvalor = log(market_value)
// // gen logvalor = log(market_value+1)
// // gen pj_ant = internacional_pj_ant+ local_pj_ant
// // gen goles_ant = internacional_goals_ant + local_goals_ant
//
// gen pj_ant = Internacional_pj_ant+ Local_pj_ant
// gen goles_ant = Internacional_Goals_ant + Local_Goals_ant
//
// gen pj_ant2 = internacional_pj_ant2 + local_pj_ant2
// gen goles_ant2 = internacional_goals_ant2 + local_goals_ant2
//
// gen lvalor_plantel_to = log( valor_plantel_to)
// gen lvalor_plantel_from = log( valor_plantel_from)
// gen lvalor_plantel_last = log( valor_plantel_last)
//
// encode posicion , generate(posic)
// encode club_from , generate(club_vendedor)
// encode club_to , generate(club_comprador)
// encode division_last , generate(div_last)
// encode division_to , generate(div_to)
// encode division_from , generate(div_from)
//
// gen pj2_ant = pj_ant^2
// gen goles2_ant = goles_ant^2
// gen ycard_ant = local_yellowcards_ant + internacional_yellowcards_ant
// gen rcard_ant = local_redcards_ant + internacional_redcards_ant + internacional_doubleyellow_ant+ local_doubleyellow_ant
//
// gen len_plantel_from = round(valor_plantel_from/ mean_plantel_from, 0)
// gen len_plantel_to = round(valor_plantel_to/ mean_plantel_to, 0)
//
// gen tiene_precio = transfer_fee > 0

// destring age, replace
// gen age2 = age^2
// gen altura2 = altura^2
// summ altura
// gen altura_c = altura - `r(mean)'
//
// gen jugo_int_ant =  Internacional_pj_ant  > 3


// bysort posic : egen posic_mean = mean( altura )
// gen altura_posic = altura - posic_mean
