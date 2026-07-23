// Preparar paper

replace transfer_fee_d = . if transfer_fee==.
replace market_value_d = . if market_value==.
replace valor_plantel_to_d = . if valor_plantel_to==.
replace valor_plantel_from_d = . if valor_plantel_from==.

gen logprecio = log( transfer_fee+1)
gen lvalor = log(market_value)
gen lvalor_plantel_to = log( valor_plantel_to)
gen lvalor_plantel_from = log( valor_plantel_from)

gen logprecio_d = log( transfer_fee_d+1)
gen lvalor_plantel_to_d = log( valor_plantel_to_d)
gen lvalor_plantel_from_d = log( valor_plantel_from_d)
gen logvalor_d = log(market_value_d)

gen len_plantel_from = round(valor_plantel_from/ mean_plantel_from, .)
gen len_plantel_to = round(valor_plantel_to/ mean_plantel_to, .)

encode posicion , generate(posic)
encode club_from , generate(club_vendedor)
encode club_to , generate(club_comprador)
encode division_to , generate(div_to)
encode division_from , generate(div_from)
encode nationality_to , generate(nation_to)
encode nationality_from , generate(nation_from)
encode season_part, gen(verano)


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


//Si usamos las variables nuevas (wiki y seleccion nacional)

// Variables de suma y promedio (todas excepto peak)
foreach var of varlist wikiviews*_es {
    local base = subinstr("`var'", "_es", "", .)
    if !strpos("`var'", "peak") {
        gen `base' = `base'_es + `base'_en
    }
}

// Variables peak (máximo)
foreach var of varlist wikiviews*peak*_es {
    local base = subinstr("`var'", "_es", "", .)
    gen `base' = cond(`base'_es > `base'_en, `base'_es, `base'_en)
}


foreach var of varlist wikiviews* {
    replace `var' = 0 if missing(`var')
    gen log_`var' = .
    replace log_`var' = ln(`var'+1) if `var' >= 0
}

replace n_idiomas = 0 if n_idiomas==.
gen n_idiomas_dif1y = n_idiomas_att - n_idiomas_1ybt