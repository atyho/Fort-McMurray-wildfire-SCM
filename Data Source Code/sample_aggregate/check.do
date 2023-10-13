clear all
capture log close

log using data_check.log, replace

insheet using df_synth.csv, comma names case clear

gen date = mofd(date(Ref_date,"YMD"))
format date %tmCCYY-NN-DD

encode fsa, gen(fsa_factor)

sort treated FM_damage fsa_factor date 

*tsspell fsa_factor
*egen n_obs = max(_seq), by(fsa_factor)
*egen date_first = min(date), by(fsa_factor)
*format date_first %tmCCYY-NN-DD

gen ml_ins_rt = ml_bal_ins_tot/ml_bal_tot
gen ml_ins_arr_rt = ml_bal_arr_ins_tot/ml_bal_ins_tot
gen nearprime_rt = N_nearprime/N_ml_ins_holder
gen subprime_rt = N_subprime/N_ml_ins_holder
egen min_N_active = min(N_active), by(fsa_factor treated FM_damage)

tabstat N_active ml_bal_ins ml_ins_rt bc_use nearprime_rt subprime_rt N_ml_ins_holder min_N_active /*
*/ if treated == 1 & FM_damage == 1, s(mean sd)

tabstat N_active ml_bal_ins ml_ins_rt bc_use nearprime_rt subprime_rt N_ml_ins_holder min_N_active /*
*/ if treated == 1 & FM_damage == 0, s(mean sd)

tabstat N_active ml_bal_ins ml_ins_rt bc_use nearprime_rt subprime_rt N_ml_ins_holder min_N_active /*
*/ if treated == 0, s(mean sd)

line ml_ins_arr_rt date if treated == 1 & FM_damage == 1, saving(FM_severe)

line ml_ins_arr_rt date if treated == 1 & FM_damage == 0, saving(FM_other)

gr combine FM_severe.gph FM_other.gph, ycommon

log close