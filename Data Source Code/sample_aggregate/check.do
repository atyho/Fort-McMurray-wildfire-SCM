clear all
capture log close

log using data_check.log, replace

insheet using df_synth.csv, comma names case clear

gen date = mofd(date(Run_Date,"YMD"))
format date %tmCCYY-NN-DD

gen event_start = mofd(date("2014-01-01","YMD"))
format event_start %tmCCYY-NN-DD

gen event_end = mofd(date("2018-01-01","YMD"))
format event_end %tmCCYY-NN-DD

encode fsa, gen(fsa_factor)

sort treated FM_damage fsa_factor date 

*tsspell fsa_factor
*egen n_obs = max(_seq), by(fsa_factor)
*egen date_first = min(date), by(fsa_factor)
*format date_first %tmCCYY-NN-DD

gen ml_ins_rt = ml_bal_ins_tot/ml_bal_tot
gen ml_ins_arr_rt = ml_bal_arr_ins_tot/ml_bal_ins_tot
gen nearprime_rt = N_nearprime/N_ml_ins
gen subprime_rt = N_subprime/N_ml_ins
gen belowprime_rt = (N_nearprime + N_subprime)/N_ml_ins
gen illiquid_rt = N_bc_use_80_plus/N_ml_ins
egen min_N_active = min(N_active), by(fsa_factor treated FM_damage)

gen belowprime_fsa_rt = (N_nearprime_fsa + N_subprime_fsa)/N_active
gen bc_arr_rt = bc_bal_arr_tot/bc_bal_tot
gen illiquid_fsa_rt = N_bc_use_80_plus_fsa/N_active

tabstat N_active ml_bal_ins ml_ins_rt bc_use nearprime_rt subprime_rt N_ml_ins min_N_active /*
*/ if treated == 1 & FM_damage == 1 & date >= event_start & date <= event_end, s(mean sd)

tabstat N_active ml_bal_ins ml_ins_rt bc_use nearprime_rt subprime_rt N_ml_ins min_N_active /*
*/ if treated == 1 & FM_damage == 0  & date >= event_start & date <= event_end, s(mean sd)

tabstat N_active ml_bal_ins ml_ins_rt bc_use_avg nearprime_rt subprime_rt N_ml_ins min_N_active /*
*/ if treated == 0 & date >= event_start & date <= event_end, s(mean sd)

line ml_ins_arr_rt date if treated == 1 & FM_damage == 1, saving(FM_severe)
line ml_ins_arr_rt date if treated == 1 & FM_damage == 0, saving(FM_other)
gr combine FM_severe.gph FM_other.gph, ycommon

line cr_score date if treated == 1 & FM_damage == 1
line cr_score date if treated == 1 & FM_damage == 0

line belowprime_rt date if treated == 1 & FM_damage == 1
line belowprime_rt date if treated == 1 & FM_damage == 0

line bc_use date if treated == 1 & FM_damage == 1
line bc_use date if treated == 1 & FM_damage == 0

line illiquid_rt date if treated == 1 & FM_damage == 1
line illiquid_rt date if treated == 1 & FM_damage == 0

line bc_bal date if treated == 1 & FM_damage == 1
line bc_bal date if treated == 1 & FM_damage == 0

/* Analysis for the whole region unconditional on insured mortgages */

line bc_arr_rt date if treated == 1 & FM_damage == 1
line bc_arr_rt date if treated == 1 & FM_damage == 0

line cr_score_fsa date if treated == 1 & FM_damage == 1
line cr_score_fsa date if treated == 1 & FM_damage == 0

line belowprime_fsa_rt date if treated == 1 & FM_damage == 1
line belowprime_fsa_rt date if treated == 1 & FM_damage == 0

line bc_use_fsa date if treated == 1 & FM_damage == 1
line bc_use_fsa date if treated == 1 & FM_damage == 0

line illiquid_fsa_rt date if treated == 1 & FM_damage == 1
line illiquid_fsa_rt date if treated == 1 & FM_damage == 0

line bc_bal_fsa date if treated == 1 & FM_damage == 1
line bc_bal_fsa date if treated == 1 & FM_damage == 0

log close