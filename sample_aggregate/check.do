clear all
capture log close

log using data_check.log, replace

insheet using df_synth.csv, comma names case clear

gen date = mofd(date(Run_Date,"YMD"))
format date %tmCCYY-NN-DD

encode fsa_mod, gen(fsa_factor)

sort treated FM_damage fsa_factor date 

drop if FM_damage == 1

egen min_active_ind = min(N_active_ind), by(fsa_factor)
drop if min_active_ind <= 1000

xtset fsa_factor date

tsspell fsa_factor
egen n_obs = max(_seq), by(fsa_factor)

egen date_first = min(date), by(fsa_factor)
format date_first %tmCCYY-NN-DD

gen ML_insured_rt = ml_bal_ins_tot/ml_bal_tot

collapse (mean) N_res_holder N_active_ind h_own_rt ML_insured_rt, by(fsa_mod)

statsmat N_res_holder N_active_ind h_own_rt ML_insured_rt if N_active_ind >= 1000 , s(mean sd p25 p50 p75)

log close