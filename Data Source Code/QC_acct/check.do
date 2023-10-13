clear all
capture log close

log using data_check.log, replace

insheet using tu_acct_sample.csv, comma names case clear

gen date = mofd(date(Ref_date,"YMD"))
format date %tmCCYY-NN-DD

xtset TU_Trade_ID date 

keep if Primary_Indicator == 0

gen MOP_new = real(MOP_adj)

tab MOP_new

/* Check location change */

gen loc_chg = 0
replace loc_chg = 1 if fsa != fsa_raw

tab loc_chg

/* Time series spells */

keep if date >= mofd(date("2016-01-01","YMD")) & date <= mofd(date("2017-12-01","YMD"))

tsspell TU_Trade_ID

egen tag_tu_id = tag(TU_Trade_ID)

egen max_spell = max(_spell), by(TU_Trade_ID)

tab max_spell if tag_tu_id == 1

tab max_spell

/* Time series progression */

*drop if L3 == "BNS"
drop if L3 != "BNS"

gen arr = 0
*replace arr = 2 if MOP >= 2
*replace arr = 3 if MOP >= 3
replace arr = 4 if MOP >= 4
*replace arr = 5 if MOP >= 5
*replace arr = 7 if MOP >= 7

collapse (sum) CURRENT_BALANCE (count) N = TU_Trade_ID if terminal != "WO" , by(date arr)

reshape wide CURRENT_BALANCE N , i(date)  j(arr)

gen D90_rt = 100*CURRENT_BALANCE4/(CURRENT_BALANCE0+CURRENT_BALANCE4)

list 

/* Data check */

gen mop_new = 0
replace mop_new = 1 if MOP_new != MOP
egen check_mop = max(mop_new) , by(TU_Trade_ID)

gen arr_trt = 0
replace arr_trt = 1 if MOP_new >= 4 & date == mofd(date("2016-10-01","YMD"))
egen check_arr = max(arr_trt) , by(TU_Trade_ID)

gen arr_recover = 0
replace arr_recover = 1 if MOP_new < 4 & date == mofd(date("2016-11-01","YMD"))
egen check_recover = max(arr_recover) , by(TU_Trade_ID)

keep if check_arr == 1 
keep if check_recover == 1
keep if date >= mofd(date("2016-04-01","YMD")) & date <= mofd(date("2018-01-01","YMD"))

log close