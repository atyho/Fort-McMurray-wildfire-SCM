clear all

disp("Stata program that check the extracted TU data")

insheet using tu_acct_sample.csv, comma names case clear

gen date = mofd(date(Run_Date,"YMD"))
format date %tmCCYY-NN-DD

sort treated FM_damage tu_consumer_id date

