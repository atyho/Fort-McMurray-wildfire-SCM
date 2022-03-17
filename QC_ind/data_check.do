clear all

disp("Stata program that check the extracted TU data")

insheet using tu_acct_sample.csv, comma names case clear

sort TU_Trade_ID Run_date LAST_UPDATED_DT

*keep if tu_trade_id==4887732 | tu_trade_id==202847407 | tu_trade_id==203848404

*keep if tu_trade_id==398850493 | tu_trade_id==102837613 | tu_trade_id==194825687 | tu_trade_id==7888730 | tu_trade_id==131828686 | tu_trade_id==148827653 | tu_trade_id==538880185

