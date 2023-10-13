############################################################
# Backdating robustness check for synthetic control method #
############################################################

# Clean and organize the work environment
rm(list = ls())

library(Synth)
library(dplyr)

load("../benchmark/FM_synth_qtr.RData")

#################################################################
# Create BACKDATED event window and define pre-treatment period #
#################################################################

# Pre-treatment window
pre_trt_dt <- event_window[event_window <= as.Date("2015-05-01", format="%Y-%m-%d")] %>% sort()

####################################
# Function for model specification #
####################################

model_spec <- function(data_in, treated_id, unit_set, time_range, pre_trt_range){
  dataprep.out <- dataprep(
    foo = data_in,
    predictors = c("ln_ml_bal_ins"),
    predictors.op = "mean", time.predictors.prior = pre_trt_dt,
    special.predictors = list(
      list("bc_use_60_80", c(yr14_q1), "mean"),
      list("bc_use_60_80", c(yr14_q2), "mean"),
      list("bc_use_60_80", c(yr14_q3), "mean"),
      list("bc_use_60_80", c(yr14_q4), "mean"),
      list("bc_use_60_80", c(yr15_q1), "mean"),
      list("bc_use_80_plus", c(yr14_q1), "mean"),
      list("bc_use_80_plus", c(yr14_q2), "mean"),
      list("bc_use_80_plus", c(yr14_q3), "mean"),
      list("bc_use_80_plus", c(yr14_q4), "mean"),
      list("bc_use_80_plus", c(yr15_q1), "mean"),
      list("subprime_rt", c(yr14_q1), "mean"),
      list("subprime_rt", c(yr14_q2), "mean"),
      list("subprime_rt", c(yr14_q3), "mean"),
      list("subprime_rt", c(yr14_q4), "mean"),
      list("subprime_rt", c(yr15_q1), "mean"),
      list("nearprime_rt", c(yr14_q1), "mean"),
      list("nearprime_rt", c(yr14_q2), "mean"),
      list("nearprime_rt", c(yr14_q3), "mean"),
      list("nearprime_rt", c(yr14_q4), "mean"),
      list("nearprime_rt", c(yr15_q1), "mean"),
      list("ml_ins_arr_rt", c(yr14_q1), "mean"),
      list("ml_ins_arr_rt", c(yr14_q2), "mean"),
      list("ml_ins_arr_rt", c(yr14_q3), "mean"),
      list("ml_ins_arr_rt", c(yr14_q4), "mean"),
      list("ml_ins_arr_rt", c(yr15_q1), "mean")
    ),
    dependent = "ml_ins_arr_rt",
    unit.variable = "fsa_id", unit.names.variable = "fsa_mod",
    time.variable = "date_num",
    treatment.identifier = treated_id,
    controls.identifier = setdiff(unit_set, treated_id),
    time.optimize.ssr = pre_trt_range,
    time.plot = time_range)
  
  return(dataprep.out)
}

##############################################
# For severely damaged area in Fort McMurray #
##############################################

# Prepare the data for BACKDATED synthetic control estimator
FM_severe.data <- model_spec(data_in = fsa_data_severe, 
                             treated_id = trt_id, unit_set = fsa_id_tbl$fsa_id,
                             time_range = event_window, pre_trt_range = pre_trt_dt)

# Estimate the synthetic control method
FM_severe.out <- synth(data.prep.obj = FM_severe.data,
                       optimxmethod = c("Nelder-Mead","BFGS"),
                       optimx(control = list(starttests = TRUE))
)

# Store results 
FM_severe.tables <- synth.tab(dataprep.res = FM_severe.data, synth.res = FM_severe.out)

print("Results for Fort McMurray significantly damaged areas:")
print(FM_severe.tables$tab.pred)
print(FM_severe.tables$tab.v)
print(FM_severe.tables$tab.w %>% arrange(desc(w.weights)) %>% filter(w.weights > 0) %>% head(5))

##################################################
# For non-severely damaged area in Fort McMurray #
##################################################

# Prepare the data for BACKDATED synthetic control estimator
FM_nonsevere.data <- model_spec(data_in = fsa_data_nonsevere, 
                                treated_id = trt_id, unit_set = fsa_id_tbl$fsa_id,
                                time_range = event_window, pre_trt_range = pre_trt_dt)
  
# Estimate the synthetic control method
FM_nonsevere.out <- synth(data.prep.obj = FM_nonsevere.data,
                          optimxmethod = c("Nelder-Mead","BFGS"),
                          optimx(control = list(starttests = TRUE))
)

# Store results   
FM_nonsevere.tables <- synth.tab(dataprep.res = FM_nonsevere.data, synth.res = FM_nonsevere.out)

print("Results for Fort McMurray other areas:")
print(FM_nonsevere.tables$tab.pred)
print(FM_nonsevere.tables$tab.v)
print(FM_nonsevere.tables$tab.w %>% arrange(desc(w.weights)) %>% filter(w.weights > 0) %>% head(5))
  
##################
# Save workspace # 
##################

save.image(file = "FM_synth_backdated.RData")
