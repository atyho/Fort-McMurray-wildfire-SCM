#####################################################
# Benchmark analysis using synthetic control method #
#####################################################

# Clean and organize the work environment
rm(list = ls())

library(Synth)
library(dplyr)

####################
# Data preparation #
####################

data <- read.csv(file = "../../../sample_aggregate/df_synth.csv", stringsAsFactors = FALSE) %>% 
  mutate(date = as.Date(Run_Date, format="%Y-%m-%d")) %>% 
  mutate(ln_ml_bal_ins = log(ml_bal_ins),
         bc_use_60_80 = 100*(N_bc_use_60_80_fsa)/N_active,
         bc_use_80_plus = 100*(N_bc_use_80_plus_fsa)/N_active,
         nearprime_rt = 100*(N_nearprime_fsa)/N_active,
         subprime_rt = 100*(N_subprime_fsa)/N_active) %>%
  arrange(treated, FM_damage, fsa, date)

####################################################
# Prepare data set for synthetic control algorithm #
####################################################

# Assign unique ID to each panel unit
fsa_id_tbl <- as.data.frame(unique(data$fsa) %>% sort()) %>% mutate(fsa_id = row_number())
colnames(fsa_id_tbl) <- c('name','fsa_id')

# Locate treatment unit ID
trt_id <- as.numeric(fsa_id_tbl[fsa_id_tbl$name == c("T9(H|J|K)"), 2])

# Join panel id to data frame
fsa_data <- data %>% left_join(fsa_id_tbl, by = c("fsa" = "name")) %>% 
  arrange(treated, fsa_id, date) %>% mutate(date_num = as.numeric(date))

#######################################################
# Create event window and define pre-treatment period #
#######################################################

# Create a list of event time periods in the data set
event_window <- unique(data$date) %>%  sort()
event_window <- event_window[event_window >= as.Date("2014-01-01", format="%Y-%m-%d")
                             & event_window <= as.Date("2018-01-01", format="%Y-%m-%d")] %>% sort()

# Pre-treatment window
pre_trt_dt <- event_window[event_window <= as.Date("2016-05-01", format="%Y-%m-%d")] %>% sort()

# Divide time periods into quarters for taking quarterly averages
yr14_q1 <- event_window[event_window > as.Date("2014-01-01", format="%Y-%m-%d") 
                        & event_window <= as.Date("2014-04-01", format="%Y-%m-%d")] %>% sort()
yr14_q2 <- event_window[event_window > as.Date("2014-04-01", format="%Y-%m-%d") 
                        & event_window <= as.Date("2014-07-01", format="%Y-%m-%d")] %>% sort()
yr14_q3 <- event_window[event_window > as.Date("2014-07-01", format="%Y-%m-%d") 
                        & event_window <= as.Date("2014-10-01", format="%Y-%m-%d")] %>% sort()
yr14_q4 <- event_window[event_window > as.Date("2014-10-01", format="%Y-%m-%d") 
                        & event_window <= as.Date("2015-01-01", format="%Y-%m-%d")] %>% sort()
yr15_q1 <- event_window[event_window > as.Date("2015-01-01", format="%Y-%m-%d") 
                        & event_window <= as.Date("2015-04-01", format="%Y-%m-%d")] %>% sort()
yr15_q2 <- event_window[event_window > as.Date("2015-04-01", format="%Y-%m-%d") 
                        & event_window <= as.Date("2015-07-01", format="%Y-%m-%d")] %>% sort()
yr15_q3 <- event_window[event_window > as.Date("2015-07-01", format="%Y-%m-%d") 
                        & event_window <= as.Date("2015-10-01", format="%Y-%m-%d")] %>% sort()
yr15_q4 <- event_window[event_window > as.Date("2015-10-01", format="%Y-%m-%d") 
                        & event_window <= as.Date("2016-01-01", format="%Y-%m-%d")] %>% sort()
yr16_q1 <- event_window[event_window > as.Date("2016-01-01", format="%Y-%m-%d") 
                        & event_window <= as.Date("2016-04-01", format="%Y-%m-%d")] %>% sort()

####################################
# Function for model specification #
####################################

model_spec <- function(data_in, treated_id, unit_set, time_range, pre_trt_range){
  dataprep.out <- dataprep(
    foo = data_in,
    predictors = c("ln_ml_bal_ins"),
    predictors.op = "mean", time.predictors.prior = pre_trt_range,
    special.predictors = list(
      list("bc_use_60_80", c(yr14_q1), "mean"),
      list("bc_use_60_80", c(yr14_q2), "mean"),
      list("bc_use_60_80", c(yr14_q3), "mean"),
      list("bc_use_60_80", c(yr14_q4), "mean"),
      list("bc_use_60_80", c(yr15_q1), "mean"),
      list("bc_use_60_80", c(yr15_q2), "mean"),
      list("bc_use_60_80", c(yr15_q3), "mean"),
      list("bc_use_60_80", c(yr15_q4), "mean"),
      list("bc_use_60_80", c(yr16_q1), "mean"),
      list("bc_use_80_plus", c(yr14_q1), "mean"),
      list("bc_use_80_plus", c(yr14_q2), "mean"),
      list("bc_use_80_plus", c(yr14_q3), "mean"),
      list("bc_use_80_plus", c(yr14_q4), "mean"),
      list("bc_use_80_plus", c(yr15_q1), "mean"),
      list("bc_use_80_plus", c(yr15_q2), "mean"),
      list("bc_use_80_plus", c(yr15_q3), "mean"),
      list("bc_use_80_plus", c(yr15_q4), "mean"),
      list("bc_use_80_plus", c(yr16_q1), "mean"),
      list("nearprime_rt", c(yr14_q1), "mean"),
      list("nearprime_rt", c(yr14_q2), "mean"),
      list("nearprime_rt", c(yr14_q3), "mean"),
      list("nearprime_rt", c(yr14_q4), "mean"),
      list("nearprime_rt", c(yr15_q1), "mean"),
      list("nearprime_rt", c(yr15_q2), "mean"),
      list("nearprime_rt", c(yr15_q3), "mean"),
      list("nearprime_rt", c(yr15_q4), "mean"),
      list("nearprime_rt", c(yr16_q1), "mean"),
      list("subprime_rt", c(yr14_q1), "mean"),
      list("subprime_rt", c(yr14_q2), "mean"),
      list("subprime_rt", c(yr14_q3), "mean"),
      list("subprime_rt", c(yr14_q4), "mean"),
      list("subprime_rt", c(yr15_q1), "mean"),
      list("subprime_rt", c(yr15_q2), "mean"),
      list("subprime_rt", c(yr15_q3), "mean"),
      list("subprime_rt", c(yr15_q4), "mean"),
      list("subprime_rt", c(yr16_q1), "mean"),
      list("cr_score", c(yr14_q1), "mean"),
      list("cr_score", c(yr14_q2), "mean"),
      list("cr_score", c(yr14_q3), "mean"),
      list("cr_score", c(yr14_q4), "mean"),
      list("cr_score", c(yr15_q1), "mean"),
      list("cr_score", c(yr15_q2), "mean"),
      list("cr_score", c(yr15_q3), "mean"),
      list("cr_score", c(yr15_q4), "mean"),
      list("cr_score", c(yr16_q1), "mean")
    ),
    dependent = "cr_score",
    unit.variable = "fsa_id", unit.names.variable = "fsa",
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

# Create the dataset for severely damaged area
fsa_data_severe <- fsa_data %>% filter(treated == 0 | FM_damage == 1) %>% arrange(treated, fsa_id, date)

# Prepare the data for synthetic control estimator
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

# Create the dataset for non-severely damaged area
fsa_data_nonsevere <- fsa_data %>% filter(treated == 0 | FM_damage == 0) %>% arrange(treated, fsa_id, date)

# Prepare the data for synthetic control estimator
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

save.image(file = "FM_synth.RData")
