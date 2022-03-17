# Clean and organize the work environment
rm(list = ls())

library(Synth)
load("synth_mod.RData")

library(doParallel)
library(data.table)
library(statar)
library(dplyr)
library(doBy)
library(Cairo)

source('function_SCM-CS_v08.R')

load("FM_synth_qtr.RData")

##############################################
# For severely damaged area in Fort McMurray #
##############################################

fsa_data <- fsa_data_severe

###########################################
# Estimate the SC weights for each region #
###########################################

fsa_set <- fsa_id_tbl$fsa_id 

synth.results <- foreach(j = fsa_set, .combine = rbind, .packages = "Synth") %do% {
  
  print(j)
  
  # Prepare the data for synthetic control estimator
  synth.data <- model_spec(data_in = fsa_data, treated_id = j, unit_set = fsa_set)
  
  # Estimating the synthetic control method
  synth.out <- try(synth_mod(data.prep.obj = synth.data,
                             optimxmethod = c("Nelder-Mead","BFGS"),
                             optimx(control = list(starttests = TRUE))
  ))
  
  # Try other methods if the default methods do not work
  if ( inherits(synth.out, "try-error") ) {
    synth.out <- try(synth_mod(data.prep.obj = synth.data,
                               optimxmethod = c("All"),
                               optimx(control = list(starttests = TRUE))
    )) }
  
  if ( !inherits(synth.out, "try-error") ) {
    Y <- synth.data$Y1plot
    weights <- synth.out$solution.w } 
  else {
    Y <- rep(NA, length.out = length(event_window))
    weights <- rep(NA, length.out = length(fsa_set)-1) }
  
  return(c(Y, weights, j))
  
}

#save.image(file = "FM_synth_qtr_CI_sd.RData")

#####################################################
# Compute the confidence sets using function_SCM-CS #
#####################################################

load("FM_synth_qtr_CI_sd.RData")

# Remove placebos that were unable to be estimate (debug)
error_set <- unname(synth.results[is.na(synth.results[,1]),ncol(synth.results)])
synth.results <- synth.results[-error_set,-(length(event_window)+error_set)]

# Organize our results
Ymat <- t(synth.results[, 1:length(event_window)])
weightsmat <- t(synth.results[, (length(event_window)+1):(ncol(synth.results)-1)])

# Define the options of the function_SCM-CS.
T0 <- match(as.Date("2016-05-01", format="%Y-%m-%d"), event_window)
phi <- 0 # each region is equally likely to receive treatment
v <- matrix(0, 1, length(setdiff(fsa_set,error_set)))
precision <- 30
type <- "uniform"
significance <- ceiling(length(fsa_set)*0.05)/length(fsa_set)

# Compute th confidence set
bounds <- SCM.CS(Ymat, weightsmat, treated = trt_id-length(error_set), T0, phi, v, precision, type, significance, plot = FALSE)

# Customized graph
FM_severe.gap <- bind_cols(FM_severe.data$tag$time.plot, 
                           FM_severe.data$Y1plot - (FM_severe.data$Y0plot %*% FM_severe.out$solution.w),
                           bounds)
colnames(FM_severe.gap) <- c("date","gap","upper","lower")
write.csv(FM_severe.gap,'FM_severe_gap.csv')

plot_gap_severe <- ggplot(data = FM_severe.gap) + 
  geom_line(aes(x=date, y=gap), color="black", linetype="solid", size=0.75) +
  geom_ribbon(aes(x=date, ymin=lower, ymax=upper), colour=NA, fill="grey25",alpha=0.15) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  geom_hline(yintercept=0, color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(-0.4,1.0)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(-0.4, 1.0, by = 0.2)) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.key = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line"))

plot_gap_severe

cairo_ps(filename = "plot_synth_gap_severe.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
plot_gap_severe
dev.off()
