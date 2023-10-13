# Clean and organize the work environment
rm(list = ls())

library(Synth)
source('function_SCM-CS_v08.R')

library(doParallel)
library(dplyr)
library(Cairo)

library(ggplot2)
library(ggthemes)
options(scipen=999)  # turn-off scientific notation like 1e+48
theme_set(theme_bw()) 

load("FM_synth.RData")

####################################################
# Optional: Set up Cluster for parallel processing #
####################################################

# Create the cluster
cluster_type <- if(Sys.info()["sysname"] == "Windows"){"PSOCK"} else if(Sys.info()["sysname"] == "Linux"){"FORK"}
local_cl <- parallel::makeCluster(parallel::detectCores() - 1, type = cluster_type)

# Register cluster to be used by %dopar%
doParallel::registerDoParallel(cl = local_cl)

##############################################
# For severely damaged area in Fort McMurray #
##############################################

# Specify the data set used
fsa_data <- fsa_data_severe

# Specify the set of units
fsa_set <- fsa_id_tbl$fsa_id

###########################################
# Estimate the SC weights for each region #
###########################################

synth.results <- foreach(j = fsa_set, .combine = rbind, .packages = "Synth") %dopar% {
  
  # Prepare the data for synthetic control estimator
  synth.data <- model_spec(data_in = fsa_data, 
                           treated_id = j, unit_set = fsa_set,
                           time_range = event_window, pre_trt_range = pre_trt_dt)
  
  # Estimate the synthetic control method
  synth.out <- try(synth(data.prep.obj = synth.data,
                         optimxmethod = c("Nelder-Mead","BFGS"),
                         optimx(control = list(starttests = TRUE))
                         )
                   )

  # Try other methods if the default methods do not work
  if ( inherits(synth.out, "try-error") ) {
    synth.out <- try(synth(data.prep.obj = synth.data,
                           optimxmethod = c("All"),
                           optimx(control = list(starttests = TRUE))
                           )
                     )
    }
  
  if ( !inherits(synth.out, "try-error") ) {
    Y <- synth.data$Y1plot
    weights <- synth.out$solution.w 
    } 
  else {
    Y <- rep(NA, length.out = length(event_window))
    weights <- rep(NA, length.out = length(fsa_set)-1) 
    }
  
  return(c(Y, weights, j))
  
}

#####################################################
# Compute the confidence sets using function_SCM-CS #
#####################################################

synth.results <- synth.results[order(synth.results[,ncol(synth.results)]), ]

# Remove placebos that were unable to be estimated (debug for exceptional scenario)
error_set <- unname(synth.results[is.na(synth.results[,1]), ncol(synth.results)])
if(length(error_set) != 0){
  synth.results <- synth.results[-error_set,-(length(event_window)+error_set)]
  }

# Organize the placebo results
Ymat <- t(synth.results[, 1:length(event_window)])
weightsmat <- t(synth.results[, (length(event_window)+1):(ncol(synth.results)-1)])
n_unit <- length(fsa_set)
treated_id <- synth.results[synth.results[, ncol(synth.results)] == trt_id, ncol(synth.results)] # trt_id

if(length(error_set) != 0){
  weightsmat <- apply(weightsmat, 1, function(x) x/sum(x))
  n_unit <- length(setdiff(fsa_set, error_set))
  }

# Define the options of the function_SCM-CS.
T0 <- match(as.Date("2016-05-01", format="%Y-%m-%d"), event_window)
phi <- 0  # each region is equally likely to receive treatment
v <- matrix(0, 1, n_unit)
precision <- 30
type <- "uniform"
significance <- ceiling(n_unit*0.05)/n_unit

# Compute th confidence set
bounds <- SCM.CS(Ymat, weightsmat, treated = treated_id, T0, phi, v, precision, type, significance, plot = FALSE)

# Customized graph
FM_severe.gap <- bind_cols(date = FM_severe.data$tag$time.plot, 
                           gap = ( FM_severe.data$Y1plot - (FM_severe.data$Y0plot %*% FM_severe.out$solution.w) )[,1],
                           upper = bounds$u[,1], lower = bounds$l[,1])

plot_gap_severe <- ggplot(data = FM_severe.gap) + 
  geom_line(aes(x=date, y=gap), color="black", linetype="solid", linewidth=0.75) +
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

cairo_ps(filename = "figures/plot_synth_gap_severe.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
plot_gap_severe
dev.off()

##################
# Save workspace # 
##################

save.image(file = "FM_synth_CI_severe.RData")

####################
# Stop the cluster #
####################

parallel::stopCluster(cl = local_cl)
