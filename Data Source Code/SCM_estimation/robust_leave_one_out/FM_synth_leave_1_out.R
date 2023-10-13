###############################################################
# Leave-one-out robustness check for synthetic control method #
###############################################################

# Clean and organize the work environment
rm(list = ls())

library(Synth)
library(dplyr)
library(Cairo)

library(ggplot2)
library(ggthemes)
options(scipen=999)  # turn-off scientific notation like 1e+48
theme_set(theme_bw()) 

load("../benchmark/FM_synth.RData")

##############################################################
# Function for estimating the leave-one-out robustness check #
##############################################################

SCM_leave_out <- function(data, SCM.tables){
  
  # Obtain the list of donors with positive weight (sorted in descending weight)
  donor_ls <- SCM.tables$tab.w %>% filter(w.weights > 0) %>% arrange(desc(w.weights))

  # Remove one nonzero-weight donor at a time and re-estimate the synthetic path
  n_donor = nrow(donor_ls)
  SCM_paths <- matrix(ncol = n_donor, nrow = length(event_window))
  SCM_gaps <- matrix(ncol = n_donor, nrow = length(event_window))
  SCM_weights <- matrix(ncol = n_donor, nrow = length(fsa_id_tbl$fsa_id))
  for (i in 1:n_donor){

    writeLines(paste("Removing donor ID",donor_ls$unit.numbers[i],donor_ls$unit.names[i],"(#",i,"out of",n_donor,")"))

    # Set the filtered pool of donors
    fsa_set <- setdiff(fsa_id_tbl$fsa_id, donor_ls$unit.numbers[i])
    
    # Reset the data set for synthetic control method
    SCM_filtered.data <- model_spec(data_in = data, 
                                    treated_id = trt_id, unit_set = fsa_set,
                                    time_range = event_window, pre_trt_range = pre_trt_dt)

    # Estimate the synthetic control method
    SCM_filtered.out <- try(synth(data.prep.obj = SCM_filtered.data,
                                  optimxmethod = c("Nelder-Mead","BFGS"),
                                  optimx(control = list(starttests = TRUE))
                                  )
                            )

    # Try other methods if the default methods do not work
    if ( inherits(SCM_filtered.out, "try-error") ) {
      SCM_filtered.out <- try(synth(data.prep.obj = SCM_filtered.data,
                                    optimxmethod = c("All"),
                                    optimx(control = list(starttests = TRUE))
                                    )
                              )
      }
    
    if ( !inherits(SCM_filtered.out, "try-error") ) {
      SCM_paths[,i] <- SCM_filtered.data$Y0plot %*% SCM_filtered.out$solution.w
      SCM_gaps[,i] = SCM_filtered.data$Y1plot - SCM_paths[,i]
      SCM_weights[-c(trt_id,donor_ls$unit.numbers[i]),i] <- SCM_filtered.out$solution.w
      } 

  }
  
  SCM_leave_out.out <- list("paths" = SCM_paths, "gaps" = SCM_gaps, "weights" = SCM_weights)
  
  return(SCM_leave_out.out)
  
}

##############################################
# For severely damaged area in Fort McMurray #
##############################################

FM_severe.leave_out <- SCM_leave_out(fsa_data_severe, FM_severe.tables)

# Customized graph
FM_severe.path <- bind_cols(date = FM_severe.data$tag$time.plot,
                            Y1 = FM_severe.data$Y1plot[,1],
                            Y0 = (FM_severe.data$Y0plot %*% FM_severe.out$solution.w)[,1],
                            as.data.frame(FM_severe.leave_out$paths))

# Settings for robustness paths
robust_ggpaths <- as.data.frame(FM_severe.leave_out$paths)
robust_ggalpha <- rep(0.8, dim(robust_ggpaths)[2])
#robust_ggalpha <- seq(0.8, 0.2, by=(0.2-0.8)/(dim(robust_ggpaths)[2]-1))

plot_severe.leave_out <- ggplot() + 
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid")
for (i in 1:dim(robust_ggpaths)[2]){
  temp <- bind_cols(date = FM_severe.path[,1], value = robust_ggpaths[,i])
  plot_severe.leave_out <- plot_severe.leave_out + 
    geom_line(data=temp, aes(x=date, y=value, color="Leave-one-out", linetype="Leave-one-out"), 
              alpha=robust_ggalpha[i], linewidth=0.6)
}  

plot_severe.leave_out <- plot_severe.leave_out +
  geom_line(data=FM_severe.path, aes(x=date, y=Y1, color="Treated", linetype="Treated"), linewidth=0.75) +
  geom_line(data=FM_severe.path, aes(x=date, y=Y0, color="Synthetic", linetype="Synthetic"), linewidth=0.75) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  coord_cartesian(ylim = c(0, 1.2)) + scale_y_continuous(breaks = seq(0.0, 1.2, by = 0.2)) + 
  scale_colour_manual(name = "",
                      values = c("Treated"="black","Synthetic"="black","Leave-one-out"="grey60"),
                      labels = c("Treated"="Treated","Synthetic"="Synthetic","Leave-one-out"="Leave-one-out")) +
  scale_linetype_manual(name = "",
                        values = c("Treated"="solid","Synthetic"="dashed","Leave-one-out"="solid"),
                        labels = c("Treated"="Treated","Synthetic"="Synthetic","Leave-one-out"="Leave-one-out")) +  
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        axis.title.x = element_blank(), axis.title.y = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.box="vertical", legend.margin=margin(),
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line"))

plot_severe.leave_out

cairo_ps(filename = "figures/plot_leave_1_out_severe.eps", width=4, height=4, pointsize = 12, fallback_resolution = 300)
plot_severe.leave_out
dev.off()

##################################################
# For non-severely damaged area in Fort McMurray #
##################################################

FM_nonsevere.leave_out <- SCM_leave_out(fsa_data_nonsevere, FM_nonsevere.tables)

# Customized graph
FM_nonsevere.path <- bind_cols(date = FM_nonsevere.data$tag$time.plot,
                               Y1 = FM_nonsevere.data$Y1plot[,1],
                               Y0 = (FM_nonsevere.data$Y0plot %*% FM_nonsevere.out$solution.w)[,1],
                               as.data.frame(FM_nonsevere.leave_out$paths))

# Settings for robustness paths
robust_ggpaths <- as.data.frame(FM_nonsevere.leave_out$paths)
robust_ggalpha <- rep(0.8, dim(robust_ggpaths)[2])
#robust_ggalpha <- seq(0.8, 0.2, by=(0.2-0.8)/(dim(robust_ggpaths)[2]-1))

plot_nonsevere.leave_out <- ggplot() + 
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid")
for (i in 1:dim(robust_ggpaths)[2]){
  temp <- bind_cols(date = FM_nonsevere.path[,1], value = robust_ggpaths[,i])
  plot_nonsevere.leave_out <- plot_nonsevere.leave_out + 
    geom_line(data=temp, aes(x=date, y=value, color="Leave-one-out", linetype="Leave-one-out"), 
              alpha=robust_ggalpha[i], linewidth=0.6)
  }  

plot_nonsevere.leave_out <- plot_nonsevere.leave_out + 
  geom_line(data=FM_nonsevere.path, aes(x=date, y=Y1, color="Treated", linetype="Treated"), linewidth=0.75) +
  geom_line(data=FM_nonsevere.path, aes(x=date, y=Y0, color="Synthetic", linetype="Synthetic"), linewidth=0.75) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  coord_cartesian(ylim = c(0, 1.2)) + scale_y_continuous(breaks = seq(0.0, 1.2, by = 0.2)) + 
  scale_colour_manual(name = "",
                      values = c("Treated"="black","Synthetic"="black","Leave-one-out"="grey60"),
                      labels = c("Treated"="Treated","Synthetic"="Synthetic","Leave-one-out"="Leave-one-out")) +
  scale_linetype_manual(name = "",
                        values = c("Treated"="solid","Synthetic"="dashed","Leave-one-out"="solid"),
                        labels = c("Treated"="Treated","Synthetic"="Synthetic","Leave-one-out"="Leave-one-out")) +  
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        axis.title.x = element_blank(), axis.title.y = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line"))

plot_nonsevere.leave_out

cairo_ps(filename = "figures/plot_leave_1_out_nonsevere.eps", width=4, height=4, pointsize = 12, fallback_resolution = 300)
plot_nonsevere.leave_out
dev.off()

##################
# Save workspace # 
##################

save.image(file = "FM_synth_leave_1_out.RData")
