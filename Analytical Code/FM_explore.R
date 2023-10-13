# Clean and organize the work environment
rm(list = ls())

library(dplyr)
library(doBy)
library(Cairo)

library(openxlsx)
library(ggplot2)
library(ggthemes)
options(scipen=999)  # turn-off scientific notation like 1e+48
theme_set(theme_bw()) 
library(ggpubr)

load("benchmark/FM_synth.RData")

###########################
# Plots of stylized facts #
###########################

BC <- data %>% filter(treated==1 
                      & date >= as.Date("2014-01-01", format="%Y-%m-%d")
                      & date <= as.Date("2018-01-01", format="%Y-%m-%d")) %>%
  mutate(belowprime_rt = (N_nearprime + N_subprime)/N_ml_ins,
         belowprime_rt_fsa = (N_nearprime_fsa + N_subprime_fsa)/N_active,
         illiquid_rt = N_bc_use_80_plus/N_ml_ins,
         illiquid_rt_fsa = N_bc_use_80_plus_fsa/N_active,
         bc_arr_rt = 100*bc_bal_arr_tot/bc_bal_tot)


###################################
# Plots for below-prime consumers #
###################################

plot_cr_score <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=cr_score, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=cr_score, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(720,780)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(720, 780, by = 10)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

plot_cr_score_fsa <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=cr_score_fsa, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=cr_score_fsa, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(720,780)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(720, 780, by = 10)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

ggarrange(plot_cr_score, plot_cr_score_fsa, ncol = 2)

###################################
# Plots for below-prime consumers #
###################################

plot_belowprime_rt <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=belowprime_rt, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=belowprime_rt, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.0, 0.5)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 0.5, by = 0.1)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

plot_belowprime_rt_fsa <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=belowprime_rt_fsa, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=belowprime_rt_fsa, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.0, 0.5)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 0.5, by = 0.1)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

ggarrange(plot_belowprime_rt, plot_belowprime_rt_fsa, ncol = 2)

###########################
# Plots for credit cards  #
###########################

# Credit card arrears rate for insured mortgage holders
plot_BC_arr_rt <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=bc_arr_rt, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=bc_arr_rt, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0,3.5)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 3.5, by = 0.5)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

plot_BC_arr_rt

# Plots for credit card use
plot_BC_bal <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=bc_bal, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=bc_bal, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(4000, 9000)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(4000, 9000, by = 1000)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

plot_BC_bal_fsa <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=bc_bal_fsa, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=bc_bal_fsa, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(4000, 9000)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(4000, 9000, by = 1000)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
       color = guide_legend(reverse=TRUE))

ggarrange(plot_BC_bal, plot_BC_bal_fsa, ncol = 2)

# Plots for credit card utilization
plot_bc_use <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=bc_use, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=bc_use, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.2, 0.5)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.2, 0.5, by = 0.05)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

plot_bc_use_fsa <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=bc_use_fsa, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=bc_use_fsa, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.2, 0.5)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.2, 0.5, by = 0.05)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

ggarrange(plot_bc_use, plot_bc_use_fsa, ncol = 2)

# Plots for illiquid_rt
plot_illiquid_rt <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=illiquid_rt, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=illiquid_rt, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.0, 0.3)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 0.3, by = 0.1)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

plot_illiquid_rt_fsa <- ggplot() + 
  geom_line(data=BC%>%filter(treated==1 & FM_damage==1), 
            aes(y=illiquid_rt_fsa, x=date, color="severe", linetype="severe"), linewidth=0.75) +
  geom_line(data=BC%>%filter(treated==1 & FM_damage==0), 
            aes(y=illiquid_rt_fsa, x=date, color="other", linetype="other"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.0, 0.3)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 0.3, by = 0.1)) +
  scale_colour_manual(name = "",
                      values = c("severe"="black","other"="black"),
                      labels = c("severe"="Severely damaged","other"="Other areas")) +
  scale_linetype_manual(name = "",
                        values = c("severe"="solid","other"="dashed"),
                        labels = c("severe"="Severely damaged","other"="Other areas")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line")) +
  guides(linetype = guide_legend(reverse=TRUE),
         color = guide_legend(reverse=TRUE))

ggarrange(plot_illiquid_rt, plot_illiquid_rt_fsa, ncol = 2)
