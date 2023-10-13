# Clean and organize the work environment
rm(list = ls())

library(Synth)
library(doParallel)
library(data.table)
library(statar)
library(dplyr)
library(doBy)
library(Cairo)

library(openxlsx)
library(lubridate)
library(ggplot2)
library(ggthemes)
options(scipen=999)  # turn-off scientific notation like 1e+48
theme_set(theme_bw()) 

#source('function_SCM-CS_v08.R')

load("FM_synth_qtr.RData")

wb <- createWorkbook()

##############################################
# For severely damaged area in Fort McMurray #
##############################################

addWorksheet(wb=wb, sheetName = "Severe-Predictors")
writeData(wb, sheet = "Severe-Predictors", rowNames = TRUE, FM_severe.tables$tab.pred)

addWorksheet(wb=wb, sheetName = "Severe-V values")
writeData(wb, sheet = "Severe-V values", rowNames = TRUE, FM_severe.tables$tab.v)

addWorksheet(wb=wb, sheetName = "Severe-W values")
writeData(wb, sheet = "Severe-W values", rowNames = TRUE,
          FM_severe.tables$tab.w %>% filter(w.weights > 0) %>% arrange(desc(w.weights)) %>% head(5))

print(FM_severe.tables$tab.pred)
print(FM_severe.tables$tab.v)
print(FM_severe.tables$tab.w %>% arrange(desc(w.weights)) %>% filter(w.weights > 0) %>% head(5))

# Customized graph
FM_severe.path <- bind_cols(FM_severe.data$tag$time.plot,
                            FM_severe.data$Y1plot,
                            FM_severe.data$Y0plot %*% FM_severe.out$solution.w)
colnames(FM_severe.path) <- c("date","Y1","Y0")

plot_path_severe <- ggplot(data = FM_severe.path) + 
  geom_line(aes(x=date, y=Y1, color="Treated", linetype="Treated"), size=0.75) +
  geom_line(aes(x=date, y=Y0, color="Synthetic", linetype="Synthetic"), size=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.0,1.2)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 1.2, by = 0.2)) +
  scale_colour_manual(name = "",
                      values = c("Treated"="black","Synthetic"="black"),
                      labels = c("Treated"="Treated","Synthetic"="Synthetic")) +
  scale_linetype_manual(name = "",
                        values = c("Treated"="solid","Synthetic"="dashed"),
                        labels = c("Treated"="Treated","Synthetic"="Synthetic")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line"))

plot_path_severe

cairo_ps(filename = "plot_synth_path_severe.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
plot_path_severe
dev.off()

#path.plot(synth.res = FM_severe.out, dataprep.res = FM_severe.data, 
#          Ylab = "", Xlab = "Year", Ylim = c(0,1.2), Legend.position = "topleft",
#          abline(v = as.Date("2016-05-01", format="%Y-%m-%d"), col="black"))

##################################################
# For non-severely damaged area in Fort McMurray #
##################################################

addWorksheet(wb=wb, sheetName = "Nonsevere-Predictors")
writeData(wb, sheet = "Nonsevere-Predictors", rowNames =TRUE, FM_nonsevere.tables$tab.pred)

addWorksheet(wb=wb, sheetName = "Nonsevere-V values")
writeData(wb, sheet = "Nonsevere-V values", rowNames = TRUE, FM_nonsevere.tables$tab.v)

addWorksheet(wb=wb, sheetName = "Nonsevere-W values")
writeData(wb, sheet = "Nonsevere-W values", rowNames = TRUE, 
          FM_nonsevere.tables$tab.w %>% filter(w.weights > 0) %>% arrange(desc(w.weights)) %>% head(5))

print(FM_nonsevere.tables$tab.pred)
print(FM_nonsevere.tables$tab.v)
print(FM_nonsevere.tables$tab.w %>% arrange(desc(w.weights)) %>% filter(w.weights > 0) %>% head(5))

# Customized graph
FM_nonsevere.path <- bind_cols(FM_nonsevere.data$tag$time.plot,
                               FM_nonsevere.data$Y1plot,
                               FM_nonsevere.data$Y0plot %*% FM_nonsevere.out$solution.w)
colnames(FM_nonsevere.path) <- c("date","Y1","Y0")

plot_path_nonsevere <- ggplot(data = FM_nonsevere.path) + 
  geom_line(aes(x=date, y=Y1, color="Treated", linetype="Treated"), size=0.75) +
  geom_line(aes(x=date, y=Y0, color="Synthetic", linetype="Synthetic"), size=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.0,1.2)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 1.2, by = 0.2)) +
  scale_colour_manual(name = "",
                      values = c("Treated"="black","Synthetic"="black"),
                      labels = c("Treated"="Treated","Synthetic"="Synthetic")) +
  scale_linetype_manual(name = "",
                        values = c("Treated"="solid","Synthetic"="dashed"),
                        labels = c("Treated"="Treated","MSynthetic"="Synthetic")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line"))

plot_path_nonsevere
  
cairo_ps(filename = "plot_synth_path_nonsevere.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
plot_path_nonsevere
dev.off()

#path.plot(synth.res = FM_nonsevere.out, dataprep.res = FM_nonsevere.data, 
#          Ylab = "", Xlab = "Year", Ylim = c(0,1.2), Legend.position = "topleft",
#          abline(v = as.Date("2016-05-01", format="%Y-%m-%d"), col="black"))

saveWorkbook(wb, "FM_synth_results.xlsx", overwrite = TRUE)

###########################
# Plots of stylized facts #
###########################

# Aggregate mortgage data
ML <- data %>% filter(treated==1 
                      & date >= as.Date("2014-01-01", format="%Y-%m-%d")
                      & date <= as.Date("2018-01-01", format="%Y-%m-%d")) %>%
  summaryBy(formula = 
              ml_bal_ins_tot + ml_bal_arr_ins_tot + ml_chargoff_new_ins_tot +
              ml_bal_tot + ml_bal_arr_tot + ml_chargoff_new_tot ~
              treated + FM_damage + date, FUN = c(sum), keep.names = TRUE) %>%
  mutate(ml_ins_arr_rt = 100*(ml_bal_arr_ins_tot + ml_chargoff_new_ins_tot)/(ml_bal_ins_tot),
         ml_arr_rt = 100*(ml_bal_arr_tot + ml_chargoff_new_tot) / (ml_bal_tot) )

# Plots for mortgage arrears rate
plot_ML_severe <- ggplot() + 
  geom_line(data=ML%>%filter(treated==1 & FM_damage==1), 
            aes(y=ml_ins_arr_rt, x=date, color="ML_ins", linetype="ML_ins"), size=0.75) +
  geom_line(data=ML%>%filter(treated==1 & FM_damage==1), 
            aes(y=ml_arr_rt, x=date, color="ML_all", linetype="ML_all"), size=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0,1.25)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 1.2, by = 0.2)) +
  scale_colour_manual(name = "",
                      values = c("ML_ins"="black","ML_all"="black"),
                      labels = c("ML_ins"="Insured ML","ML_all"="All ML")) +
  scale_linetype_manual(name = "",
                        values = c("ML_ins"="solid","ML_all"="dashed"),
                        labels = c("ML_ins"="Insured ML","ML_all"="All ML")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line"))

plot_ML_severe

cairo_ps(filename = "plot_path_severe.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
print(plot_ML_severe)
dev.off()

plot_ML_nonsevere <- ggplot() + 
  geom_line(data=ML%>%filter(treated==1 & FM_damage==0), 
            aes(y=ml_ins_arr_rt, x=date, color="ML_ins", linetype="ML_ins"), size=0.75) +
  geom_line(data=ML%>%filter(treated==1 & FM_damage==0), 
            aes(y=ml_arr_rt, x=date, color="ML_all", linetype="ML_all"), size=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0,1.25)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 1.2, by = 0.2)) +
  scale_colour_manual(name = "",
                      values = c("ML_ins"="black","ML_all"="black"),
                      labels = c("ML_ins"="Insured ML","ML_all"="All ML")) +
  scale_linetype_manual(name = "",
                        values = c("ML_ins"="solid","ML_all"="dashed"),
                        labels = c("ML_ins"="Insured ML","ML_all"="All ML")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line"))

plot_ML_nonsevere

cairo_ps(filename = "plot_path_nonsevere.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
print(plot_ML_nonsevere)
dev.off()

# Plots for mortgage arrears and outstanding balance normalized to May 2016
ML_severe_norm <- ML%>% filter(FM_damage==1) %>% 
  mutate(ml_bal_ins_norm = ml_bal_ins_tot/ml_bal_ins_tot[date == as.Date("2016-06-01", format="%Y-%m-%d")],
         ml_bal_arr_ins_norm = ml_bal_arr_ins_tot/ml_bal_arr_ins_tot[date == as.Date("2016-06-01", format="%Y-%m-%d")] )

ML_nonsevere_norm <- ML%>% filter(FM_damage==0) %>% 
  mutate(ml_bal_ins_norm = ml_bal_ins_tot/ml_bal_ins_tot[date == as.Date("2016-06-01", format="%Y-%m-%d")],
         ml_bal_arr_ins_norm = ml_bal_arr_ins_tot/ml_bal_arr_ins_tot[date == as.Date("2016-06-01", format="%Y-%m-%d")] )

plot_ML_stock_severe <- ggplot() + 
  geom_line(data=ML_severe_norm %>% filter(treated==1 & FM_damage==1), 
            aes(y=ml_bal_ins_norm, x=date, color="ML_ins_tot", linetype="ML_ins_tot"), size=0.75) +
  geom_line(data=ML_severe_norm %>% filter(treated==1 & FM_damage==1), 
            aes(y=ml_bal_arr_ins_norm, x=date, color="ML_ins_arr", linetype="ML_ins_arr"), size=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.0,1.6)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 1.6, by = 0.2)) +
  scale_colour_manual(name = "",
                      values = c("ML_ins_tot"="black","ML_ins_arr"="black"),
                      labels = c("ML_ins_tot"="Outstanding","ML_ins_arr"="Arrears")) +
  scale_linetype_manual(name = "",
                        values = c("ML_ins_tot"="dashed","ML_ins_arr"="solid"),
                        labels = c("ML_ins_tot"="Outstanding","ML_ins_arr"="Arrears")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line"))

plot_ML_stock_severe

cairo_ps(filename = "plot_stock_severe.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
print(plot_ML_stock_severe)
dev.off()

plot_ML_stock_nonsevere <- ggplot() + 
  geom_line(data=ML_nonsevere_norm %>% filter(treated==1 & FM_damage==0), 
            aes(y=ml_bal_ins_norm, x=date, color="ML_ins_tot", linetype="ML_ins_tot"), size=0.75) +
  geom_line(data=ML_nonsevere_norm %>% filter(treated==1 & FM_damage==0), 
            aes(y=ml_bal_arr_ins_norm, x=date, color="ML_ins_arr", linetype="ML_ins_arr"), size=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  labs(x=NULL, y=NULL) + coord_cartesian(ylim=c(0.0,1.6)) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(breaks = seq(0.0, 1.6, by = 0.2)) +
  scale_colour_manual(name = "",
                      values = c("ML_ins_tot"="black","ML_ins_arr"="black"),
                      labels = c("ML_ins_tot"="Outstanding","ML_ins_arr"="Arrears")) +
  scale_linetype_manual(name = "",
                        values = c("ML_ins_tot"="dashed","ML_ins_arr"="solid"),
                        labels = c("ML_ins_tot"="Outstanding","ML_ins_arr"="Arrears")) +
  theme(axis.line = element_line(colour = "black"),
        panel.background = element_blank(),
        panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        legend.position='bottom', legend.direction='horizontal',
        legend.background = element_blank(),
        legend.box.background = element_blank(),
        legend.key.width = unit(2, "line"))

plot_ML_stock_nonsevere

cairo_ps(filename = "plot_stock_nonsevere.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
print(plot_ML_stock_nonsevere)
dev.off()