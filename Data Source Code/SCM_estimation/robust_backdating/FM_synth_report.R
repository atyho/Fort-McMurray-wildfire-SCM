# Clean and organize the work environment
rm(list = ls())

library(dplyr)
library(Cairo)

library(openxlsx)
library(ggplot2)
library(ggthemes)
options(scipen=999)  # turn-off scientific notation like 1e+48
theme_set(theme_bw()) 

load("FM_synth_backdated.RData")

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

# Customized graph
FM_severe.path <- bind_cols(date = FM_severe.data$tag$time.plot,
                            Y1 = FM_severe.data$Y1plot[,1],
                            Y0 = (FM_severe.data$Y0plot %*% FM_severe.out$solution.w)[,1])

plot_path_severe <- ggplot(data = FM_severe.path) + 
  geom_line(aes(x=date, y=Y1, color="Treated", linetype="Treated"), linewidth=0.75) +
  geom_line(aes(x=date, y=Y0, color="Synthetic", linetype="Synthetic"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  geom_vline(xintercept=as.Date("2015-06-01", format="%Y-%m-%d"), color="black",linetype="dotted") +
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

cairo_ps(filename = "figures/plot_backdated_severe.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
plot_path_severe
dev.off()

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

# Customized graph
FM_nonsevere.path <- bind_cols(date = FM_nonsevere.data$tag$time.plot,
                               Y1 = FM_nonsevere.data$Y1plot[,1],
                               Y0 = (FM_nonsevere.data$Y0plot %*% FM_nonsevere.out$solution.w)[,1])

plot_path_nonsevere <- ggplot(data = FM_nonsevere.path) + 
  geom_line(aes(x=date, y=Y1, color="Treated", linetype="Treated"), linewidth=0.75) +
  geom_line(aes(x=date, y=Y0, color="Synthetic", linetype="Synthetic"), linewidth=0.75) +
  geom_vline(xintercept=as.Date("2016-06-01", format="%Y-%m-%d"), color="black",linetype="solid") +
  geom_vline(xintercept=as.Date("2015-06-01", format="%Y-%m-%d"), color="black",linetype="dotted") +
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

plot_path_nonsevere
  
cairo_ps(filename = "figures/plot_backdated_nonsevere.eps", width=3, height=3, pointsize = 12, fallback_resolution = 300)
plot_path_nonsevere
dev.off()

saveWorkbook(wb, "FM_synth_backdated.xlsx", overwrite = TRUE)
