# GENERAL INFORMATION

Title: Data and Code Supporting 'We Didn't Start the Fire: Effects of a Natural Disaster on Consumers’ Financial Distress' 

DOI of publication: https://doi.org/10.1016/j.jeem.2023.102790

Author Information 

1. Name: Anson T. Y. Ho
- Institution: Toronto Metropolitan University; Bank of Canada Financial System Research Centre
- Email: atyho@torontomu.ca; ansonho@bankofcanada.ca
- [ORCID:] https://orcid.org/0000-0002-1772-2320 

2. Name: Kim P Huynh
- Institution: Department of Currency, Bank of Canada
- Email: khuynh@bankofcanada.ca
- [ORCID:] https://orcid.org/0000-0003-2015-0244 

3. Name: David T. Jacho-Chavez 
- Institution: Department of Economics, Emory University
- Email: djachocha@emory.edu
- [ORCID:] https://orcid.org/0000-0003-3511-422X 

4. Name: Genevieve Vallee
- Institution: Department of Financial Stability, Bank of Canada
- Email: gvallee@bank-banque-canada.ca
- [ORCID:]

# LIST OF FILES & FILE OVERVIEW

For data processing:

Folder: sample_crc

1. Filename: df_crc.py
Format: Python program
brief description: Create sample of consumers

2. Filename: df_tenure.py
Format: Python program
brief description: Estimate consumers' tenure

3. Filename: cr_use.py
Format: Python program
brief description: Joint data set generate dby df_crc.py and df_tenure.py

Folder: sample_trade/ML

1. Filename: cr_use_ml.py
Format: Python program
brief description: Create sample of trade-level mortgage data

2. Filename: cr_use_ml_time_adj.py
Format: Python program
brief description: Adjust for reporting time differences

3. Filename: cr_use_ml_ind.py
Format: Python program
brief description: Aggregate trade-level data to individual-level data

Folder: sample_trade/BC
Same file structure as sample_trade/ML, replace "ml" by "bc" in file names.

For data analysis:

1. Filename: FM_synth.R
Format: R program
brief description: Conduct the SCM analysis

2. Filename: FM_synth_report.R
Format: R program
brief description: Generate tables and figures for the results

3. Filename: FM_synth_CI_severe.R and FM_synth_CI_nonsevere.R
Format: R program
brief description: Generate the Ci for severely damaged areas and other areas, respectively.

# DATA CITATION INFORMATION (Where to access the data)

TransUnion Canada Consumer Credit Dataset available at the Bank fo Canada.

See TransUnion credit microdata at Data and Information Resources A to Z for further information.

# DATA AVAILABILITY STATEMENT 

The consumer credit data used in our analysis is owned by the Bank via an agreement with TransUnion Canada. Data file is excluded due to redistribution restriction. The programs provided for “We Didn’t Start the Fire: Effects of a Natural Disaster on Consumers' Financial Distress,” which includes the source code for reproducing the data used for our empirical analysis, can be shared internally with permission from the authors -- please contact Kim P. Huynh, Director of Economic Research & Analysis, Currency Department (khuynh@bankofcanada.ca) for further information. 

Externally, under the contractual agreement with TransUnion, the data are not publicly available. The Bank of Canada does, however, have a process for external researchers to work with these data. The Bank of Canada's Financial System Research Center is a hub for research on household finance (https://www.bankofcanada.ca/research/financial-system-research-centre/). Interested parties, who are Canadian citizens or permanent residents, can contact Jason Allen (e-mail: Jallen@bankofcanada.ca) or the Managing Director of research Jim MacGee (e-mail: JMacGee@bankofcanada.ca).

# DATA-SPECIFIC INFORMATION

Specialized formats or other abbreviations used: 
- comma-separated-values (CSV)
- RData for R objects

# CODE-SPECIFIC INFORMATION

Requirements: R/3.4.0, Apache Spark/2.4.4 on Edith 2.0

Other: R package
- Synth 1.1-6
- dplyr 1.0.10
- doParallel 1.0.17
- ggplot2 3.4.0
- Cairo 1.6-0

# [CODE/SOFTWARE/PROGRAM] CITATION INFORMATION: 

Firpo, S., Possebom, V., 2018. Synthetic control method: Inference, sensitivity analysis and confidence sets. Journal of Causal Inference 6. URL: https://doi.org/10.1515/jci-2016-0026, doi:doi:10.1515/jci-2016-0026.

# METHODOLOGICAL INFORMATION

Methods for processing the data: Submitted data were generated from the raw data stored on Edith 2.0 using the programs stored in folder Data Source Code. The folders should be executed in the following order: sample_crc, sample_trade, and then sample_aggregate. The sample_aggregate folder contains the final FSA level dataset named df_synth.csv.

For quality-assurance procedures performed on the data, see Ho, A.T.Y., Morin, L., Paarsch, H.J., Huynh, K.P., 2022. Consumer credit usage in canada during the coronavirus pandemic. Canadian Journal of Economics/Revue canadienne
d'economique 55, 88–114. doi:10.1111/caje.12544.

# INSTRUCTIONS FOR GENERATING RESULTS

1.0 Run Data Source Code to generate the FSA-level data set

2.0 Run Analytical Code to generate the results

2.1 Main program is located in mortgage_baseline folder
 - Run FM_synth.R to conduct the SCM analysis
 - Run FM_synth_report.R to generate tables and figures for the results
 - Run FM_synth_CI_severe.R and FM_synth_CI_nonsevere.R to generate the Ci for severely damaged areas and other areas, respectively.

2.2 For sensitivity tests, robust_backdating and robust_leave_one_out folders contain the codes for backdating test and the leave-one-out analysis, respectively. The files follow the same structure as in 2.1.

2.3 For other consumer finances, folde credit_card and credit_scores contain the analysis on credit card balances and credit scores, respectively. The files follow the same structure as in 2.1.
