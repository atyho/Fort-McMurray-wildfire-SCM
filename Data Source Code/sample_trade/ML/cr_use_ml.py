from pyspark.sql import SQLContext, SparkSession, Row
from pyspark import *
from pyspark.sql.functions import * # Call SparkSQL native functions such as abs, sqrt...
from pyspark.sql.types import DateType

spark=SparkSession\
.builder\
.appName("Python Spark Dataframe")\
.config(conf = SparkConf())\
.getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

# Comments / Remarks

# Exclude:
# 1. outdated accounts with MOP LIKE [0-5], i.e. no update for more than 3 months. 

# Caution with the first period of data or when significant data onboarding is observed:
# (1) accounts first reported in previous periods (6-month cutoff is used) are identified as new originations, as such the timing may be misrepresented
#     WORKAROUND: use OPENED_date instead of Run_date to count new loans in each period
#     Opened_date may not always be reported, and it may also underestimate new accounts in the last few sample periods due to delayed reporting
# (2) unable to determine if legacy accounts are charged off in the first period of appearance
#     i.e. FI may keep updating TU on their charged off accounts
#     Dropping new_file LIKE 'legacy' AND terminal LIKE 'WO' will likely to underestimate 'WO' in the first period

# CLOSED_date is not always populated with terminal LIKE '(WO|PD|AC)' for the following reasons:
# (1) charged off but not closed by FIs
# (2) CLOSED_date missing with CLOSED_INDIC LIKE 'Y'

########################
# Loading source files #
########################

# Load files for trade-level data
df_acct = sqlContext.read.parquet("/appdata/TU/TU_Official/Account/account_201[0-8]*_parq") \
  .select("TU_Consumer_ID","TU_Trade_ID","Joint_Account_ID","Primary_Indicator","ACCT_TYPE","PRODUCT_TYPE", \
          "BEST_AMT_FLAG","CREDIT_LIMIT","HIGH_CREDIT_AMT","NARRATIVE_CODE1","NARRATIVE_CODE2","CLOSED_INDIC", \
          "CURRENT_BALANCE","CHARGOFF_AMT","MOP","L3","TERMS_DUR","TERMS_FREQUENCY","TERMS_AMT", \
          col("PAYMT_PAT").substr(1,12).alias("PAYMT_PAT"),"MONTHS_REVIEWED","Insurance_Indicator", \
          to_date("Run_date").alias("Run_date"), \
          to_date(concat(col("REPORTED_DT").substr(1,4),lit("-"), \
                         col("REPORTED_DT").substr(5,2),lit("-"), \
                         col("REPORTED_DT").substr(7,2))).alias("REPORTED_date"), \
          to_date(concat(col("LAST_UPDATED_DT").substr(1,4),lit("-"), \
                         col("LAST_UPDATED_DT").substr(5,2),lit("-"), \
                         col("LAST_UPDATED_DT").substr(7,2))).alias("LAST_UPDATED_date"), \
          to_date(concat(col("OPENED_DT").substr(1,4),lit("-"), \
                         col("OPENED_DT").substr(5,2),lit("-"), \
                         col("OPENED_DT").substr(7,2))).alias("OPENED_date"), \
          to_date(concat(col("CLOSED_DT").substr(1,4),lit("-"), \
                         col("CLOSED_DT").substr(5,2),lit("-"), \
                         col("CLOSED_DT").substr(7,2))).alias("CLOSED_date"), \
          to_date(concat(col("FILE_SINCE_DT").substr(1,4),lit("-"), \
                         col("FILE_SINCE_DT").substr(5,2),lit("-"), \
                         col("FILE_SINCE_DT").substr(7,2))).alias("FILE_SINCE_date") ) \
  .where("PRODUCT_TYPE LIKE 'ML'").where("Run_date >= DATE('2010-12-01')")

df_acct.createOrReplaceTempView("df_acct")

# Load consumer-level CRC data
df_crc = sqlContext.read.parquet("../../sample_crc/df_crc.parquet")
df_crc.createOrReplaceTempView("df_crc")
df_crc.printSchema()

######################################################################
# Trim data to only include individuals who have ever lived in AB/SK #
######################################################################

# Only keep consumers in the filtered CRC data set
df_acct = spark.sql("SELECT df_acct.* FROM df_acct \
                    WHERE df_acct.TU_Consumer_ID IN (SELECT DISTINCT tu_consumer_id FROM df_crc) ")

df_acct.createOrReplaceTempView("df_acct")

#######################################
# Adjust mortgage insurance indicator #
#######################################

# Flag a mortgage as insured if it has ever been reported as insured
# Noted issues of flipping between insured and uninsured, esp for BNS/CIBC/National Bank 
df_ins_ls = spark.sql("SELECT DISTINCT TU_Trade_ID FROM df_acct \
                      WHERE Insurance_Indicator LIKE 'Y' ")
df_ins_ls.write.parquet(path="df_ins_ls.parquet", mode="overwrite")

df_ins_ls = sqlContext.read.parquet("df_ins_ls.parquet")
df_ins_ls.createOrReplaceTempView("df_ins_ls")

df_acct = spark.sql("SELECT df_acct.*, \
                      CASE WHEN df_ins_ls.TU_Trade_ID IS NOT NULL THEN 1 ELSE 0 END AS ml_insured \
                    FROM df_acct \
                    LEFT JOIN df_ins_ls \
                      ON df_acct.TU_Trade_ID = df_ins_ls.TU_Trade_ID ")

df_acct.createOrReplaceTempView("df_acct")

########################
# Define new variables #
########################

# Refine credit limit and charge-off amount
# NARRATIVE_CODE 'TC' represents third-party collections, most likely without further updates
# NARRATIVE_CODE 'WO' represents charge-offs
# Note that CURRENT_BALANCE may drop below the reported CHARGOFF_AMT if consumer repay defaulted debts
# In case of bankruptcy / consumer proposal, or foreclosed mortgages, CHARGOFF_AMT is the final charged-off amount
df_acct = spark.sql("SELECT *, \
                      CASE \
                        WHEN BEST_AMT_FLAG LIKE 'C' THEN IF(CURRENT_BALANCE/CREDIT_LIMIT < 1.1, CREDIT_LIMIT, CURRENT_BALANCE) \
                        WHEN BEST_AMT_FLAG LIKE 'H' THEN IF(CURRENT_BALANCE/HIGH_CREDIT_AMT < 1.1, HIGH_CREDIT_AMT, CURRENT_BALANCE) \
                        ELSE GREATEST(IFNULL(CREDIT_LIMIT,0),IFNULL(HIGH_CREDIT_AMT,0),CURRENT_BALANCE) END AS cr_lmt, \
                      CASE \
                        WHEN BEST_AMT_FLAG LIKE 'C' THEN CREDIT_LIMIT \
                        WHEN BEST_AMT_FLAG LIKE 'H' THEN HIGH_CREDIT_AMT \
                        ELSE NULL END AS cr_lmt_woadj, \
                      CASE \
                        WHEN NARRATIVE_CODE1 RLIKE '^(TC)$' OR NARRATIVE_CODE2 RLIKE '^(TC)$' THEN CURRENT_BALANCE \
                        WHEN (NARRATIVE_CODE1 RLIKE '^(WO)$' OR NARRATIVE_CODE2 RLIKE '^(WO)$') AND IFNULL(CHARGOFF_AMT,0)=0 THEN CURRENT_BALANCE \
                        ELSE LEAST(IFNULL(CHARGOFF_AMT,0),CURRENT_BALANCE) END AS chargoff_refine \
                    FROM df_acct ") \
          .drop("BEST_AMT_FLAG","HIGH_CREDIT_AMT","CREDIT_LIMIT","CHARGOFF_AMT")

df_acct.createOrReplaceTempView("df_acct")

# Calculate required monthly payment, noted inconsistency in frequency A, E, (L), (Q), T, (R)
df_acct = spark.sql("SELECT *, \
                      CASE \
                        WHEN TERMS_FREQUENCY LIKE 'A' THEN TERMS_AMT*1/12 \
                        WHEN TERMS_FREQUENCY LIKE 'B' THEN TERMS_AMT*2.16 \
                        WHEN TERMS_FREQUENCY LIKE 'D' THEN TERMS_AMT*0 \
                        WHEN TERMS_FREQUENCY LIKE 'E' THEN TERMS_AMT*2 \
                        WHEN TERMS_FREQUENCY LIKE 'L' THEN TERMS_AMT*1/2 \
                        WHEN TERMS_FREQUENCY LIKE 'M' THEN TERMS_AMT \
                        WHEN TERMS_FREQUENCY LIKE 'P' THEN TERMS_AMT*0 \
                        WHEN TERMS_FREQUENCY LIKE 'Q' THEN TERMS_AMT*1/3 \
                        WHEN TERMS_FREQUENCY LIKE 'S' THEN TERMS_AMT*1/6 \
                        WHEN TERMS_FREQUENCY LIKE 'T' THEN TERMS_AMT*1/3 \
                        WHEN TERMS_FREQUENCY LIKE 'W' THEN TERMS_AMT*4.33 \
                        WHEN TERMS_FREQUENCY LIKE 'Y' THEN TERMS_AMT*1/12 \
                        WHEN TERMS_FREQUENCY LIKE 'R' THEN TERMS_AMT*1/4 \
                        ELSE TERMS_AMT END AS TERMS_PAY \
                    FROM df_acct ")

df_acct.createOrReplaceTempView("df_acct")

###################
# Sample cleaning #
###################

# drop records with missing information on current balance and/or credit limit (rare)
# drop records that have not been updated for more than 3 months, except defaulted accounts for which updates may be infrequent
df_acct = df_acct.where("CURRENT_BALANCE IS NOT NULL").where("NOT (cr_lmt=0 OR cr_lmt IS NULL)") \
                 .where("(LAST_UPDATED_date >= add_months(Run_date,-3)) OR (MOP RLIKE '[7-9]')")

df_acct.createOrReplaceTempView("df_acct")

# Caution: NOT all TU_Trade_IDs get updated by FI, even though they share the same Joint_Account_ID
# Screening out accounts based on the logic below may accidentally drop some of the joint account holders
# and/or introduce miscounting in the number of account holders
# .where(Primary_Indicator=0).where("DATEDIFF(add_months(Run_date,-3),LAST_UPDATED_date)<0")

# Downward biased in measuring arrears roll-out transition if outdated accounts are not taken into account
# .where("(LAST_UPDATED_date >= add_months(Run_date,-3)) OR (MOP RLIKE '[7-9]')")

###################################################
# Create last period lag variables for trade data #
###################################################

# The following variables are created to identify several data issues

# new_file = [exist|new|legacy]: 
# Indicates if an account is newly originated loan or an exist loan.
# legacy accounts are those which appears in the dataset for the first time on Run_date but not a new loan.
# They are defined by the time gap (>6 months) between account opened date and the current Run_date.
# They exist mainly due to data onboarding, which is a significant issue in earlier periods, particularily for mortgages.

# terminal = [PD|WO|AC]:
# Created to indicate whether a loan is paid off (PD), charged off (WO), closed (AC), or none of the above (N) 
# Accounts may enter into charged off (WO) if:
# 1. Charged off by FI or send to third party collection (TC), see chargoff_refine.
# 2. Current balance drops below the charged off amount but not fully paid. In this case the charged off amount is also adjusted to the remaining loan balance, see chargoff_refine.
# Closed accounts may carry positive balance. For revolving loans like credit card, closed accounts means no more revolving credits.

# chargoff_new = [value]:
# total charged off amounts for new fully charged off accounts
# New charge-off excludes changes in the fully charged-off accounts, i.e. terminal = 'WO', in time t-1
# It prevents the newly charged off in current period being offset by (unexpected) repayments of previously charged off amounts

df_acct = spark.sql("SELECT df_now.*, \
                      CASE WHEN df_last.TU_Trade_ID IS NULL THEN \
                        CASE WHEN df_now.OPENED_date >= add_months(df_now.Run_date,-6) \
                          THEN 'new' ELSE 'legacy' END \
                        ELSE 'exist' END AS new_file, \
                      CASE \
                        WHEN (df_now.CURRENT_BALANCE=0 AND (df_now.CLOSED_INDIC LIKE 'Y' OR df_now.ACCT_TYPE RLIKE '^(M|I)$')) THEN 'PD' \
                        WHEN (df_now.CURRENT_BALANCE>0 AND (df_now.chargoff_refine>=df_now.CURRENT_BALANCE)) THEN 'WO' \
                        WHEN (df_now.CURRENT_BALANCE>0 AND (df_now.CLOSED_INDIC LIKE 'Y')) THEN 'AC' \
                        ELSE 'N' END AS terminal, \
                      CASE \
                        WHEN NOT(df_last.CURRENT_BALANCE>0 AND (df_last.chargoff_refine>=df_last.CURRENT_BALANCE)) \
                          AND (df_now.CURRENT_BALANCE>0 AND (df_now.chargoff_refine>=df_now.CURRENT_BALANCE)) \
                        THEN df_now.chargoff_refine ELSE 0 END AS chargoff_new \
                    FROM df_acct AS df_now \
                    LEFT JOIN df_acct AS df_last \
                      ON (df_last.Run_date = add_months(df_now.Run_date,-1)) \
                      AND (df_last.TU_Trade_ID = df_now.TU_Trade_ID) \
                    WHERE \
                      df_last.TU_Trade_ID IS NULL OR \
                      ( df_last.TU_Trade_ID IS NOT NULL AND \
                        NOT ( (df_last.CURRENT_BALANCE=0 AND df_now.CURRENT_BALANCE=0) AND \
                              (df_last.CLOSED_INDIC LIKE 'Y' OR df_last.ACCT_TYPE RLIKE '^(M|I)$') ) ) ") \
          .where("NOT(new_file RLIKE 'legacy' AND terminal RLIKE '(PD)')") \
          .drop("CLOSED_INDIC","ACCT_TYPE","PRODUCT_TYPE")

df_acct.createOrReplaceTempView("df_acct")

# WHERE clause is used to reduce file size
# The conditions filter out accounts that are PREVIOUSLY PAID, with account anomalies taken into consideration
# Known issues that may affect the count of number of account holders:
# 1. closed accounts with zero balance reopened with positive balance in the current period
# 2. Time lag in reporting closed accounts, esp. for non-primary holders
# 3. M and I with completed payment but not indicated as closed
# 4. CHARGOFF_AMT may be missing or =0 for accounts charged off with narrative code "WO"

# Caution: While it is possible to calculate account-level changes using the above logic
# The result will not be accurate if outdated accounts are dropped, because such changes are not taken into account
# E.g. mortgages renewed/ported to another FI, possibly causing the old accounts to be outdated
# Extra care is needed and preferably changes should be calculated after reporting time adjustments

##########################################
# Create group CRC from trade-level data #
##########################################

# Create group CRC based on ALL account holders CRCs
df_gcrc = spark.sql("SELECT Run_date, Joint_Account_ID, \
                      COUNT(TU_Consumer_ID) AS n_now \
                    FROM df_acct \
                    GROUP BY Run_date, Joint_Account_ID ")

df_gcrc.createOrReplaceTempView("df_gcrc")

# Merge group CRC to trade-level data
df_acct = spark.sql("SELECT df_acct.*, \
                      df_gcrc.n_now \
                    FROM df_acct \
                    LEFT JOIN df_gcrc \
                      ON df_acct.Joint_Account_ID = df_gcrc.Joint_Account_ID \
                      AND df_acct.Run_date = df_gcrc.Run_date ")

df_acct.createOrReplaceTempView("df_acct")

###########################
# Save dataframe to files #
###########################

print('Saving data to files...')
df_acct.printSchema()
df_acct.where("Run_date >= DATE('2011-01-01')").write.parquet(path="df_acct_all.parquet", mode="overwrite")

# Stop the sparkContext and cluster after processing
sc.stop()
