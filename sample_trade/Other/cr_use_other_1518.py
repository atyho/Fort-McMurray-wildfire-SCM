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
# 1. *outdated accounts* with MOP LIKE [0-5]

# Caution with the first period of data (at least for measuring new account and the total charged offs) because:
# (1) accounts opened in previous quarters are identified as new accounts
#     WORKAROUND: use OPENED_date instead of Run_date, still it captures more new accounts in the first period of data
# (2) unable to determine if legacy accounts are charged off in the current period
#     i.e. FI may keep updating TU on charged off accounts
#     WORKAROUND: drop new_file LIKE 'legacy' AND terminal LIKE 'WO' - likely to underestimate WO in the first month/quarter

# CLOSED_date is not always populated with terminal LIKE '(WO|PD|CL)' for the following reasons:
# (1) charged off but not closed by FIs
# (2) CLOSED_date missing with CLOSED_INDIC LIKE 'Y'

########################
# Loading source files #
########################

# Load files for trade-level data
df_acct = sqlContext.read.parquet("/appdata/TU/TU_Official/Account/account_201[4-8]*_parq") \
  .select("TU_Consumer_ID","TU_Trade_ID","Joint_Account_ID","Primary_Indicator","ACCT_TYPE","PRODUCT_TYPE", \
          "BEST_AMT_FLAG","CREDIT_LIMIT","HIGH_CREDIT_AMT","NARRATIVE_CODE1","NARRATIVE_CODE2","CLOSED_INDIC", \
          "CURRENT_BALANCE","CHARGOFF_AMT","MOP","L3","TERMS_DUR","TERMS_FREQUENCY","TERMS_AMT", \
          col("PAYMT_PAT").substr(1,12).alias("PAYMT_PAT"),"MONTHS_REVIEWED", \
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
  .where("NOT(ACCT_TYPE LIKE 'M') AND NOT(PRODUCT_TYPE RLIKE '(BC|ML|UT)')").where("Run_date >= DATE('2014-12-01')")

df_acct.createOrReplaceTempView("df_acct")

########################
# Define new variables #
########################

# Refine credit limit and charge-off amount
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

# Calculate required monthly payment, refine term payment amount - inconsistency in A, E, (L), (Q), T, (R)
df_acct = spark.sql("SELECT *, \
                      CASE \
                        WHEN TERMS_FREQUENCY LIKE 'A' THEN TERMS_AMT*2 \
                        WHEN TERMS_FREQUENCY LIKE 'B' THEN TERMS_AMT*2.16 \
                        WHEN TERMS_FREQUENCY LIKE 'D' THEN TERMS_AMT*0 \
                        WHEN TERMS_FREQUENCY LIKE 'E' THEN TERMS_AMT*2 \
                        WHEN TERMS_FREQUENCY LIKE 'L' THEN TERMS_AMT*1/2 \
                        WHEN TERMS_FREQUENCY LIKE 'M' THEN TERMS_AMT \
                        WHEN TERMS_FREQUENCY LIKE 'P' THEN TERMS_AMT*0 \
                        WHEN TERMS_FREQUENCY LIKE 'Q' THEN TERMS_AMT*1/3 \
                        WHEN TERMS_FREQUENCY LIKE 'R' THEN TERMS_AMT*1/4 \
                        WHEN TERMS_FREQUENCY LIKE 'S' THEN TERMS_AMT*1/6 \
                        WHEN TERMS_FREQUENCY LIKE 'T' THEN TERMS_AMT*1/4 \
                        WHEN TERMS_FREQUENCY LIKE 'W' THEN TERMS_AMT*4.33 \
                        WHEN TERMS_FREQUENCY LIKE 'Y' THEN TERMS_AMT*1/12 \
                        ELSE TERMS_AMT END AS TERMS_PAY \
                    FROM df_acct ")

df_acct.createOrReplaceTempView("df_acct")

#######################
# Refine product type #
#######################

# Using NARRATIVE_CODE = 'SB' for AL may be questionable
# Most of them are from BNS (TERMS_DUR > 0) and BMO (TERMS_DUR = 0)
# Among these 'SB' IL, some of them are <15k (about 13%) or <7k (about 7%)
# They are mostly with TERMS_DUR > 0 & TERMS_DUR < 36, indeed loans with TERMS_DUR = 0 have better cr_lmt 
# Those 'SB' with TERMS_DUR = 0 have cr_lmt_woadj = 0 but cr_lmt looks reseasonable

# Identify HELOC: identify accounts by highest historical credit limit >= 50000
df_heloc = sqlContext.read.parquet("../HELOC/df_heloc_ls.parquet")
df_heloc.createOrReplaceTempView("df_heloc")
 
df_acct = spark.sql("SELECT df_acct.*, \
                      CASE \
                        WHEN PRODUCT_TYPE LIKE 'ML' THEN 'ML' \
                        WHEN PRODUCT_TYPE LIKE 'BC' THEN 'BC' \
                        WHEN PRODUCT_TYPE LIKE 'LC' THEN \
                          CASE \
                            WHEN df_heloc.TU_Trade_ID IS NOT NULL THEN 'HELOC' \
                            WHEN NARRATIVE_CODE1 RLIKE '^(SB)$' OR NARRATIVE_CODE2 RLIKE '^(SB)$' THEN 'HELOC' \
                            ELSE 'LC' END \
                        WHEN PRODUCT_TYPE LIKE 'AL' THEN 'AL' \
                        WHEN PRODUCT_TYPE LIKE 'SL' THEN 'SL' \
                        WHEN ACCT_TYPE LIKE 'I' THEN \
                          CASE \
                            WHEN PRODUCT_TYPE LIKE 'IL' AND cr_lmt > 65000 OR TERMS_DUR > 96 THEN 'IL_unusual' \
                            WHEN L3 RLIKE '^(EasyFinancial|CITI Financial|National Money Mart)$' THEN 'h_int_IL' \
                            WHEN PRODUCT_TYPE LIKE 'IL' AND (cr_lmt BETWEEN 15000 AND 65000) THEN 'AL_Dcr' \
                            WHEN PRODUCT_TYPE LIKE 'IL' AND (NARRATIVE_CODE1 LIKE 'SB' OR NARRATIVE_CODE2 LIKE 'SB') THEN 'AL_Dsb' \
                            ELSE 'IL' END \
                        WHEN PRODUCT_TYPE LIKE 'UT' THEN 'UT' \
                        ELSE 'Other' END AS product_cat \
                    FROM df_acct \
                    LEFT JOIN df_heloc \
                      ON df_acct.TU_Trade_ID = df_heloc.TU_Trade_ID ") \
          .drop("NARRATIVE_CODE1","NARRATIVE_CODE2")

df_acct.createOrReplaceTempView("df_acct")

# Exclude HELOC
df_acct = df_acct.where("product_cat NOT LIKE 'HELOC' ")
df_acct.createOrReplaceTempView("df_acct")

###################
# Sample cleaning #
###################

df_acct = df_acct.where("CURRENT_BALANCE IS NOT NULL").where("NOT (cr_lmt=0 OR cr_lmt IS NULL)") \
                 .where("(LAST_UPDATED_date >= add_months(Run_date,-3)) OR (MOP RLIKE '[7-9]')")

df_acct.createOrReplaceTempView("df_acct")

# NOT all TU_Trade_IDs get updated by FI, even though they share the same Joint_Account_ID
# Screening out accounts based on the following logic may introduce miscounting in the number of account holders
# and/or accidentally drop some of the joint account holders when .where(Primary_Indicator=0) is used
#                 .where("DATEDIFF(add_months(Run_date,-3),LAST_UPDATED_date)<0")

# downward biased in arrears roll-out transition calculation if the follow filters are applied
#                 .where("NOT (CURRENT_BALANCE = 0)").where("MOP NOT RLIKE '[8-9]'")

###################################################
# Create last period lag variables for trade data #
###################################################

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

# Accounts may enter into charged off status if:
# 1. Charged off by FI or send to third party collection (TC)
# 2. Was partially charged off and the current balance drops below the charged off amount (but not fully paid)  
# New charge-off excludes changes in the charged-off amount for accounts already in 'WO' in time t-1

# Possible to calculate account-level changes using the above logic
# But the result will not be right if outdated accounts are discarded, because changes due to discarding accounts is not taken into account
# e.g. mortgage renewed/ported to another FI, possibly causing the old account to be outdated

# WHERE clause is used to reduce file size
# The conditions filter out accounts that are PREVIOUSLY PAID, with account abnormalities taken into account
# Known issues that may affect the count of number of account holders:
# 1. closed accounts with zero balance reopened with positive balance in the current period
# 2. Time lag in reporting closed accounts, esp. for non-primary holders
# 3. M and I with completed payment but not indicated as closed
# 4. CHARGOFF_AMT may be missing or =0 for accounts charged off with narrative code "WO"

df_acct.createOrReplaceTempView("df_acct")

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
df_acct.where("Run_date >= DATE('2015-01-01')").write.parquet(path="df_acct_1518.parquet", mode="overwrite")

# Stop the sparkContext and cluster after processing
sc.stop()
