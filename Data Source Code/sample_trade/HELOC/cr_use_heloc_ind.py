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

#########################
# Load trade-level data #
#########################

df_acct = sqlContext.read.parquet("df_acct.parquet")
df_acct.createOrReplaceTempView("df_acct")
df_acct.printSchema()

################################################
# Aggregate trade-level data to consumer level #
################################################

# Already excluded:
# 1. outdated accounts
# 2. legacy accounts that are fully paid (PD)

# Further exclude:
# 1. fully paid (PD) / charged off (WO) accounts for number of accounts and credit limit

df_ind = spark.sql("SELECT TU_Consumer_ID, Ref_date, \
                     SUM(IF(terminal NOT RLIKE 'WO', CURRENT_BALANCE, 0)) AS heloc_bal, \
                     SUM(IF(MOP RLIKE '[4-5]' AND terminal NOT RLIKE 'WO', CURRENT_BALANCE, 0)) AS heloc_bal_arr, \
                     SUM(IF(MOP RLIKE '[7-9]' AND terminal NOT RLIKE 'WO', CURRENT_BALANCE, 0)) AS heloc_bal_def, \
                     SUM(IF(terminal RLIKE 'WO', chargoff_refine, 0)) AS heloc_chargoff, \
                     SUM(IF(terminal RLIKE 'WO', chargoff_new, 0)) AS heloc_chargoff_new \
                   FROM df_acct \
                   GROUP BY TU_Consumer_ID, Ref_date ")

df_ind.createOrReplaceTempView("df_ind")

# Exclude fully paid (PD) / charged off (WO) accounts for number of accounts, credit limit, and credit score at origination
df_ind_active = spark.sql("SELECT TU_Consumer_ID, Ref_date, \
                            COUNT(TU_Trade_ID) AS N_heloc, \
                            SUM(cr_lmt) AS heloc_lmt \
                          FROM df_acct \
                          WHERE terminal NOT RLIKE '(PD|WO)' \
                          GROUP BY TU_Consumer_ID, Ref_date ")

df_ind_active.createOrReplaceTempView("df_ind_active")

df_ind = spark.sql("SELECT df_ind.*, \
                     df_ind_active.N_heloc, df_ind_active.heloc_lmt \
                   FROM df_ind \
                   LEFT JOIN df_ind_active \
                     ON df_ind.TU_Consumer_ID = df_ind_active.TU_Consumer_ID \
                     AND df_ind.Ref_date = df_ind_active.Ref_date ")

df_ind.createOrReplaceTempView("df_ind")

# Save working data set
print('Saving data to files...')
df_ind.printSchema()
df_ind.write.parquet(path="df_ind.parquet", mode="overwrite")

# Stop the sparkContext and cluster after processing
sc.stop()