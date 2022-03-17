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

#############
# Load data #
#############

# Load trade-level data
df_acct = sqlContext.read.parquet("df_acct_*.parquet") \
  .drop("ACTIVITY_PERIOD","BIN","deferral","Joint_Account_ID","Primary_Indicator","TERMS_DUR","LAST_UPDATED_date","cr_lmt_woadj")
df_acct.createOrReplaceTempView("df_acct")
df_acct.printSchema()

##############################################
# Reconstruct dataset based on reported date #
##############################################

# Reconstruct the reference date (standardize to the 1st date of the month)
df_ref_dt = spark.sql("SELECT DISTINCT TU_Trade_ID, \
                        DATE(CONCAT(YEAR(Run_date),'-',MONTH(Run_date),'-','01')) AS Ref_date \
                      FROM df_acct")

df_ref_dt.createOrReplaceTempView("df_ref_dt")

# Keep unique records based on the reported and the most recent run date
# Use the most recent run date because some records (rare) got updated by TU without new reports from FI
df_acct_ls = spark.sql("SELECT TU_Trade_ID, REPORTED_date, \
                         MAX(Run_date) AS last_run_date \
                       FROM df_acct \
                       GROUP BY TU_Trade_ID, REPORTED_date ")

df_acct_ls.createOrReplaceTempView("df_acct_ls")

# Matching the last reported date to the (TU_Trade_ID, Ref_date) tuple
# Find the record with the minimum time lag by finding MAX(Run_date)
# Only retain the record with the minimum time lag
df_ref_dt = spark.sql("SELECT df_ref_dt.TU_Trade_ID, df_ref_dt.Ref_date, \
                        MAX(df_acct_ls.last_run_date) AS select_run_date \
                      FROM df_ref_dt \
                      LEFT JOIN df_acct_ls \
                        ON df_acct_ls.TU_Trade_ID = df_ref_dt.TU_Trade_ID \
                        AND df_acct_ls.REPORTED_date BETWEEN add_months(df_ref_dt.Ref_date,-3) AND df_ref_dt.Ref_date \
                      GROUP BY df_ref_dt.TU_Trade_ID, df_ref_dt.Ref_date ")

df_ref_dt.createOrReplaceTempView("df_ref_dt")

# Merge reference date with account-level data
df_acct = spark.sql("SELECT df_ref_dt.Ref_date, df_acct.* \
                    FROM df_ref_dt \
                    INNER JOIN df_acct \
                      ON df_ref_dt.TU_Trade_ID = df_acct.TU_Trade_ID \
                      AND df_ref_dt.select_run_date = df_acct.Run_date \
                    WHERE NOT(df_ref_dt.Ref_date >= add_months(df_acct.REPORTED_date,1) AND df_acct.terminal RLIKE '(PD)') ")

df_acct.createOrReplaceTempView("df_acct")

#########################
# Save working data set #
#########################

print('Saving data to files...')
df_acct.printSchema()
df_acct.write.parquet(path="df_acct.parquet", mode="overwrite")

# Stop the sparkContext and cluster after processing
sc.stop()
