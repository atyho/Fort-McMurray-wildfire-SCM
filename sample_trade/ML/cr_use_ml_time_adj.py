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
df_acct = sqlContext.read.parquet("df_acct_all.parquet")
df_acct.createOrReplaceTempView("df_acct")
df_acct.printSchema()

# Load consumer-level CRC data
df_crc = sqlContext.read.parquet("../../sample_crc/df_cdb.parquet")
df_crc.createOrReplaceTempView("df_crc")
df_crc.printSchema()

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

############################################################
# Revise MOP for recording updates on financial assistance #
############################################################

# Get the payment pattern on 2017-02-01 to revise the MOP between 2016-06-01 and 2016-12-01
df_mop_rev = spark.sql("SELECT TU_Trade_ID, Ref_date, MOP, PAYMT_PAT, MONTHS_REVIEWED \
                       FROM df_acct WHERE Ref_date = DATE('2017-02-01') ")

df_mop_rev.createOrReplaceTempView("df_mop_rev")

df_acct = spark.sql("SELECT df_acct.*, \
                    SUBSTR(df_mop_rev.PAYMT_PAT, (df_mop_rev.MONTHS_REVIEWED -df_acct.MONTHS_REVIEWED +1), 1) AS MOP_hist \
                  FROM df_acct \
                  LEFT JOIN df_mop_rev \
                    ON df_acct.TU_Trade_ID = df_mop_rev.TU_Trade_ID \
                    AND df_acct.Ref_date BETWEEN DATE('2016-06-01') AND df_mop_rev.Ref_date ")

df_acct.createOrReplaceTempView("df_acct")

df_acct = spark.sql("SELECT df_acct.*, \
                      CASE \
                        WHEN MOP_hist IS NULL THEN MOP \
                        WHEN MOP NOT LIKE MOP_hist THEN MOP_hist \
                        ELSE MOP END AS MOP_rev \
                    FROM df_acct ").drop("MOP_hist")

df_acct.createOrReplaceTempView("df_acct")

# Merge account data with CRC to identify treated individuals
df_acct = spark.sql("SELECT df_acct.*, df_crc.treated_ind \
                    FROM df_acct \
                    LEFT JOIN df_crc \
                      ON df_acct.TU_Consumer_ID = df_crc.tu_consumer_id \
                      AND df_acct.Run_date = df_crc.Run_Date ")

df_acct.createOrReplaceTempView("df_acct")

# Account for deferral between May and September, 2016 (some people may only deal with it after return in June)
df_pre_arr = spark.sql("SELECT DISTINCT TU_Trade_ID \
                       FROM df_acct \
                       WHERE \
                         treated_ind = 1 \
                         AND Ref_date BETWEEN DATE('2016-03-01') AND DATE('2016-05-01') \
                         AND MOP_rev NOT RLIKE '[X0-1]' ")
                         
df_pre_arr.createOrReplaceTempView("df_pre_arr")

df_post_arr = spark.sql("SELECT DISTINCT TU_Trade_ID \
                        FROM df_acct \
                        WHERE \
                          treated_ind = 1 \
                          AND Ref_date BETWEEN DATE('2016-11-01') AND DATE('2016-12-01') \
                          AND MOP_rev NOT RLIKE '[X0-1]' ")

df_post_arr.createOrReplaceTempView("df_post_arr")

df_acct = spark.sql("SELECT df_acct.*, \
                      CASE WHEN df_acct.MOP_rev NOT RLIKE '[X0-1]' AND treated_ind = 1 \
                        AND ( df_acct.Ref_date BETWEEN DATE('2016-06-01') AND DATE('2016-10-01') ) \
                        AND df_pre_arr.TU_Trade_ID IS NULL AND df_post_arr.TU_Trade_ID IS NULL \
                        THEN 1 ELSE df_acct.MOP_rev END AS MOP_adj \
                    FROM df_acct \
                    LEFT JOIN df_pre_arr ON df_acct.TU_Trade_ID = df_pre_arr.TU_Trade_ID \
                    LEFT JOIN df_post_arr ON df_acct.TU_Trade_ID = df_post_arr.TU_Trade_ID ") \
          .withColumnRenamed("MOP", "MOP_old").withColumnRenamed("MOP_adj", "MOP")

df_acct.createOrReplaceTempView("df_acct")

#########################
# Save working data set #
#########################

print('Saving data to files...')
df_acct.printSchema()
df_acct.write.parquet(path="df_acct.parquet", mode="overwrite")

# Stop the sparkContext and cluster after processing
sc.stop()
