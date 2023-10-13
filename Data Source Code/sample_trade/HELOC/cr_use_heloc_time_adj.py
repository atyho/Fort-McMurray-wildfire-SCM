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
df_crc = sqlContext.read.parquet("../../sample_crc/df_crc.parquet")
df_crc.createOrReplaceTempView("df_crc")
df_crc.printSchema()

##############################################
# Reconstruct dataset based on reported date #
##############################################

# Reconstruct the reference date (standardize to the 1st date of the month)
# For historical reasons, the run date has switched from the 1st to the 15th of the month in 2020

# CAUTION: this method reflects the most current status of an account
# If the time lag between reporting and archiving is more than a month, which can be the case due to the file processing time:
# (1) The first entry will likely be omitted, because there will be a more recent update for the earliest Run_Date
# (2) The last entry will be repeated because there will be no new updates if the latest data is used (depending on data coverage)
# E.g. Reported on Jan 25th, first appears on Run_date Mar 1st; meanwhile another new report on Feb 25th, update appears on Run_date Apr 1st
# This method will omit the Jan 25th record, because the Feb 25th report appeared on Run_date Apr 1st reflects to latest info as of Mar 1st 
# Workaround is to supplement exisitng Run_Date with additional periods based on reporte_dates

df_ref_dt = spark.sql("SELECT DISTINCT TU_Trade_ID, \
                        DATE(CONCAT(YEAR(add_months(REPORTED_date,1)),'-',MONTH(add_months(REPORTED_date,1)),'-','01')) AS Ref_date \
                        FROM df_acct \
                      UNION \
                      SELECT DISTINCT TU_Trade_ID, \
                        DATE(CONCAT(YEAR(Run_date),'-',MONTH(Run_date),'-','01')) AS Ref_date \
                        FROM df_acct ")

df_ref_dt.createOrReplaceTempView("df_ref_dt")

# Keep unique records based on the reported date and the most recent run date
# Use the most recent run date because some records (though they are rare) got updated by TU without new reports from FI
# These anomalies can be identified by a significant time gap between reported date and last updated date
# Using the most recent run date can ensure that the record has the latest update
df_acct_ls = spark.sql("SELECT TU_Trade_ID, REPORTED_date, \
                         MAX(Run_date) AS last_run_date \
                       FROM df_acct \
                       GROUP BY TU_Trade_ID, REPORTED_date ")

df_acct_ls.createOrReplaceTempView("df_acct_ls")

# Matching the last reported date to the (TU_Trade_ID, Ref_date) tuple
# Find the record with the minimum time lag by finding MAX(Run_date), i.e. the most up-to-date report with respect to the ref_date
# Return NULL for records that are at least 3 months older than the ref_date
df_ref_dt = spark.sql("SELECT df_ref_dt.TU_Trade_ID, df_ref_dt.Ref_date, \
                        MAX(df_acct_ls.last_run_date) AS select_run_date \
                      FROM df_ref_dt \
                      LEFT JOIN df_acct_ls \
                        ON df_acct_ls.TU_Trade_ID = df_ref_dt.TU_Trade_ID \
                        AND df_acct_ls.REPORTED_date BETWEEN add_months(df_ref_dt.Ref_date,-3) AND df_ref_dt.Ref_date \
                      GROUP BY df_ref_dt.TU_Trade_ID, df_ref_dt.Ref_date ")

df_ref_dt.createOrReplaceTempView("df_ref_dt")

# Merge reference date with account-level data
# Exclude outdated records (NULL select_run_date) based on the revised time frame
# Most of the outdated records were already purged using LAST_UPDATED_date >= add_months(Run_date,-3)
# Drop duplicated records for accounts in terminal conditions, because they may show up twice due to the timing of the reported date
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

# Use the payment pattern on 2017-02-01 to revise the MOP between 2016-06-01 and 2016-12-01
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

# Account treated as deferral between May and September 2016, retrospectively (some people may only deal with it afterwards)
# Adjustments are made for accounts in good standing before AND after the deferral periods
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
          .withColumnRenamed("MOP", "MOP_raw").withColumnRenamed("MOP_adj", "MOP")

df_acct.createOrReplaceTempView("df_acct")

######################################
# Location when HELOC was originated #
######################################

# Real estates are immobile and they are collaterialized against specific mortgage contracts
# Consumers may update their primary residential addresses when they move, without terminating their existing mortgages
# For example, someone may move to a new address while keeping their previous home as an investment property
# Thus, one's current residential location may misrepresent the location of the property and the associated mortgage contracts  
# Caution: the data will still not be able to identify a different location for one's second home and/or investment property, if it has never been used as a primary residency 

# Merge the HELOC holder's location with the account-level records
# Use the location reported in the CRC files on the same Run_date 
df_acct_loc = spark.sql("SELECT df_acct.TU_Trade_ID, df_acct.Ref_date, \
                          df_crc.fsa, df_crc.Encrypted_LDU, df_crc.prov, \
                          df_crc.distance_min, df_crc.FM_damage \
                        FROM df_acct \
                        LEFT JOIN df_crc \
                          ON df_acct.TU_Consumer_ID = df_crc.tu_consumer_id \
                          AND df_acct.Run_Date = df_crc.Run_date ")
                      
df_acct_loc.createOrReplaceTempView("df_acct_loc")

# Find out the earliest record in the dataset and use that to determine a consumer's mortgage location
df_loan_origin = spark.sql("SELECT TU_Trade_ID, MIN(Ref_date) AS min_loan_date \
                           FROM df_acct_loc \
                           WHERE fsa IS NOT NULL AND Encrypted_LDU IS NOT NULL \
                           GROUP BY TU_Trade_ID ")

df_loan_origin.createOrReplaceTempView("df_loan_origin")

df_loan_origin = spark.sql("SELECT df_loan_origin.*, \
                             df_acct_loc.fsa AS fsa_loan, df_acct_loc.Encrypted_LDU AS Encrypted_LDU_loan, \
                             df_acct_loc.prov AS prov_loan, \
                             df_acct_loc.distance_min AS distance_min_loan, df_acct_loc.FM_damage AS FM_damage_loan \
                           FROM df_loan_origin \
                           INNER JOIN df_acct_loc \
                             ON df_acct_loc.TU_Trade_ID = df_loan_origin.TU_Trade_ID \
                             AND df_acct_loc.Ref_date = df_loan_origin.min_loan_date ")

df_loan_origin.createOrReplaceTempView("df_loan_origin")

# Merge the locations of mortgage origination with account level records
df_acct = spark.sql("SELECT df_acct.*, \
                      df_loan_origin.fsa_loan AS fsa_heloc, df_loan_origin.Encrypted_LDU_loan AS Encrypted_LDU_heloc, \
                      df_loan_origin.prov_loan AS prov_heloc, \
                      df_loan_origin.distance_min_loan AS distance_min_heloc, df_loan_origin.FM_damage_loan AS FM_damage_heloc \
                    FROM df_acct \
                    LEFT JOIN df_loan_origin \
                      ON df_acct.TU_Trade_ID = df_loan_origin.TU_Trade_ID ")

df_acct.createOrReplaceTempView("df_acct")

#########################
# Save working data set #
#########################

print('Saving data to files...')
df_acct.printSchema()
df_acct.write.parquet(path="df_acct.parquet", mode="overwrite")

# Stop the sparkContext and cluster after processing
sc.stop()
