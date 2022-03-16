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

###########################
# Load files for CRC data #
###########################

# Load files for CRC data
df_crc = sqlContext.read.parquet("/appdata/TU/TU_Official/CRCS_LDU/crcs_ldu_20{0,1}[0-9]*_parq") \
  .select("tu_consumer_id", "Run_Date", "fsa", col("Encrypted_LDU").alias("LDU"), col("MC33").alias("ML_bal_crc")) \
  .withColumn("Run_Date", to_date(concat(col("Run_Date").substr(1,4),lit("-"), col("Run_Date").substr(5,2),lit("-01"))))

df_crc.createOrReplaceTempView("df_crc")

# Load files for sample CRC data
df_sample = sqlContext.read.parquet("df_crc.parquet")
df_sample.createOrReplaceTempView("df_sample")

###################################################
# Only keep the CRC history for sampled consumers #
###################################################

df_cid = spark.sql("SELECT DISTINCT tu_consumer_id FROM df_sample")
df_cid.createOrReplaceTempView("df_cid")

df_crc = spark.sql("SELECT df_crc.* \
                   FROM df_crc \
                   INNER JOIN df_cid \
                     ON df_crc.tu_consumer_id = df_cid.tu_consumer_id")

df_crc.createOrReplaceTempView("df_crc")

#######################
# Identify homeowners #
#######################

# Define the scope of the dataset - only keep data since 2011
df_owner = df_crc.where("YEAR(Run_Date) >= 2011")
df_owner.createOrReplaceTempView("df_owner")

# Obtain ALL mortgage accounts from trade-level data
df_mrtg = sqlContext.read.parquet("/appdata/TU/TU_Official/Account/account_*_parq") \
  .where("PRODUCT_TYPE LIKE 'ML'").where("MOP NOT RLIKE '[8-9]'") \
  .select("TU_Consumer_ID", "TU_Trade_ID", "CLOSED_INDIC", "CURRENT_BALANCE", \
          to_date("Run_date").alias("Run_date"), \
          to_date(concat(col("OPENED_DT").substr(1,4),lit("-"),col("OPENED_DT").substr(5,2),lit("-01"))).alias("OPENED_date"), \
          to_date(concat(col("CLOSED_DT").substr(1,4),lit("-"),col("CLOSED_DT").substr(5,2),lit("-01"))).alias("CLOSED_date"), \
          to_date(concat(col("LAST_PAYMT_DT").substr(1,4),lit("-"),col("LAST_PAYMT_DT").substr(5,2),lit("-01"))).alias("LAST_PAYMT_date"), \
          to_date(concat(col("FILE_SINCE_DT").substr(1,4),lit("-"),col("FILE_SINCE_DT").substr(5,2),lit("-"),col("FILE_SINCE_DT").substr(7,2))).alias("FILE_SINCE_date"), \
          to_date(concat(col("LAST_UPDATED_DT").substr(1,4),lit("-"),col("LAST_UPDATED_DT").substr(5,2),lit("-"),col("LAST_UPDATED_DT").substr(7,2))).alias("LAST_UPDATED_date"))

df_mrtg.createOrReplaceTempView("df_mrtg")

# Count the number of mortgages held by a consumer
df_n_mrtg = spark.sql("SELECT TU_Consumer_ID, Run_date, \
                        COUNT(TU_Trade_ID) AS N_all_mrtg \
                      FROM df_mrtg \
                      GROUP BY TU_Consumer_ID, Run_date ")

df_n_mrtg.createOrReplaceTempView("df_n_mrtg")

# Replace missing opened/closed date by last payment date for closed accounts
df_mrtg = spark.sql("SELECT df_mrtg.*, \
                      CASE WHEN CURRENT_BALANCE>0 THEN 1 ELSE 0 END AS active_mrtg, \
                      CASE WHEN OPENED_date IS NULL THEN add_months(LAST_PAYMT_date,-18) ELSE OPENED_date END AS OPENED_date_refine, \
                      CASE WHEN CLOSED_INDIC LIKE 'Y' OR CURRENT_BALANCE=0 THEN IFNULL(CLOSED_date,LAST_PAYMT_date) ELSE CLOSED_date END AS CLOSED_date_refine, \
                      CASE WHEN FILE_SINCE_date IS NOT NULL THEN FILE_SINCE_date ELSE LAST_UPDATED_date END AS FILE_SINCE_date_refine \
                    FROM df_mrtg ") \
          .drop("Run_date","LAST_PAYMT_date","CLOSED_INDIC","CURRENT_BALANCE","OPENED_date","CLOSED_date","FILE_SINCE_date","LAST_UPDATED_date") \
          .withColumnRenamed("OPENED_date_refine","OPENED_date").withColumnRenamed("CLOSED_date_refine","CLOSED_date").withColumnRenamed("FILE_SINCE_date_refine","FILE_SINCE_date")

df_mrtg.createOrReplaceTempView("df_mrtg")

# Convert to opened quarter for historical data 2003-2008
df_mrtg = spark.sql("SELECT DISTINCT df_mrtg.*, \
                      CASE WHEN YEAR(OPENED_date)>=2009 \
                        THEN YEAR(add_months(OPENED_date,1))*100 +MONTH(add_months(OPENED_date,1)) \
                        ELSE \
                          CASE \
                            WHEN MONTH(OPENED_date) BETWEEN 1 AND 3 THEN YEAR(OPENED_date)*100 +4 \
                            WHEN MONTH(OPENED_date) BETWEEN 4 AND 6 THEN YEAR(OPENED_date)*100 +7 \
                            WHEN MONTH(OPENED_date) BETWEEN 7 AND 9 THEN YEAR(OPENED_date)*100 +10 \
                            WHEN MONTH(OPENED_date) BETWEEN 10 AND 12 THEN (YEAR(OPENED_date)+1)*100 +1 \
                            ELSE NULL END \
                      END AS opened_ym, \
                      CASE WHEN YEAR(CLOSED_date)>=2009 \
                        THEN YEAR(CLOSED_date)*100 +MONTH(CLOSED_date) \
                        ELSE \
                          CASE \
                            WHEN MONTH(CLOSED_date) BETWEEN 1 AND 3 THEN YEAR(CLOSED_date)*100 +1 \
                            WHEN MONTH(CLOSED_date) BETWEEN 4 AND 6 THEN YEAR(CLOSED_date)*100 +4 \
                            WHEN MONTH(CLOSED_date) BETWEEN 7 AND 9 THEN YEAR(CLOSED_date)*100 +7 \
                            WHEN MONTH(CLOSED_date) BETWEEN 10 AND 12 THEN YEAR(CLOSED_date)*100 +10 \
                            ELSE NULL END \
                      END AS closed_ym \
                    FROM df_mrtg ") \
          .withColumn("OPENED_qtr", to_date(concat(col("opened_ym").substr(1,4),lit("-"), col("opened_ym").substr(5,2),lit("-01")))) \
          .withColumn("CLOSED_qtr", to_date(concat(col("closed_ym").substr(1,4),lit("-"), col("closed_ym").substr(5,2),lit("-01")))) \
          .drop("OPENED_date","CLOSED_date","opened_ym","closed_ym")

df_mrtg.createOrReplaceTempView("df_mrtg")

# Merge with CRC to identify consumer location to infer home ownership
# Also backtrack ownership before ML balance data became available
# (1) by location of fully paid ML on opened date
# (2) by location of fully paid ML on closed date
# (3) by location of active ML

# (1) location of fully paid ML - backfill to location using CRC 12mos after mortgage opened date
df_opened_ml = spark.sql("SELECT DISTINCT df_mrtg.TU_Consumer_ID, \
                           df_crc.fsa AS mrtg_fsa, df_crc.LDU AS mrtg_LDU \
                         FROM df_mrtg \
                         LEFT JOIN df_crc \
                           ON df_mrtg.TU_Consumer_ID = df_crc.tu_consumer_id \
                           AND YEAR(add_months(df_mrtg.OPENED_qtr,12)) = YEAR(df_crc.Run_Date) \
                           AND MONTH(add_months(df_mrtg.OPENED_qtr,12)) = MONTH(df_crc.Run_Date) \
                         WHERE df_mrtg.active_mrtg = 0 ")
#                           MIN(df_mrtg.OPENED_qtr) AS first_opened_qtr \
#                         GROUP BY df_mrtg.TU_Consumer_ID, df_crc.fsa, df_crc.LDU

df_opened_ml.createOrReplaceTempView("df_opened_ml")

df_owner = spark.sql("SELECT df_owner.*, \
                       CASE WHEN (df_opened_ml.TU_Consumer_ID IS NOT NULL) THEN 1 ELSE 0 END AS fsa_opened \
                     FROM df_owner \
                     LEFT JOIN df_opened_ml \
                       ON df_opened_ml.TU_Consumer_ID = df_owner.tu_consumer_id \
                       AND df_opened_ml.mrtg_fsa = df_owner.fsa \
                       AND df_opened_ml.mrtg_LDU = df_owner.LDU ")
# CASE WHEN (df_opened_ml.TU_Consumer_ID IS NOT NULL) AND DATEDIFF(df_owner.Run_Date,df_opened_ml.first_opened_qtr)>=0 THEN 1 ELSE 0 END AS fsa_opened

df_owner.createOrReplaceTempView("df_owner")

# (2) location of fully paid ML - backfill to location using CRC 12mos before mortgage closed date
df_closed_ml = spark.sql("SELECT DISTINCT df_mrtg.TU_Consumer_ID, \
                           df_crc.fsa AS mrtg_fsa, df_crc.LDU AS mrtg_LDU \
                         FROM df_mrtg \
                         LEFT JOIN df_crc \
                           ON df_mrtg.TU_Consumer_ID = df_crc.tu_consumer_id \
                           AND YEAR(add_months(df_mrtg.CLOSED_qtr,-12)) = YEAR(df_crc.Run_Date) \
                           AND MONTH(add_months(df_mrtg.CLOSED_qtr,-12)) = MONTH(df_crc.Run_Date) \
                         WHERE df_mrtg.active_mrtg = 0 ")
#                           MAX(df_mrtg.CLOSED_qtr) AS last_closed_qtr
#                         GROUP BY df_mrtg.TU_Consumer_ID, df_crc.fsa, df_crc.LDU

df_closed_ml.createOrReplaceTempView("df_closed_ml")

df_owner = spark.sql("SELECT df_owner.*, \
                       CASE WHEN (df_closed_ml.TU_Consumer_ID IS NOT NULL) THEN 1 ELSE 0 END AS fsa_closed \
                     FROM df_owner \
                     LEFT JOIN df_closed_ml \
                       ON df_closed_ml.TU_Consumer_ID = df_owner.tu_consumer_id \
                       AND df_closed_ml.mrtg_fsa = df_owner.fsa \
                       AND df_closed_ml.mrtg_LDU = df_owner.LDU ")
# CASE WHEN (df_closed_ml.TU_Consumer_ID IS NOT NULL) AND DATEDIFF(df_owner.Run_Date,df_closed_ml.last_closed_qtr)>=0 THEN 1 ELSE 0 END AS fsa_closed

df_owner.createOrReplaceTempView("df_owner")

# (3) location of active ML - backfill to location using CRC 1mo after mortgage FILE_SINCE_date
df_active_ml = spark.sql("SELECT DISTINCT df_mrtg.TU_Consumer_ID, \
                           df_crc.fsa AS mrtg_fsa, df_crc.LDU AS mrtg_LDU \
                         FROM df_mrtg \
                         LEFT JOIN df_crc \
                           ON df_mrtg.TU_Consumer_ID = df_crc.tu_consumer_id \
                           AND YEAR(add_months(FILE_SINCE_date,1)) = YEAR(df_crc.Run_Date) \
                           AND MONTH(add_months(FILE_SINCE_date,1)) = MONTH(df_crc.Run_Date) \
                         WHERE df_mrtg.active_mrtg = 1 ")
#                           MIN(df_mrtg.FILE_SINCE_date) AS first_active_qtr
#                         GROUP BY df_mrtg.TU_Consumer_ID, df_crc.fsa, df_crc.LDU

df_active_ml.createOrReplaceTempView("df_active_ml")

df_owner = spark.sql("SELECT df_owner.*, \
                       CASE WHEN (df_active_ml.TU_Consumer_ID IS NOT NULL) THEN 1 ELSE 0 END AS fsa_active \
                     FROM df_owner \
                     LEFT JOIN df_active_ml \
                       ON df_active_ml.TU_Consumer_ID = df_owner.tu_consumer_id \
                       AND df_active_ml.mrtg_fsa = df_owner.fsa \
                       AND df_active_ml.mrtg_LDU = df_owner.LDU ")
# CASE WHEN (df_active_ml.TU_Consumer_ID IS NOT NULL) AND DATEDIFF(df_owner.Run_Date,df_active_ml.first_active_qtr)>=0 THEN 1 ELSE 0 END AS fsa_active

df_owner.createOrReplaceTempView("df_owner")

# Identify as homeowner if
# (1) consumer currently associated with an active mortgage, with backfilling for missing data
# (2) backward adj: consumer without an active mortgage (or missing data), but a mortgage was originated at current address
# (3) forward adj: consumer without an active mortgage, but a mortgage was paid/closed at current address

# Definition (1) includes homeowners who moved since mortgage origination, meaning that they may NOT be living on propoerties that they owned
# E.g. consumer purchased a property and rent somewhere else
# Backfilling active mortgage just fill the missing observations for the above in more recent data
# Definition (2) will still misidentify homeowners due to moving (downward biased), but hopefully (3) can close some of the loopholes
# To be added: consumers without an opened mortgage, and with mortgages closed before location data becomes available (and never moved since then?)

df_owner = spark.sql("SELECT tu_consumer_id, Run_Date, fsa, LDU, \
                       CASE \
                         WHEN (ML_bal_crc>0) THEN 1 \
                         WHEN (ML_bal_crc=0) AND GREATEST(fsa_opened, fsa_closed, fsa_active)>0 THEN 1 \
                         ELSE 0 END AS homeowner_mrtg \
                     FROM df_owner ")

df_owner.createOrReplaceTempView("df_owner")

# Avoid confusion: for homeowners without mortgage data, e.g. before FI was onboarded, nullify the mortgage balance
df_owner = spark.sql("SELECT df_owner.*, df_n_mrtg.N_all_mrtg, \
                       CASE \
                         WHEN df_owner.homeowner_mrtg=1 AND df_n_mrtg.N_all_mrtg IS NULL \
                         THEN 'Y' ELSE 'N' END AS mrtg_null_refine \
                     FROM df_owner \
                     LEFT JOIN df_n_mrtg \
                       ON df_owner.tu_consumer_id = df_n_mrtg.TU_Consumer_ID \
                       AND df_owner.Run_Date = df_n_mrtg.Run_date ")

df_owner.createOrReplaceTempView("df_owner")

#######################################
# Duration of stay at current address #
#######################################

# Duration of ownership can be tricky as accounts may not be updated timely
# Only keep data since 2009 
df_loc_dur = spark.sql("SELECT df_now.tu_consumer_id, df_now.fsa, df_now.LDU, df_now.Run_Date, \
                         SUM( CASE WHEN YEAR(df_past.Run_date)>=2009 AND months_between(df_now.Run_date,df_past.Run_date)>=1 THEN 1 \
                                   WHEN YEAR(df_past.Run_date)<2009 THEN 3 ELSE 0 END ) AS stay_dur, \
                         CASE WHEN MIN(df_past.Run_date) = DATE('2003-01-01') THEN 1 ELSE 0 END AS stay_censored \
                       FROM df_crc AS df_now \
                       LEFT JOIN df_crc AS df_past \
                         ON df_now.tu_consumer_id = df_past.tu_consumer_id \
                         AND df_now.fsa = df_past.fsa \
                         AND df_now.LDU = df_past.LDU \
                         AND df_now.Run_Date > df_past.Run_Date \
                       WHERE YEAR(df_now.Run_Date) >= 2009 \
                       GROUP BY df_now.tu_consumer_id, df_now.fsa, df_now.LDU, df_now.Run_Date ")

df_loc_dur.createOrReplaceTempView("df_loc_dur")

#################################
# Join derived tenure data sets #
#################################

# Join derived data sets - homeowner indicator and tenure duration
df_tenure = spark.sql("SELECT df_owner.tu_consumer_id, df_owner.Run_Date, \
                        df_owner.fsa, df_owner.LDU, df_owner.homeowner_mrtg, \
                        df_loc_dur.stay_dur, df_loc_dur.stay_censored \
                      FROM df_owner \
                      LEFT JOIN df_loc_dur \
                        ON df_owner.tu_consumer_id = df_loc_dur.tu_consumer_id \
                        AND df_owner.Run_Date = df_loc_dur.Run_Date ")

df_tenure.createOrReplaceTempView("df_tenure")

#########################
# Save working data set #
#########################

print('Saving data to files...')
df_tenure.printSchema()
df_tenure.write.parquet(path="df_tenure.parquet", mode="overwrite")

# Stop the sparkContext and cluster after processing
sc.stop()
