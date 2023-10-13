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

####################
# Load sample data #
####################

# Load consumer-level CRC data
df_crc = sqlContext.read.parquet("../sample_crc/df_crc.parquet")
df_crc.createOrReplaceTempView("df_crc")
df_crc.printSchema()

# Load account-level mortgage data
df_ML = sqlContext.read.parquet("../sample_trade/ML/df_ind.parquet")
df_ML.createOrReplaceTempView("df_ML")
df_ML.printSchema()

# Load account-level HELOC data
df_HELOC = sqlContext.read.parquet("../sample_trade/HELOC/df_ind.parquet")
df_HELOC.createOrReplaceTempView("df_HELOC")
df_HELOC.printSchema()

# Load account-level credit card data
df_BC = sqlContext.read.parquet("../sample_trade/BC/df_ind.parquet")
df_BC.createOrReplaceTempView("df_BC")
df_BC.printSchema()

# Load account-level credit card data
df_other = sqlContext.read.parquet("../sample_trade/Other/df_ind.parquet")
df_other.createOrReplaceTempView("df_other")
df_other.printSchema()

##########################################################
# Create individual-level data set for residential loans #
##########################################################

# Aggregate to individual-level mortgage data
df_ML = spark.sql("SELECT TU_Consumer_ID, Ref_date, \
                    SUM(ml_bal) AS ml_bal, \
                    SUM(ml_bal_arr) AS ml_bal_arr, \
                    SUM(ml_bal_def) AS ml_bal_def, \
                    SUM(ml_chargoff_new) AS ml_chargoff_new, \
                    SUM(ml_bal_ins) AS ml_bal_ins, \
                    SUM(ml_bal_arr_ins) AS ml_bal_arr_ins, \
                    SUM(ml_bal_def_ins) AS ml_bal_def_ins, \
                    SUM(ml_chargoff_new_ins) AS ml_chargoff_new_ins \
                  FROM df_ML \
                  GROUP BY TU_Consumer_ID, Ref_date ")

df_ML.createOrReplaceTempView("df_ML")

# Aggregate to individual-level HELOC data
df_HELOC = spark.sql("SELECT TU_Consumer_ID, Ref_date, \
                       SUM(heloc_bal) AS heloc_bal, \
                       SUM(heloc_bal_arr) AS heloc_bal_arr, \
                       SUM(heloc_bal_def) AS heloc_bal_def, \
                       SUM(heloc_chargoff_new) AS heloc_chargoff_new \
                     FROM df_HELOC \
                     GROUP BY TU_Consumer_ID, Ref_date ")

df_HELOC.createOrReplaceTempView("df_HELOC")

###########################################
# Create individual-level credit data set #
###########################################

# Merge with mortgage data
df_ind = spark.sql("SELECT df_crc.*, \
                     df_ML.ml_bal, df_ML.ml_bal_arr, df_ML.ml_bal_def, df_ML.ml_chargoff_new, \
                     df_ML.ml_bal_ins, df_ML.ml_bal_arr_ins, df_ML.ml_bal_def_ins, df_ML.ml_chargoff_new_ins, \
                     CASE WHEN df_ML.TU_Consumer_ID IS NOT NULL THEN 1 ELSE 0 END AS ml_exist \
                   FROM df_crc \
                   LEFT JOIN df_ML \
                     ON df_crc.tu_consumer_id = df_ML.TU_Consumer_ID \
                     AND df_crc.Run_Date = df_ML.Ref_date ").fillna(value=0)

df_ind.createOrReplaceTempView("df_ind")

# Merge with HELOC data
df_ind = spark.sql("SELECT df_ind.*, \
                     df_HELOC.heloc_bal, df_HELOC.heloc_bal_arr, df_HELOC.heloc_bal_def, df_HELOC.heloc_chargoff_new, \
                     CASE WHEN df_HELOC.TU_Consumer_ID IS NOT NULL THEN 1 ELSE 0 END AS heloc_exist \
                   FROM df_ind \
                   LEFT JOIN df_HELOC \
                     ON df_ind.tu_consumer_id = df_HELOC.TU_Consumer_ID \
                     AND df_ind.Run_Date = df_HELOC.Ref_date ").fillna(value=0)

df_ind.createOrReplaceTempView("df_ind")

# Merge with credit card data
df_ind = spark.sql("SELECT df_ind.*, \
                     df_BC.bc_bal, df_BC.bc_bal_arr, df_BC.bc_bal_def, df_BC.bc_chargoff_new, df_BC.bc_lmt, \
                     CASE WHEN df_BC.TU_Consumer_ID IS NOT NULL THEN 1 ELSE 0 END AS bc_exist \
                   FROM df_ind \
                   LEFT JOIN df_BC \
                     ON df_ind.tu_consumer_id = df_BC.TU_Consumer_ID \
                     AND df_ind.Run_Date = df_BC.Ref_date ").fillna(value=0)

df_ind.createOrReplaceTempView("df_ind")

# Merge with data on other loans
df_ind = spark.sql("SELECT df_ind.*, \
                     df_other.other_bal, df_other.other_bal_arr, df_other.other_bal_def, df_other.other_chargoff_new, \
                     CASE WHEN df_other.TU_Consumer_ID IS NOT NULL THEN 1 ELSE 0 END AS other_exist \
                   FROM df_ind \
                   LEFT JOIN df_other \
                     ON df_ind.tu_consumer_id = df_other.TU_Consumer_ID \
                     AND df_ind.Run_Date = df_other.Ref_date ").fillna(value=0)

df_ind.createOrReplaceTempView("df_ind")

##################################
# Identify treatment and control #
##################################

# Define control as dissemination area at least 100km away from Fort McMurray
# Keep treated and control groups only
df_ind = spark.sql("SELECT *, \
                     CASE WHEN fsa RLIKE '^(T9H|T9J|T9K)$' THEN 1 ELSE 0 END AS treated, \
                     CASE WHEN prov RLIKE '(AB|SK)' and distance_min >= 100.0 THEN 1 ELSE 0 END AS control \
                   FROM df_ind ") \
         .where("(treated = 1 OR control = 1)") \

df_ind.createOrReplaceTempView("df_ind")

# Save working data set
print('Saving data to files...')
df_ind.printSchema()
df_ind.write.parquet(path="df_ind.parquet", mode="overwrite")

df_ind = sqlContext.read.parquet("df_ind.parquet")
df_ind.createOrReplaceTempView("df_ind")

#############################
# Filter data for relevance #
#############################

# Keep credit active consumers only
# Group all treated FSAs into one single area
df_ind = spark.sql("SELECT *, \
                     CASE WHEN treated = 1 THEN 'T9(H|J|K)' ELSE fsa END AS fsa_grp \
                   FROM df_ind \
                   WHERE (ml_bal + heloc_bal + other_bal + bc_lmt) > 0 ") \
         .drop("fsa").withColumnRenamed("fsa_grp", "fsa")

df_ind.createOrReplaceTempView("df_ind")

#####################
# Regional data set #
#####################

# Aggregation for insured mortgage holders
df_synth = spark.sql("SELECT Run_Date, treated, FM_damage, fsa, \
                       COUNT(DISTINCT tu_consumer_id) AS N_ml_ins, \
                       AVG(ml_bal_ins) AS ml_bal_ins, \
                       AVG(heloc_bal) AS heloc_bal, \
                       AVG(ml_bal + heloc_bal) AS res_bal, \
                       AVG(other_bal) AS cl_bal, \
                       AVG(bc_bal) AS bc_bal, \
                       AVG(bc_bal_arr + bc_bal_def) AS bc_bal_arr, \
                       AVG(IFNULL(bc_bal/bc_lmt, 0)) AS bc_use, \
                       SUM(IF(bc_bal/bc_lmt >= 0.6 AND bc_bal/bc_lmt < 0.8, 1, 0)) AS N_bc_use_60_80, \
                       SUM(IF(bc_bal/bc_lmt >= 0.8, 1, 0)) AS N_bc_use_80_plus, \
                       AVG(cvsc100) AS cr_score, \
                       SUM(IF(cvsc100 >= 640 AND cvsc100 < 720, 1, 0)) AS N_nearprime, \
                       SUM(IF(cvsc100 < 640, 1, 0)) AS N_subprime \
                     FROM df_ind \
                     WHERE (ml_exist = 1 AND ml_bal_ins > 0) \
                     GROUP BY Run_Date, treated, FM_damage, fsa ")

df_synth.createOrReplaceTempView("df_synth")

################################################
# Join data set with other aggregate variables #
################################################

df_fsa_lv = spark.sql("SELECT Run_Date, treated, FM_damage, fsa, \
                        COUNT(DISTINCT tu_consumer_id) AS N_active, \
                        AVG(cvsc100) AS cr_score_fsa, \
                        SUM(IF(cvsc100 >= 640 AND cvsc100 < 720, 1, 0)) AS N_nearprime_fsa, \
                        SUM(IF(cvsc100 < 640, 1, 0)) AS N_subprime_fsa, \
                        SUM(ml_bal)/1000000 AS ml_bal_tot, \
                        SUM(ml_bal_arr + ml_bal_def)/1000000 AS ml_bal_arr_tot, \
                        SUM(ml_chargoff_new)/1000000 AS ml_chargoff_new_tot, \
                        SUM(ml_bal_ins)/1000000 AS ml_bal_ins_tot, \
                        SUM(ml_bal_arr_ins + ml_bal_def_ins)/1000000 AS ml_bal_arr_ins_tot, \
                        SUM(ml_chargoff_new_ins)/1000000 AS ml_chargoff_new_ins_tot, \
                        SUM(heloc_bal)/1000000 AS heloc_bal_tot, \
                        SUM(heloc_bal_arr + heloc_bal_def)/1000000 AS heloc_bal_arr_tot, \
                        SUM(heloc_chargoff_new)/1000000 AS heloc_chargoff_new_tot, \
                        AVG(bc_bal) AS bc_bal_fsa, \
                        AVG(IFNULL(bc_bal/bc_lmt, 0)) AS bc_use_fsa, \
                        SUM(IF(bc_bal/bc_lmt >= 0.6 AND bc_bal/bc_lmt < 0.8, 1, 0)) AS N_bc_use_60_80_fsa, \
                        SUM(IF(bc_bal/bc_lmt >= 0.8, 1, 0)) AS N_bc_use_80_plus_fsa, \
                        SUM(bc_bal)/1000000 AS bc_bal_tot, \
                        SUM(bc_bal_arr + bc_bal_def)/1000000 AS bc_bal_arr_tot, \
                        SUM(bc_chargoff_new)/1000000 AS bc_chargoff_new_tot, \
                        SUM(other_bal)/1000000 AS other_bal_tot, \
                        SUM(other_bal_arr + other_bal_def)/1000000 AS other_bal_arr_tot, \
                        SUM(other_chargoff_new)/1000000 AS other_chargoff_new_tot \
                      FROM df_ind \
                      GROUP BY Run_Date, treated, FM_damage, fsa ")

df_fsa_lv.createOrReplaceTempView("df_fsa_lv")

df_synth = spark.sql("SELECT df_synth.*, N_active, cr_score_fsa, N_nearprime_fsa, N_subprime_fsa, \
                       df_fsa_lv.ml_bal_tot, df_fsa_lv.ml_bal_arr_tot, df_fsa_lv.ml_chargoff_new_tot, \
                       df_fsa_lv.ml_bal_ins_tot, df_fsa_lv.ml_bal_arr_ins_tot, df_fsa_lv.ml_chargoff_new_ins_tot, \
                       df_fsa_lv.heloc_bal_tot, df_fsa_lv.heloc_bal_arr_tot, df_fsa_lv.heloc_chargoff_new_tot, \
                       bc_bal_fsa, bc_use_fsa, N_bc_use_60_80_fsa, N_bc_use_80_plus_fsa, \
                       df_fsa_lv.bc_bal_tot, df_fsa_lv.bc_bal_arr_tot, df_fsa_lv.bc_chargoff_new_tot, \
                       df_fsa_lv.other_bal_tot, df_fsa_lv.other_bal_arr_tot, df_fsa_lv.other_chargoff_new_tot \
                     FROM df_synth \
                     LEFT JOIN df_fsa_lv \
                       ON df_synth.Run_Date = df_fsa_lv.Run_Date \
                       AND df_synth.treated = df_fsa_lv.treated \
                       AND df_synth.FM_damage = df_fsa_lv.FM_damage \
                       AND df_synth.fsa = df_fsa_lv.fsa ")

df_synth.createOrReplaceTempView("df_synth")

##########################################################################
# Drop rural FSAs and FSAs with less than 5000 credit-active individuals #
##########################################################################

# Filter by selection criteria
df_keep = spark.sql("SELECT DISTINCT treated, FM_damage, fsa \
                    FROM df_synth \
                    WHERE SUBSTRING(fsa, 2, 1) NOT LIKE '0' \
                    GROUP BY treated, FM_damage, fsa \
                    HAVING MIN(N_active) >= 5000 OR MIN(treated) = 1 ")

df_keep.createOrReplaceTempView("df_keep")

# Apply filter
df_synth = spark.sql("SELECT df_synth.* \
                     FROM df_synth \
                     INNER JOIN df_keep \
                       ON df_synth.treated = df_keep.treated \
                       AND df_synth.FM_damage = df_keep.FM_damage \
                       AND df_synth.fsa = df_keep.fsa ")

df_synth.createOrReplaceTempView("df_synth")

print('Saving data to files...')
df_synth.printSchema()
df_synth.write.csv(path="df_synth", mode="overwrite", sep=",", header="true")

##########################
# Descriptive statistics #
##########################

# Only for data within the event timeframe
df_fact = spark.sql("SELECT treated, FM_damage, \
                      COUNT(DISTINCT fsa) AS N_fsa, \
                      AVG(N_active) AS N_active_avg, STD(N_active) AS N_active_sd, \
                      AVG(ml_bal_ins) AS ml_bal_ins_avg, STD(ml_bal_ins) AS ml_bal_ins_sd, \
                      AVG(ml_bal_ins_tot/ml_bal_tot) AS ml_ins_rt_avg, STD(ml_bal_ins_tot/ml_bal_tot)AS ml_ins_rt_sd, \
                      AVG(bc_use) AS bc_use_avg, STD(bc_use) AS bc_use_sd, \
                      AVG(N_nearprime/N_ml_ins) AS nearprime_rt_avg, STD(N_nearprime/N_ml_ins) AS nearprime_rt_sd, \
                      AVG(N_subprime/N_ml_ins) AS subprime_rt_avg, STD(N_subprime/N_ml_ins) AS subprime_rt_sd \
                    FROM df_synth \
                    WHERE Run_Date BETWEEN DATE('2014-01-01') AND DATE('2018-01-01') \
                    GROUP BY treated, FM_damage ")

df_fact.createOrReplaceTempView("df_fact")

print('Saving data to files...')
df_fact.printSchema()
df_fact.write.csv(path="df_fact", mode="overwrite", sep=",", header="true")

# Stop the sparkContext and cluster after processing
sc.stop()
