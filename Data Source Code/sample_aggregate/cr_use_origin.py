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

df_RESL = spark.sql("SELECT IFNULL(df_ML.TU_Consumer_ID,df_HELOC.TU_Consumer_ID) AS TU_Consumer_ID, \
                      IFNULL(df_ML.Ref_date,df_HELOC.Ref_date) AS Ref_date, \
                      IFNULL(df_ML.fsa_ml,df_HELOC.fsa_heloc) AS fsa, \
                      IFNULL(df_ML.Encrypted_LDU_ml,df_HELOC.Encrypted_LDU_heloc) AS Encrypted_LDU, \
                      IFNULL(df_ML.FM_damage_ml,df_HELOC.FM_damage_heloc) AS FM_damage, \
                      IFNULL(df_ML.prov_ml,df_HELOC.prov_heloc) AS prov, \
                      IFNULL(df_ML.distance_min_ml,df_HELOC.distance_min_heloc) AS distance_min, \
                      ml_bal, ml_bal_arr, ml_bal_def, ml_chargoff, ml_chargoff_new, \
                      ml_bal_ins, ml_bal_arr_ins, ml_bal_def_ins, ml_chargoff_ins, ml_chargoff_new_ins, \
                      heloc_bal, heloc_bal_arr, heloc_bal_def, heloc_chargoff, heloc_chargoff_new \
                    FROM df_ML \
                    FULL JOIN df_HELOC \
                      ON df_ML.TU_Consumer_ID = df_HELOC.TU_Consumer_ID \
                      AND df_ML.Ref_date = df_HELOC.Ref_date \
                      AND df_ML.fsa_ml = df_HELOC.fsa_heloc \
                      AND df_ML.Encrypted_LDU_ml = df_HELOC.Encrypted_LDU_heloc ").fillna(value=0)

df_RESL.createOrReplaceTempView("df_RESL")

##############################################################
# Identify treatment and control within the event time frame #
##############################################################

# Define control as dissemination area at least 100km away from Fort McMurray
df_crc = spark.sql("SELECT *, \
                     CASE WHEN fsa RLIKE '^(T9H|T9J|T9K)$' THEN 'T9(H|J|K)' ELSE fsa END AS fsa_grp, \
                     CASE WHEN fsa RLIKE '^(T9H|T9J|T9K)$' THEN 1 ELSE 0 END AS treated, \
                     CASE WHEN prov RLIKE '(AB|SK)' and distance_min >= 100.0 THEN 1 ELSE 0 END AS control \
                   FROM df_crc ") \
         .where("Run_Date BETWEEN DATE('2014-02-01') AND DATE('2018-01-01')") \
         .where("(treated = 1 OR control = 1)") \
         .drop("fsa").withColumnRenamed("fsa_grp", "fsa")
           
df_crc.createOrReplaceTempView("df_crc")

# Define control as dissemination area at least 100km away from Fort McMurray
df_RESL = spark.sql("SELECT *, \
                      CASE WHEN fsa RLIKE '^(T9H|T9J|T9K)$' THEN 'T9(H|J|K)' ELSE fsa END AS fsa_grp, \
                      CASE WHEN fsa RLIKE '^(T9H|T9J|T9K)$' THEN 1 ELSE 0 END AS treated, \
                      CASE WHEN prov RLIKE '(AB|SK)' and distance_min >= 100.0 THEN 1 ELSE 0 END AS control \
                    FROM df_RESL ") \
          .where("Ref_date BETWEEN DATE('2014-02-01') AND DATE('2018-01-01')") \
          .where("(treated = 1 OR control = 1)") \
          .drop("fsa").withColumnRenamed("fsa_grp", "fsa")

df_RESL.createOrReplaceTempView("df_RESL")

################################################################
# Create regional RESL data set by the location of origination #
################################################################

# Merge RESL data with CRC
df_RESL = spark.sql("SELECT df_RESL.*, df_crc.cvsc100 \
                    FROM df_RESL \
                    LEFT JOIN df_crc \
                    ON df_RESL.TU_Consumer_ID = df_crc.tu_consumer_id \
                      AND df_RESL.Ref_date = df_crc.Run_Date ")

df_RESL.createOrReplaceTempView("df_RESL")

# Merge RESL data with credit card data
df_RESL = spark.sql("SELECT df_RESL.*, \
                      IFNULL(df_BC.bc_bal, 0) AS bc_bal, IFNULL(df_BC.bc_bal/df_BC.bc_lmt, 0) AS bc_use \
                    FROM df_RESL \
                    LEFT JOIN df_BC \
                      ON df_RESL.TU_Consumer_ID = df_BC.TU_Consumer_ID \
                      AND df_RESL.Ref_date = df_BC.Ref_date ")

df_RESL.createOrReplaceTempView("df_RESL")

# Aggregate individual-level RESL data into regional data set
df_synth = spark.sql("SELECT Ref_date, fsa, treated, FM_damage, \
                       SUM(ml_bal)/1000000 AS ml_bal_tot, \
                       SUM(ml_bal_arr + ml_bal_def)/1000000 AS ml_bal_arr_tot, \
                       SUM(ml_chargoff_new)/1000000 AS ml_chargoff_new_tot, \
                       SUM(ml_bal_ins)/1000000 AS ml_bal_ins_tot, \
                       SUM(ml_bal_arr_ins + ml_bal_def_ins)/1000000 AS ml_bal_arr_ins_tot, \
                       SUM(ml_chargoff_new_ins)/1000000 AS ml_chargoff_new_ins_tot, \
                       SUM(heloc_bal)/1000000 AS heloc_bal_tot, \
                       SUM(heloc_bal_arr + heloc_bal_def)/1000000 AS heloc_bal_arr_tot, \
                       SUM(heloc_chargoff_new)/1000000 AS heloc_chargoff_new_tot \
                     FROM df_RESL \
                     GROUP BY Ref_date, fsa, treated, FM_damage ")

df_synth.createOrReplaceTempView("df_synth")

# Aggregate ONLY for insured mortgage holders
df_ML_ins = spark.sql("SELECT Ref_date, fsa, treated, FM_damage, \
                        COUNT(DISTINCT tu_consumer_id) AS N_ml_ins_holders, \
                        AVG(ml_bal_ins) AS ml_bal_ins, \
                        AVG(bc_bal) AS bc_bal, \
                        AVG(bc_use) AS bc_use, \
                        AVG(cvsc100) AS crsc, \
                        SUM(IF(bc_use >= 0.6 AND bc_use < 0.8, 1, 0)) AS N_bc_use_60_80, \
                        SUM(IF(bc_use >= 0.8, 1, 0)) AS N_bc_use_80_plus, \
                        SUM(IF(cvsc100 >= 640 AND cvsc100 < 720, 1, 0)) AS N_nearprime, \
                        SUM(IF(cvsc100 < 640, 1, 0)) AS N_subprime \
                      FROM df_RESL \
                      WHERE ml_bal_ins > 0 \
                      GROUP BY Ref_date, fsa, treated, FM_damage ")

df_ML_ins.createOrReplaceTempView("df_ML_ins")

df_synth = spark.sql("SELECT df_synth.*, \
                       df_ML_ins.N_ml_ins_holders, df_ML_ins.ml_bal_ins, \
                       df_ML_ins.bc_bal, df_ML_ins.bc_use, \
                       df_ML_ins.N_bc_use_60_80, df_ML_ins.N_bc_use_80_plus, \
                       df_ML_ins.N_nearprime, df_ML_ins.N_subprime \
                     FROM df_synth \
                     LEFT JOIN df_ML_ins \
                       ON df_synth.Ref_date = df_ML_ins.Ref_date \
                       AND df_synth.fsa = df_ML_ins.fsa \
                       AND df_synth.treated = df_ML_ins.treated \
                       AND df_synth.FM_damage = df_ML_ins.FM_damage ")

df_synth.createOrReplaceTempView("df_synth")

###########################################
# Create individual-level credit data set #
###########################################

# Merge CRC with RESL data
df_RESL_ind = spark.sql("SELECT TU_Consumer_ID, Ref_date, \
                          SUM(ml_bal) AS ml_bal, SUM(heloc_bal) AS heloc_bal \
                        FROM df_RESL \
                        GROUP BY TU_Consumer_ID, Ref_date ")

df_RESL_ind.createOrReplaceTempView("df_RESL_ind")

df_ind = spark.sql("SELECT df_crc.*, \
                     IFNULL(df_RESL_ind.ml_bal,0) AS ml_bal, IFNULL(df_RESL_ind.heloc_bal,0) AS heloc_bal \
                   FROM df_crc \
                   LEFT JOIN df_RESL_ind \
                     ON df_crc.tu_consumer_id = df_RESL_ind.TU_Consumer_ID \
                     AND df_crc.Run_Date = df_RESL_ind.Ref_date ")

df_ind.createOrReplaceTempView("df_ind")

# Merge CRC with credit card data
df_ind = spark.sql("SELECT df_ind.*, IFNULL(df_BC.bc_lmt,0) AS bc_lmt \
                   FROM df_ind \
                   LEFT JOIN df_BC \
                     ON df_ind.tu_consumer_id = df_BC.TU_Consumer_ID \
                     AND df_ind.Run_Date = df_BC.Ref_date ")

df_ind.createOrReplaceTempView("df_ind")

# Merge CRC with data on other loans
df_ind = spark.sql("SELECT df_ind.*, IFNULL(df_other.other_bal,0) AS other_bal \
                   FROM df_ind \
                   LEFT JOIN df_other \
                     ON df_ind.tu_consumer_id = df_other.TU_Consumer_ID \
                     AND df_ind.Run_Date = df_other.Ref_date ")

df_ind.createOrReplaceTempView("df_ind")

##########################################################################
# Drop rural FSAs and FSAs with less than 5000 credit-active individuals #
##########################################################################

# Count credit active consumers by FSA
df_active = spark.sql("SELECT Run_Date, fsa, treated, FM_damage, \
                        COUNT(DISTINCT TU_Consumer_ID) AS N_active \
                      FROM df_ind \
                      WHERE (ml_bal + heloc_bal + other_bal + bc_lmt) > 0 \
                      GROUP BY Run_Date, fsa, treated, FM_damage ")

df_active.createOrReplaceTempView("df_active")

# Join to regional data set
df_synth = spark.sql("SELECT df_synth.*, df_active.N_active \
                     FROM df_synth \
                     INNER JOIN df_active \
                       ON df_synth.fsa = df_active.fsa \
                       AND df_synth.treated = df_active.treated \
                       AND df_synth.FM_damage = df_active.FM_damage \
                       AND df_synth.Ref_date = df_active.Run_Date ")

df_synth.createOrReplaceTempView("df_synth")

# Filter by selection criteria
df_keep = spark.sql("SELECT fsa, treated, FM_damage \
                    FROM df_synth \
                    WHERE SUBSTRING(fsa, 2, 1) NOT LIKE '0' \
                    GROUP BY fsa, treated, FM_damage \
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

df_fact = spark.sql("SELECT treated, FM_damage, \
                      COUNT(DISTINCT fsa) AS N_fsa, \
                      AVG(N_active) AS N_active_avg, STD(N_active) AS N_active_sd, \
                      AVG(ml_bal_ins) AS ml_bal_ins_avg, STD(ml_bal_ins) AS ml_bal_ins_sd, \
                      AVG(ml_bal_ins_tot/ml_bal_tot) AS ml_ins_rt_avg, STD(ml_bal_ins_tot/ml_bal_tot)AS ml_ins_rt_sd, \
                      AVG(bc_use) AS bc_use_avg, STD(bc_use) AS bc_use_sd, \
                      AVG(N_nearprime/N_ml_ins_holders) AS nearprime_rt_avg, STD(N_nearprime/N_ml_ins_holders) AS nearprime_rt_sd, \
                      AVG(N_subprime/N_ml_ins_holders) AS subprime_rt_avg, STD(N_subprime/N_ml_ins_holders) AS subprime_rt_sd \
                    FROM df_synth \
                    GROUP BY treated, FM_damage ")

df_fact.createOrReplaceTempView("df_fact")

print('Saving data to files...')
df_fact.printSchema()
df_fact.write.csv(path="df_fact", mode="overwrite", sep=",", header="true")

# Stop the sparkContext and cluster after processing
sc.stop()
