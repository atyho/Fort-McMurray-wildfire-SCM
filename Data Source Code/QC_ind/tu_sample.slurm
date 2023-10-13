#!/bin/bash

#SBATCH --job-name="pyspark"
#SBATCH -o pyspark.out
#SBATCH -e pyspark.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=36
#SBATCH --time=3:00:00
#SBATCH --mem=110G

module load sbt
module load anaconda2/4.3.1
module load java/1.8.0_141
module load spark/2.3.0
module list

ulimit -s unlimited
ulimit -u 81920

# AUTOMATIC CONFIGURATION
smart_launch.sh -m $SLURM_MEM_PER_NODE -u $SLURM_CPUS_ON_NODE -c sortByKeyDF -n $SLURM_JOB_NUM_NODES -e 4 -p tu_sample.py

rm tu_acct_sample.csv
awk 'FNR==1&&NR!=1{next;}{print}' tu_acct_sample_folder/*.csv >> tu_acct_sample.csv
rm -r tu_acct_sample_folder
