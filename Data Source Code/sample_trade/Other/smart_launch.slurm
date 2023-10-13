#!/bin/bash

#SBATCH --job-name="pyspark"
#SBATCH -o cr_use.out
#SBATCH -e cr_use.err
#SBATCH --nodes=1
#SBATCH --cpus-per-task=36
#SBATCH --time=23:59:59
#SBATCH --mem=995G
#SBATCH -p normal

module load sbt/0.13.15
module load anaconda2/4.3.1
module load java/1.8.0_141
module load spark/2.3.0
module list

ulimit -s unlimited
ulimit -u 81920

# AUTOMATIC CONFIGURATION

#smart_launch.sh -m $SLURM_MEM_PER_NODE -u $SLURM_CPUS_ON_NODE -c sortByKeyDF -n $SLURM_JOB_NUM_NODES -e 4 -p cr_use_other_1114.py
#smart_launch.sh -m $SLURM_MEM_PER_NODE -u $SLURM_CPUS_ON_NODE -c sortByKeyDF -n $SLURM_JOB_NUM_NODES -e 4 -p cr_use_other_1518.py

smart_launch.sh -m $SLURM_MEM_PER_NODE -u $SLURM_CPUS_ON_NODE -c sortByKeyDF -n $SLURM_JOB_NUM_NODES -e 4 -p cr_use_other_time_adj.py
smart_launch.sh -m $SLURM_MEM_PER_NODE -u $SLURM_CPUS_ON_NODE -c sortByKeyDF -n $SLURM_JOB_NUM_NODES -e 4 -p cr_use_other_ind.py

seff "${SLURM_JOB_ID}"
