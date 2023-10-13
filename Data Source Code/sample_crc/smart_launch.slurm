#!/bin/bash

#SBATCH --job-name="pyspark"
#SBATCH -o cr_use.out
#SBATCH -e cr_use.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=36
#SBATCH --time=23:59:59
#SBATCH --mem=245G
#SBATCH -p normal

module load sbt
module load anaconda2/4.3.1
module load java/1.8.0_141
module load spark/2.3.0
module list

ulimit -s unlimited
ulimit -u 81920

# AUTOMATIC CONFIGURATION
smart_launch.sh -m $SLURM_MEM_PER_NODE -u $SLURM_CPUS_ON_NODE -c sortByKeyDF -n $SLURM_JOB_NUM_NODES -e 4 -p df_crc.py
smart_launch.sh -m $SLURM_MEM_PER_NODE -u $SLURM_CPUS_ON_NODE -c sortByKeyDF -n $SLURM_JOB_NUM_NODES -e 4 -p df_tenure.py
#smart_launch.sh -m $SLURM_MEM_PER_NODE -u $SLURM_CPUS_ON_NODE -c sortByKeyDF -n $SLURM_JOB_NUM_NODES -e 4 -p cr_use.py

seff "${SLURM_JOB_ID}"
