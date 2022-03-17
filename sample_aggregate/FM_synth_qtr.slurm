#!/bin/bash

#SBATCH --job-name="pyspark"
#SBATCH -o FM_synth_qtr.out
#SBATCH -e FM_synth_qtr.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=36
#SBATCH --time=1:00:00
#SBATCH --mem=10G
#SBATCH -p normal

module purge
module load slurm
module load R
module list

ulimit -s unlimited

# AUTOMATIC CONFIGURATION
Rscript FM_synth_qtr.R

seff "${SLURM_JOB_ID}"
