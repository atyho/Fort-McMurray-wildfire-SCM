#!/bin/bash

#SBATCH --job-name="pyspark"
#SBATCH -o FM_synth_qtr_CI_sd.out
#SBATCH -e FM_synth_qtr_CI_sd.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=36
#SBATCH --time=24:00:00
#SBATCH --mem=110G
#SBATCH -p normal

module purge
module load slurm
module load R
module list

ulimit -s unlimited

# AUTOMATIC CONFIGURATION
Rscript FM_synth_qtr_CI_sd.R

seff "${SLURM_JOB_ID}"
