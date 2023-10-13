#!/bin/bash

#SBATCH --job-name="pyspark"
#SBATCH -o FM_synth_qtr_CI_sd.out
#SBATCH -e FM_synth_qtr_CI_sd.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=36
#SBATCH --time=3:00:00
#SBATCH --mem=110G
#SBATCH -p normal

module load R
module list

# AUTOMATIC CONFIGURATION
Rscript FM_synth_qtr_CI_severe.R

seff "${SLURM_JOB_ID}"
