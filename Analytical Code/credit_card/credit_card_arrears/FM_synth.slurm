#!/bin/bash

#SBATCH --job-name="pyspark"
#SBATCH -o FM_synth.out
#SBATCH -e FM_synth.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=36
#SBATCH --time=6:00:00
#SBATCH --mem=110G
#SBATCH -p normal

module load R/4.1.0
module list

# AUTOMATIC CONFIGURATION

Rscript FM_synth.R

Rscript FM_synth_report.R

Rscript FM_synth_CI_severe.R

Rscript FM_synth_CI_nonsevere.R
