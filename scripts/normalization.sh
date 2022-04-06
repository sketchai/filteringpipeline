#!/bin/bash

#SBATCH --job-name=sketchgraphs_preprocessing
#SBATCH --output=normalization_output.out
#SBATCH --error=normalization_output.err
#SBATCH --qos=an_all_short
#SBATCH --partition=an
#SBATCH --nodes=1
#SBATCH --time=08:00:00
#SBATCH --wckey="P11MM:SALOME"
#SBATCH --exclusive
set -x
srun hostname
. /home/f49681/anaconda3/etc/profile.d/conda.sh
now=$(date +"%m_%d_%Y_%H_%M_%S")
folder="scripts/normalization_$now"
mkdir $folder
STARTTIME=$(date +%s)

conda activate sg_prep
srun python experiments/experiment_normalization.py --dataset train

mv normalization_output.err $folder
mv normalization_output.out $folder

ENDTIME=$(date +%s)
echo "Time spent: $((($ENDTIME - $STARTTIME)/ 60)) minutes"
