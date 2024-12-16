#!/bin/bash
start_time="202301010000"
end_time="202301072345"
min_temp=5
max_temp=0
agg_fact=3
dt=5
pole="np"
# sbatch -J "$name" "$GTE_DIR"/Cloud_tracking/job.bsub -h $max_temp -l $min_temp -a 3 -s $start_time -e $end_time -p $pole
bash "$GTE_DIR"/Cloud_tracking/job.bsub  -h $max_temp -l $min_temp -p $pole

