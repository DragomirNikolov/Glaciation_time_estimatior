#!/bin/bash
agg_fact=3
dt=3
for min_temp in $(seq $dt $dt 38); do
    # max = min - dt because we deal with absolutes of negative numbers
    max_temp=$((min_temp - dt))
    for pole in "np" "sp"; do
        name=T_"$min_temp"_"$max_temp"_"$agg_fact"_"$pole"
        sbatch -J "$name" "$GTE_DIR"/Cloud_tracking/job.bsub -h $max_temp -l $min_temp -p $pole -c "/cluster/work/climate/dnikolo/n2o/Glaciation_time_estimator/configs/config_testing_2024.yaml"
        #bash "$GTE_DIR"/Cloud_tracking/job.bsub  -h $max_temp -l $min_temp -a 3 -s $start_time -e $end_time -p $pole
    done
done
