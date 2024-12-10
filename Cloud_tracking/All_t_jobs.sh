#!/bin/bash
#5 38
#
start_time="202301010000"
end_time="202301072345"
# min_temp=5
# max_temp=0
agg_fact=3
dt=5
agg_fact=3
for min_temp in $(seq $dt $dt 38); do
    # max = min - dt because we deal with absolutes of negative numbers
    max_temp=$((min_temp - dt))
    for pole in "np" "sp"; do
        name=T_"$min_temp"_"$max_temp"_"$agg_fact"_"$start_time"_"$end_time"_"$pole"
        sbatch -J "$name" --output "$WORK_DIR"/Job_output/Output/"$name".out --error "$WORK_DIR"/Job_output/Error/"$name".err "$GTE_DIR"/Cloud_tracking/job.bsub -h $max_temp -l $min_temp -a 3 -s $start_time -e $end_time -p $pole
    done
done
