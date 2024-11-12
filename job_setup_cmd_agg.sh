#!/bin/bash
#5 38
#
for dt in 5; do
    for min_temp in $(seq $dt $dt 38); do
        # max = min - dt because we deal with absolutes of negative numbers
        max_temp=$((min_temp - dt))
        for agg_val in 1; do
            sbatch -J T-"$min_temp"-"$max_temp"-"$agg_val" --output "$WORK_DIR"/Job_output/Output/T-"$min_temp"-"$max_temp"-agg-"$agg_val".out --error "$WORK_DIR"/Job_output/Error/T-"$min_temp"-"$max_temp"-agg-"$agg_val".err "$WORK_DIR"/Jobs/base_job_cmd_agg.bsub "$min_temp" "$max_temp" "$agg_val"
        done
    done
done
