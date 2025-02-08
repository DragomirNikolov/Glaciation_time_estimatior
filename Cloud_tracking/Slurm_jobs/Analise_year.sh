#!/bin/bash
YEARS=("$@")

for YEAR in "${YEARS[@]}"; do
    YEAR=$1
    for MONTH in {01..12}; do
        CONFIG_FILE="/cluster/work/climate/dnikolo/n2o/Glaciation_time_estimator/configs/${YEAR}_tracking/${MONTH}.yaml"
        bash /cluster/work/climate/dnikolo/n2o/Glaciation_time_estimator/Cloud_tracking/Slurm_jobs/All_t_jobs.sh -c $CONFIG_FILE
        if [ $((10#$MONTH % 4)) -eq 0 ]; then
            sleep 5h
        fi
    done
done