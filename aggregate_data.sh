#!/bin/bash
set -x
for agg_fact in 2 3 5 10; do
    dt=5
    for min_temp in $(seq $dt $dt 38); do
        # max = min - dt because we deal with absolutes of negative numbers
        max_temp=$((min_temp - dt))
        # echo "cdo gridboxmean,$agg_fact,$agg_fact /cluster/work/climate/dnikolo/n2o/TEST/example_preprocessing/WG-T-$min_temp-$max_temp-agg-0-01-02-2004_14:15:00.nc /cluster/work/climate/dnikolo/n2o/TEST/example_preprocessing/WG-T-$min_temp-$max_temp-agg-$agg_fact-01-02-2004_14:15:00.nc" 
        if ! cdo gridboxmedian,$agg_fact,$agg_fact "/cluster/work/climate/dnikolo/n2o/TEST/example_preprocessing/WG-T-$min_temp-$max_temp-agg-1-01-02-2004_14:15:00.nc" "/cluster/work/climate/dnikolo/n2o/TEST/example_preprocessing/WG-T-$min_temp-$max_temp-agg-$agg_fact-01-02-2004_14:15:00.nc"; then
            echo "cdo command failed for agg_fact=$agg_fact, min_temp=$min_temp, max_temp=$max_temp"
        fi
    done
done