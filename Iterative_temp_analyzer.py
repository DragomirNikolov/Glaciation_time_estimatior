import numpy as np
import netCDF4 as nc
import yaml
import subprocess
from Helper_fun import generate_temp_range
with open("/wolke_scratch/dnikolo/TEST/example_preprocessing/custom_setup.yml",'r') as stream:
        try:
            custom_setup=yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            exit
t_deltas=[2,5,10]
temp_bounds=generate_temp_range(t_deltas)
for t_min,t_max in temp_bounds:
    
    temp_setup=custom_setup
    temp_setup["databasename"]=f"CTT-it-{round(abs(min_temp))}-{round(abs(max_temp))}-"
    temp_setup['stats_path_name']=f'stats_T-{round(abs(min_temp))}-{round(abs(max_temp))}'       
    setup_fp=f'/wolke_scratch/dnikolo/TEST/example_preprocessing/It_Setups/setup_T-{round(abs(min_temp))}-{round(abs(max_temp))}.yml'
    with open(setup_fp, 'w') as file:
        yaml.dump(temp_setup, file)
    err_fp=
    with open()
    subprocess.run(["/wolke_scratch/dnikolo/flextrkr/bin/python","/wolke_scratch/dnikolo/PyFLEXTRKR/runscripts/run_generic_tracking.py", setup_fp])
        ####!!!Make it wait for the completion of the setup!!!

#"/wolke_scratch/dnikolo/TEST/example_preprocessing/It_Setups/e.yaml"