import yaml
import subprocess
from Helper_fun import generate_temp_range
import datetime
work_dir='/wolke_scratch/dnikolo'
with open(work_dir+"/Glaciation_time_estimatior/custom_setup.yml", 'r') as stream:
    try:
        custom_setup = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
        exit

t_deltas = [5]
min_temp_array, max_temp_array = generate_temp_range(t_deltas)
for i in range(len(min_temp_array)):
    min_temp, max_temp = min_temp_array[i],max_temp_array[i]
    temp_setup = custom_setup
    temp_setup["databasename"] = f"CTT-it-{round(abs(min_temp))}-{round(abs(max_temp))}-"
    temp_setup['root_path']=f'/wolke_scratch/dnikolo/TEST/example_postprocessing/T-{round(abs(min_temp))}-{round(abs(max_temp))}'
    temp_setup['stats_path_name'] = f'stats_T-{round(abs(min_temp))}-{round(abs(max_temp))}'
    temp_setup['logger_filepath']=work_dir+f'/Glaciation_time_estimatior/Debug/Logging/log_T-{round(abs(min_temp))}-{round(abs(max_temp))}.log'
    temp_setup['error_filepath']=work_dir+f'/Glaciation_time_estimatior/Debug/Errors/err_T-{round(abs(min_temp))}-{round(abs(max_temp))}.log'
    temp_setup_fp = work_dir+f'/TEST/example_preprocessing/It_Setups/setup_T-{round(abs(min_temp))}-{round(abs(max_temp))}.yml'
    # Inform the world of the new iteration coming
    # Behold, he is coming with the clouds, and every eye will see him, even those who pierced him, and all tribes of the earth will wail on account of him.
    # Even so. Amen.
    now = datetime.datetime.now()
    new_iteration_text = f"\n\n\n###########################\nNEW ITERATION: {min_temp}<T<{max_temp}\nTime: {now}\n###########################\n\n"
    print(new_iteration_text)
    with open(temp_setup_fp, 'w') as file:
        yaml.dump(temp_setup, file)
    # with open(temp_setup["logger_filepath"], 'a+') as log_file:
    #     log_file.write(new_iteration_text)
    with open(temp_setup["error_filepath"], 'a+') as err_file:
        err_file.write(new_iteration_text)
        #RUN NEW ITERATION AS A SUBPROCESS
        subprocess.run(["/wolke_scratch/dnikolo/flextrkr/bin/python",
                       work_dir+"/PyFLEXTRKR/runscripts/run_generic_tracking.py", temp_setup_fp], stderr=err_file)

# "/wolke_scratch/dnikolo/TEST/example_preprocessing/It_Setups/e.yaml"
