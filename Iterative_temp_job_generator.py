import yaml
from Helper_fun import generate_temp_range
import datetime
import os


def config_setup(work_dir: str, min_temp, max_temp, base_setup_contnet: list) -> str:
    """
    Writes configuratuin file

    Input:
    work_dir: Base directory in which the TEST and Glaciation_time_estimatior folders are
    min_temp: Minimum CTT of clouds after filtering
    max_temp: Maximum CTT of clouds after filtering
    base_setup_content: The contents of a setup file that will be used as a basis for the new one. 
                        This file should contain all the variables necessary for PyFLEXTRKR to run.
    Outout:
    temp_setup_fp: location of the newly created configuation file
    """
    temp_setup = base_setup_contnet
    # Set temp_setup names
    temp_setup["databasename"] = f"CTT-it-{round(abs(min_temp))}-{round(abs(max_temp))}-"
    temp_setup['stats_path_name'] = f'stats_T-{round(abs(min_temp))}-{round(abs(max_temp))}'
    temp_setup['root_path'] = work_dir + \
        f'/example_postprocessing/T-{round(abs(min_temp))}-{round(abs(max_temp))}'
    temp_setup['logger_filepath'] = work_dir + \
        f'/Glaciation_time_estimatior/Debug/Logging/log_T-{round(abs(min_temp))}-{round(abs(max_temp))}.log'
    temp_setup['error_filepath'] = work_dir + \
        f'/Glaciation_time_estimatior/Debug/Errors/err_T-{round(abs(min_temp))}-{round(abs(max_temp))}.log'
    temp_setup_fp = work_dir + \
        f'/TEST/example_preprocessing/It_Setups/setup_T-{round(abs(min_temp))}-{round(abs(max_temp))}.yml'
    with open(temp_setup_fp, 'w') as file:
        yaml.dump(temp_setup, file)
    return temp_setup_fp


def job_setup(work_dir, min_temp, max_temp, base_job_contnet, work_dir_outer):
    """
    Writes new slurm job
    """
    # Set job file variables
    current_job_content = base_job_content + \
        ["python " + work_dir+"/PyFLEXTRKR/runscripts/run_generic_tracking.py " +
         temp_setup_fp + "\n",  'date']
    current_job_content[1] = f'#SBATCH -J T-{round(abs(min_temp))}-{round(abs(max_temp))}\n'
    current_job_content[
        2] = '#SBATCH -o ' + work_dir + f'Job_output/Output/pyflex_T-{round(abs(min_temp))}-{round(abs(max_temp))}.out\n'
    current_job_content[
        3] = '#SBATCH -o ' + work_dir + f'Job_output/Error/pyflex_T-{round(abs(min_temp))}-{round(abs(max_temp))}.err\n'

    # Write job file
    with open(work_dir_outer+f'/Jobs/test_job_T-{round(abs(min_temp))}-{round(abs(max_temp))}.bsub', 'w+') as job_current:
        job_current.writelines(current_job_content)


##############################################
# Load files
##############################################
# Directory of the Glaciation_time_estimator and TEST folders
work_dir = os.environ['PyFLEXTRKR_LIB_DIR']
work_dir_outer = os.environ['WORK_DIR']
# tmp_dir = os.environ['TMPDIR']

# Load a copy of the setup and job files used as base
with open(work_dir+"/Glaciation_time_estimatior/custom_setup.yml", 'r') as stream:
    try:
        custom_setup = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
        exit
with open(work_dir_outer+f'/Jobs/test_job.bsub', 'r') as job_base:
    base_job_content = job_base.readlines()

##############################################
# Generate temp range
##############################################
# List of temperature deltas for which to generate files, ex. t_deltas=[10,15] min_temp=[-10,-20,-30,-15,-30] max_temp=[0,-10,-20,0,-15]
t_deltas = [2, 5, 10, 15, 38]
# Generate arrays for min and max temp as described in previous comment
min_temp_array, max_temp_array = generate_temp_range(t_deltas)

##############################################
# Write job and config files for each range
##############################################

for i in range(len(min_temp_array)):
    min_temp, max_temp = min_temp_array[i], max_temp_array[i]
    # Create setup that will be used for the given time range

    # Inform the world of the new iteration coming
    # Behold, he is coming with the clouds, and every eye will see him, even those who pierced him, and all tribes of the earth will wail on account of him.
    # Even so. Amen.
    now = datetime.datetime.now()
    new_iteration_text = f"\n\n\n###########################\nNEW ITERATION: {min_temp}<T<{max_temp}\nTime: {now}\n###########################\n\n"
    print(new_iteration_text)

    temp_setup_fp = config_setup(work_dir, min_temp, max_temp, custom_setup)
    job_setup(work_dir,min_temp,max_temp,base_job_content,work_dir_outer)

# "/wolke_scratch/dnikolo/TEST/example_preprocessing/It_Setups/e.yaml"
