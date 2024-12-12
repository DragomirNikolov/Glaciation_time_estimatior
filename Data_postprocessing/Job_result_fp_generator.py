import os
import sys
from Glaciation_time_estimatior.Auxiliary_func.Helper_fun import generate_temp_range
from Glaciation_time_estimatior.Data_preprocessing.Output_file_generation import MissingFilesSearcher
import datetime as dt

if os.environ['WORK_DIR'] != '':
    global WORK_DIR
    WORK_DIR = os.environ['WORK_DIR']
    PyFLEXTRKR_LIB_DIR = os.environ['PyFLEXTRKR_LIB_DIR']
else:
    raise ValueError(
        "PyFLEXTRKR_LIB_DIR environmental variable is empty or doesn't exist")
job_output_dir = WORK_DIR+'/Job_output'

def generate_output_filenames(start_time, end_time, t_deltas, agg_fact):
    for pole in poles:
        job_output_dir = os.path.join(WORK_DIR,'Job_output' ,pole)
    min_temp_array, max_temp_array = generate_temp_range(t_deltas)
    

    fp_gen = MissingFilesSearcher(start_time,end_time,t_deltas, agg_factor)
    MissingFilesSearcher.gen_cloudtrack_filenames()

# pole/Agg_3_T_5_0/pixel_path_tracking/YmD.HM(start)_YmD.HM(end)
# pole/Agg_3_T_5_0/stats/YmD.HM(start)_YmD.HM(end)
# pole/Agg_3_T_5_0/tracking/YmD.HM(start)_YmD.HM(end)
# pole/Agg_3_T_5_0/aux/YmD.HM(start)_YmD.HM(end)/log
# pole/Agg_3_T_5_0/aux/YmD.HM(start)_YmD.HM(end)/setup