from File_name_generator import generate_filename_dict
from datetime import datetime
import numpy as np
import os


def fps_by_folder(fp_arr_target):
    vec_dirname = np.vectorize(os.path.dirname)
    _, folder_start_ind = np.unique(
        vec_dirname(fp_arr_target), return_index=True)
    fps_by_folder = []
    for ind in range(len(folder_start_ind)):
        # print("b")
        if ind < len(folder_start_ind)-1:
            start_ind = folder_start_ind[ind]
            end_ind = folder_start_ind[ind+1]
            fps_by_folder.append(fp_arr_target[start_ind:end_ind])
            # print(start_ind, end_ind)
        else:
            start_ind = folder_start_ind[ind]
            fps_by_folder.append(fp_arr_target[start_ind:])
    return fps_by_folder

t_deltas = [5]
agg_fact = 3
pole = "np"
target_filenames = generate_filename_dict(start_time=datetime(
    2023, 1, 19, 0, 0), end_time=datetime(2023, 1, 19, 1, 0), t_deltas=t_deltas, agg_fact=agg_fact)
folder_fp_ind=0
folder_resample_res_fps = fps_by_folder(
            target_filenames[pole]["resample_res"])
folder_agg_res_fps = fps_by_folder(
            target_filenames[pole]["agg_res"])
resample_res_fps = folder_resample_res_fps[folder_fp_ind]

import subprocess
cdo_comands_fp=os.path.join(os.path.dirname(resample_res_fps[0]),"cdo_comands.txt")
with open(cdo_comands_fp, 'w+') as cdo_command_file:
    for filename_ind in range(len(resample_res_fps)):
        resample_res_file = resample_res_fps[filename_ind]
        agg_res_file = folder_agg_res_fps[folder_fp_ind][filename_ind]
        cdo_command_file.write(" ".join(["cdo", f"gridboxmean,{agg_fact},{agg_fact}",
                    "-selname,cph_resampled,ctt_resampled", resample_res_file, agg_res_file])+"\n")
args = [f"bash /wolke_scratch/dnikolo/Glaciation_time_estimatior/Data_preprocessing/paralel_aggregation.sh {cdo_comands_fp}"]
result = subprocess.run(
    args,
    shell=True,
    capture_output=True,
    text=True
)

# Check results
if result.returncode == 0:
    print("All tasks completed successfully:")
    print(result.stdout)
else:
    print("Error occurred:")
    print(result.stderr)